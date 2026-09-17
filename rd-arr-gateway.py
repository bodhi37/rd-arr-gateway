#!/usr/bin/env python3
"""Durable qBittorrent-compatible Real-Debrid-first gateway.

Every trackable Arr add is offered to Decypharr first.  A request accepted by
Decypharr is retained on disk until its SSD download reaches pausedUP.  If the
RD job errors, disappears, or stops making progress, the original add payload
is replayed verbatim to qBittorrent, where the existing storage router takes
over HDD placement.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

LISTEN = ("127.0.0.1", 8283)
RD_BASE = "http://127.0.0.1:8282"
QBIT_BASE = "http://127.0.0.1:8080"
BYPASS_FILE = "/var/lib/rd-capacity-guard/bypass-rd"


STATE_DIR = Path(os.environ.get("RD_ARR_GATEWAY_STATE_DIR", "/var/lib/rd-arr-gateway"))
PENDING_DIR = STATE_DIR / "pending"
CLEANUP_DIR = STATE_DIR / "cleanup"
METRICS_FILE = STATE_DIR / "metrics.json"
MAX_BODY = 64 * 1024 * 1024
POLL_SECONDS = int(os.environ.get("RD_ARR_GATEWAY_POLL_SECONDS", "5"))
NEVER_SEEN_SECONDS = int(os.environ.get("RD_ARR_GATEWAY_NEVER_SEEN_SECONDS", "60"))
MISSING_SECONDS = int(os.environ.get("RD_ARR_GATEWAY_MISSING_SECONDS", "30"))
STALL_SECONDS = int(os.environ.get("RD_ARR_GATEWAY_STALL_SECONDS", "900"))
MAX_PENDING_SECONDS = int(os.environ.get("RD_ARR_GATEWAY_MAX_PENDING_SECONDS", "21600"))
MAGNET_HASH = re.compile(r"urn:btih:([A-Fa-f0-9]{40}|[A-Z2-7]{32})", re.I)
ERROR_STATES = {"error", "missingfiles", "unknown"}
COMPLETE_STATES = {"pausedup", "uploading", "stalledup", "forcedup"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rd-arr-gateway")
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
state_lock = threading.RLock()
fallback_inflight: set[str] = set()


def backend(base: str, method: str, path: str, headers: dict[str, str], body: bytes, *, qbit: bool = False):
    forwarded = {}
    for name, value in headers.items():
        lower = name.lower()
        if lower in {"content-type", "accept", "user-agent"}:
            forwarded[name] = value
        elif not qbit and lower in {"authorization", "cookie"}:
            forwarded[name] = value
    request = urllib.request.Request(base + path, data=body if body else None, headers=forwarded, method=method)
    try:
        timeout = 75 if path.startswith("/api/v2/torrents/add") else 20
        with opener.open(request, timeout=timeout) as response:
            return response.status, dict(response.headers.items()), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers.items()), error.read()
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        reason = error.reason if hasattr(error, "reason") else error
        return 599, {"Content-Type": "text/plain"}, str(reason).encode()


def normalize_hash(value: str) -> str | None:
    value = value.strip()
    if re.fullmatch(r"[A-Fa-f0-9]{40}", value):
        return value.lower()
    if re.fullmatch(r"[A-Z2-7]{32}", value, re.I):
        try:
            return base64.b32decode(value.upper()).hex()
        except Exception:
            return None
    return None


def skip_bencoded(data: bytes, position: int) -> int:
    if position >= len(data):
        raise ValueError("truncated bencode")
    marker = data[position:position + 1]
    if marker == b"i":
        end = data.index(b"e", position + 1)
        int(data[position + 1:end])
        return end + 1
    if marker in {b"l", b"d"}:
        position += 1
        while position < len(data) and data[position:position + 1] != b"e":
            position = skip_bencoded(data, position)
            if marker == b"d":
                position = skip_bencoded(data, position)
        if position >= len(data):
            raise ValueError("truncated bencode collection")
        return position + 1
    if marker.isdigit():
        colon = data.index(b":", position)
        length = int(data[position:colon])
        end = colon + 1 + length
        if end > len(data):
            raise ValueError("truncated bencode string")
        return end
    raise ValueError("invalid bencode marker")


def read_bencoded_bytes(data: bytes, position: int) -> tuple[bytes, int]:
    colon = data.index(b":", position)
    length = int(data[position:colon])
    start = colon + 1
    end = start + length
    if end > len(data):
        raise ValueError("truncated bencode string")
    return data[start:end], end


def torrent_info_hash(data: bytes) -> str | None:
    try:
        if not data.startswith(b"d"):
            return None
        position = 1
        while data[position:position + 1] != b"e":
            key, position = read_bencoded_bytes(data, position)
            value_start = position
            position = skip_bencoded(data, position)
            if key == b"info":
                return hashlib.sha1(data[value_start:position]).hexdigest()
    except (ValueError, IndexError):
        return None
    return None


def torrent_payloads(body: bytes, content_type: str) -> list[bytes]:
    if "multipart/form-data" not in content_type.lower():
        return []
    try:
        envelope = (
            b"Content-Type: " + content_type.encode("latin-1")
            + b"\r\nMIME-Version: 1.0\r\n\r\n" + body
        )
        message = BytesParser(policy=policy.default).parsebytes(envelope)
        payloads = []
        for part in message.iter_parts():
            if part.get_filename() or part.get_param("name", header="content-disposition") == "torrents":
                payload = part.get_payload(decode=True)
                if payload:
                    payloads.append(payload)
        return payloads
    except Exception:
        return []


def info_hashes_from_body(body: bytes, content_type: str) -> list[str]:
    hashes: list[str] = []
    text = urllib.parse.unquote_plus(body.decode("latin-1", "ignore"))
    for match in MAGNET_HASH.finditer(text):
        normalized = normalize_hash(match.group(1))
        if normalized and normalized not in hashes:
            hashes.append(normalized)
    for payload in torrent_payloads(body, content_type):
        value = torrent_info_hash(payload)
        if value and value not in hashes:
            hashes.append(value)
    return hashes


def meta_path(info_hash: str) -> Path:
    return PENDING_DIR / f"{info_hash}.json"


def body_path(info_hash: str) -> Path:
    return PENDING_DIR / f"{info_hash}.body"


def cleanup_path(info_hash: str) -> Path:
    return CLEANUP_DIR / f"{info_hash}.json"


def atomic_write(path: Path, data: bytes):
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with open(temporary, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def load_meta(info_hash: str) -> dict | None:
    try:
        return json.loads(meta_path(info_hash).read_text())
    except (OSError, ValueError, TypeError):
        return None


def save_meta(meta: dict):
    atomic_write(meta_path(meta["hash"]), json.dumps(meta, separators=(",", ":"), sort_keys=True).encode())


def persist_pending(info_hash: str, body: bytes, headers: dict[str, str]):
    now = time.time()
    stored_headers = {
        name: value for name, value in headers.items()
        if name.lower() in {"content-type", "accept", "user-agent"}
    }
    with state_lock:
        clear_cleanup(info_hash)
        atomic_write(body_path(info_hash), body)
        save_meta({
            "hash": info_hash,
            "created_at": now,
            "seen": False,
            "last_seen_at": 0,
            "missing_since": 0,
            "last_progress": 0.0,
            "last_progress_at": now,
            "last_state": "accepted",
            "fallback_started": False,
            "fallback_started_at": 0,
            "retry_count": 0,
            "headers": stored_headers,
        })


def clear_pending(info_hash: str):
    with state_lock:
        for path in (meta_path(info_hash), body_path(info_hash)):
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def pending_hashes() -> list[str]:
    return sorted(path.stem for path in PENDING_DIR.glob("*.json") if re.fullmatch(r"[a-f0-9]{40}", path.stem))


def cleanup_hashes() -> list[str]:
    return sorted(path.stem for path in CLEANUP_DIR.glob("*.json") if re.fullmatch(r"[a-f0-9]{40}", path.stem))


def oldest_record_age(directory: Path) -> int:
    created = []
    for path in directory.glob("*.json"):
        try:
            value = json.loads(path.read_text())
            created.append(float(value.get("created_at") or path.stat().st_mtime))
        except (OSError, ValueError, TypeError):
            continue
    return max(0, int(time.time() - min(created))) if created else 0


def clear_cleanup(info_hash: str):
    try:
        cleanup_path(info_hash).unlink()
    except FileNotFoundError:
        pass


def read_metrics() -> dict:
    defaults = {
        "accepted_rd_total": 0,
        "completed_rd_total": 0,
        "sync_fallback_total": 0,
        "delayed_fallback_total": 0,
        "untrackable_total": 0,
    }
    try:
        stored = json.loads(METRICS_FILE.read_text())
        if isinstance(stored, dict):
            defaults.update(stored)
    except (OSError, ValueError, TypeError):
        pass
    return defaults


def metric_increment(name: str, *, reason: str | None = None):
    with state_lock:
        metrics = read_metrics()
        metrics[name] = int(metrics.get(name, 0)) + 1
        metrics["updated_at"] = int(time.time())
        if reason:
            metrics["last_fallback_reason"] = reason
        atomic_write(METRICS_FILE, json.dumps(metrics, separators=(",", ":"), sort_keys=True).encode())


def parse_items(result) -> list[dict]:
    status, _, payload = result
    if status != 200:
        return []
    try:
        items = json.loads(payload)
        return items if isinstance(items, list) else []
    except (ValueError, TypeError):
        return []


def query_hash(base: str, info_hash: str, *, qbit: bool = False):
    path = "/api/v2/torrents/info?hashes=" + urllib.parse.quote(info_hash)
    result = backend(base, "GET", path, {}, b"", qbit=qbit)
    return result, parse_items(result)


def delete_from_rd(info_hash: str):
    payload = urllib.parse.urlencode({"hashes": info_hash, "deleteFiles": "true"}).encode()
    return backend(
        RD_BASE,
        "POST",
        "/api/v2/torrents/delete",
        {"Content-Type": "application/x-www-form-urlencoded"},
        payload,
    )


def schedule_rd_cleanup(info_hash: str, reason: str):
    result = delete_from_rd(info_hash)
    if 200 <= result[0] < 300 or result[0] == 404:
        clear_cleanup(info_hash)
        return
    with state_lock:
        atomic_write(cleanup_path(info_hash), json.dumps({
            "hash": info_hash,
            "created_at": int(time.time()),
            "reason": reason,
            "last_status": result[0],
        }, separators=(",", ":"), sort_keys=True).encode())
    log.warning("cleanup_deferred backend=realdebrid status=%s reason=%s hash=%s", result[0], reason, info_hash)


def process_rd_cleanups():
    for info_hash in cleanup_hashes():
        result = delete_from_rd(info_hash)
        if 200 <= result[0] < 300 or result[0] == 404:
            clear_cleanup(info_hash)
            log.info("cleanup_completed backend=realdebrid hash=%s", info_hash)


def tag_fallback(info_hash: str):
    path = "/api/v2/torrents/info?hashes=" + urllib.parse.quote(info_hash)
    for _ in range(20):
        status, _, response = backend(QBIT_BASE, "GET", path, {}, b"", qbit=True)
        try:
            if status == 200 and json.loads(response):
                break
        except Exception:
            pass
        time.sleep(0.25)
    payload = urllib.parse.urlencode({"hashes": info_hash, "tags": "source_rd_fallback"}).encode()
    backend(
        QBIT_BASE,
        "POST",
        "/api/v2/torrents/addTags",
        {"Content-Type": "application/x-www-form-urlencoded"},
        payload,
        qbit=True,
    )


def release_fallback_claim(info_hash: str, message: str):
    with state_lock:
        fallback_inflight.discard(info_hash)
        meta = load_meta(info_hash)
        if not meta:
            return
        meta["fallback_started"] = False
        meta["fallback_started_at"] = 0
        meta["retry_count"] = int(meta.get("retry_count", 0)) + 1
        meta["last_error"] = message[:500]
        save_meta(meta)


def fallback_pending(info_hash: str, reason: str) -> bool:
    with state_lock:
        if info_hash in fallback_inflight:
            return False
        meta = load_meta(info_hash)
        if not meta:
            return False
        fallback_inflight.add(info_hash)
        meta["fallback_started"] = True
        meta["fallback_started_at"] = time.time()
        meta["fallback_reason"] = reason
        save_meta(meta)

    try:
        payload = body_path(info_hash).read_bytes()
        headers = dict(meta.get("headers") or {})
    except OSError as error:
        release_fallback_claim(info_hash, f"stored payload unavailable: {error}")
        return False

    qbit_health = backend(QBIT_BASE, "GET", "/api/v2/app/version", {}, b"", qbit=True)
    if qbit_health[0] != 200:
        release_fallback_claim(info_hash, f"qBittorrent unavailable: {qbit_health[0]}")
        return False

    _, existing = query_hash(QBIT_BASE, info_hash, qbit=True)
    if existing:
        schedule_rd_cleanup(info_hash, reason)
        tag_fallback(info_hash)
        clear_pending(info_hash)
        with state_lock:
            fallback_inflight.discard(info_hash)
        metric_increment("delayed_fallback_total", reason=reason + "_already_in_qbit")
        log.info("handoff backend=qbittorrent reason=%s hash=%s existing=true", reason, info_hash)
        return True

    status, _, response = backend(
        QBIT_BASE,
        "POST",
        "/api/v2/torrents/add",
        headers,
        payload,
        qbit=True,
    )
    if not 200 <= status < 300:
        release_fallback_claim(info_hash, f"qBittorrent add failed: {status} {response[:200]!r}")
        log.error("handoff_failed backend=qbittorrent status=%s reason=%s hash=%s", status, reason, info_hash)
        return False

    schedule_rd_cleanup(info_hash, reason)
    tag_fallback(info_hash)
    clear_pending(info_hash)
    with state_lock:
        fallback_inflight.discard(info_hash)
    metric_increment("delayed_fallback_total", reason=reason)
    log.info("handoff backend=qbittorrent reason=%s hash=%s", reason, info_hash)
    return True


def mark_rd_complete(info_hash: str, source: str):
    if load_meta(info_hash):
        clear_pending(info_hash)
        metric_increment("completed_rd_total")
        log.info("completed backend=realdebrid source=%s hash=%s", source, info_hash)


def evaluate_pending(info_hash: str):
    meta = load_meta(info_hash)
    if not meta:
        return
    now = time.time()
    result, items = query_hash(RD_BASE, info_hash)
    if items:
        item = items[0]
        state = str(item.get("state") or "").lower()
        try:
            progress = float(item.get("progress") or 0.0)
        except (TypeError, ValueError):
            progress = 0.0
        try:
            speed = int(item.get("dlspeed") or 0)
        except (TypeError, ValueError):
            speed = 0

        if state in COMPLETE_STATES and progress >= 0.999:
            mark_rd_complete(info_hash, "monitor")
            return
        if state in ERROR_STATES:
            fallback_pending(info_hash, "rd_error")
            return

        previous_progress = float(meta.get("last_progress", 0.0))
        if abs(progress - previous_progress) >= 0.0001 or speed > 0:
            meta["last_progress_at"] = now
        meta["last_progress"] = progress
        meta["last_state"] = state or "visible"
        meta["last_seen_at"] = now
        meta["seen"] = True
        meta["missing_since"] = 0
        save_meta(meta)

        if now - float(meta.get("created_at", now)) >= MAX_PENDING_SECONDS:
            fallback_pending(info_hash, "rd_max_pending")
        elif now - float(meta.get("last_progress_at", now)) >= STALL_SECONDS:
            fallback_pending(info_hash, "rd_no_progress")
        return

    if result[0] not in {200, 599} and result[0] < 500:
        meta["last_state"] = f"rd_http_{result[0]}"
    if not meta.get("missing_since"):
        meta["missing_since"] = now
    save_meta(meta)
    if meta.get("seen"):
        if now - float(meta["missing_since"]) >= MISSING_SECONDS:
            fallback_pending(info_hash, "rd_disappeared")
    elif now - float(meta.get("created_at", now)) >= NEVER_SEEN_SECONDS:
        fallback_pending(info_hash, "rd_never_visible")


def monitor_loop():
    while True:
        try:
            process_rd_cleanups()
            for info_hash in pending_hashes():
                evaluate_pending(info_hash)
        except Exception:
            log.exception("pending monitor iteration failed")
        time.sleep(POLL_SECONDS)


def hashes_from_mutation(body: bytes) -> list[str]:
    try:
        form = urllib.parse.parse_qs(body.decode("utf-8", "ignore"))
    except Exception:
        return []
    values = form.get("hashes", [])
    if any(value == "all" for value in values):
        return pending_hashes()
    result = []
    for value in values:
        for candidate in value.split("|"):
            normalized = normalize_hash(candidate)
            if normalized:
                result.append(normalized)
    return result


def merge_info_results(rd_result, qbit_result) -> tuple[int, dict[str, str], bytes]:
    rd_items = parse_items(rd_result)
    qbit_items = parse_items(qbit_result)
    qbit_by_hash = {
        str(item.get("hash") or "").lower(): item for item in qbit_items if item.get("hash")
    }
    combined = dict(qbit_by_hash)
    for item in rd_items:
        info_hash = str(item.get("hash") or "").lower()
        if not info_hash:
            continue
        meta = load_meta(info_hash)
        state = str(item.get("state") or "").lower()
        try:
            progress = float(item.get("progress") or 0.0)
        except (TypeError, ValueError):
            progress = 0.0

        if meta and info_hash in qbit_by_hash:
            schedule_rd_cleanup(info_hash, "qbit_duplicate_detected")
            tag_fallback(info_hash)
            clear_pending(info_hash)
            metric_increment("delayed_fallback_total", reason="qbit_duplicate_detected")
            continue
        if meta and state in ERROR_STATES:
            fallback_pending(info_hash, "rd_error_poll")
            continue
        if meta and state in COMPLETE_STATES and progress >= 0.999:
            mark_rd_complete(info_hash, "arr_poll")
        combined[info_hash] = item

    if rd_result[0] == 200 or qbit_result[0] == 200:
        headers = rd_result[1] if rd_result[0] == 200 else qbit_result[1]
        headers = dict(headers)
        headers["Content-Type"] = "application/json"
        return 200, headers, json.dumps(list(combined.values()), separators=(",", ":")).encode()
    return qbit_result if qbit_result[0] != 599 else rd_result


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _format, *_args):
        return

    def read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length > MAX_BODY:
            raise ValueError("request body exceeds 64 MiB")
        return self.rfile.read(length) if length else b""

    def respond(self, status: int, headers: dict[str, str], body: bytes, backend_name: str | None = None):
        if status == 599:
            status = 502
        self.send_response(status)
        self.send_header("Content-Type", headers.get("Content-Type", "text/plain"))
        if "Set-Cookie" in headers:
            self.send_header("Set-Cookie", headers["Set-Cookie"])
        if backend_name:
            self.send_header("X-Download-Backend", backend_name)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def handle_health(self):
        rd_status, _, _ = backend(RD_BASE, "GET", "/version", {}, b"")
        qbit_status, _, _ = backend(QBIT_BASE, "GET", "/api/v2/app/version", {}, b"", qbit=True)
        metrics = read_metrics()
        payload = json.dumps({
            "status": "ok" if qbit_status == 200 else "error",
            "rd": rd_status == 200,
            "qbit": qbit_status == 200,
            "rd_bypassed": os.path.exists(BYPASS_FILE),
            "durable_handoff": True,
            "pending_rd": len(pending_hashes()),
            "pending_rd_cleanup": len(cleanup_hashes()),
            "oldest_pending_age_seconds": oldest_record_age(PENDING_DIR),
            "oldest_cleanup_age_seconds": oldest_record_age(CLEANUP_DIR),
            "accepted_rd_total": metrics.get("accepted_rd_total", 0),
            "completed_rd_total": metrics.get("completed_rd_total", 0),
            "fallback_total": int(metrics.get("sync_fallback_total", 0)) + int(metrics.get("delayed_fallback_total", 0)),
            "last_fallback_reason": metrics.get("last_fallback_reason"),
        }, separators=(",", ":")).encode()
        self.respond(200 if qbit_status == 200 else 503, {"Content-Type": "application/json"}, payload)

    def add_to_qbit(self, body: bytes, request_headers: dict[str, str], info_hash: str | None, reason: str):
        status, headers, response = backend(QBIT_BASE, self.command, self.path, request_headers, body, qbit=True)
        if 200 <= status < 300:
            if info_hash:
                tag_fallback(info_hash)
            metric_increment("sync_fallback_total", reason=reason)
            log.info("accepted backend=qbittorrent reason=%s hash=%s", reason, info_hash or "unknown")
        else:
            log.error("rejected backend=qbittorrent status=%s reason=%s hash=%s", status, reason, info_hash or "unknown")
        self.respond(status, headers, response, "qbittorrent")

    def handle_add(self, body: bytes):
        request_headers = dict(self.headers.items())
        content_type = self.headers.get("Content-Type", "")
        info_hashes = info_hashes_from_body(body, content_type)
        info_hash = info_hashes[0] if len(info_hashes) == 1 else None

        if info_hash is None:
            metric_increment("untrackable_total")
            self.add_to_qbit(body, request_headers, None, "untrackable_payload")
            return

        if not os.path.exists(BYPASS_FILE):
            status, headers, response = backend(RD_BASE, self.command, self.path, request_headers, body)
            if 200 <= status < 300:
                persist_pending(info_hash, body, request_headers)
                metric_increment("accepted_rd_total")
                log.info("accepted backend=realdebrid hash=%s durable=true", info_hash)
                self.respond(status, headers, response, "realdebrid")
                return
            reason = f"rd_status_{status}"
        else:
            reason = "capacity_guard"
        self.add_to_qbit(body, request_headers, info_hash, reason)

    def dispatch(self):
        if self.path == "/_health":
            self.handle_health()
            return
        try:
            body = self.read_body()
        except ValueError as error:
            self.respond(413, {"Content-Type": "text/plain"}, str(error).encode())
            return

        if self.path.startswith("/api/v2/torrents/add") and self.command == "POST":
            self.handle_add(body)
            return

        request_headers = dict(self.headers.items())
        parsed = urllib.parse.urlsplit(self.path)

        if parsed.path == "/api/v2/torrents/info" and self.command == "GET":
            rd_result = backend(RD_BASE, self.command, self.path, request_headers, body)
            qbit_result = backend(QBIT_BASE, self.command, self.path, request_headers, body, qbit=True)
            result = merge_info_results(rd_result, qbit_result)
            self.respond(*result, "realdebrid+qbittorrent")
            return

        hash_specific = parsed.path in {
            "/api/v2/torrents/properties", "/api/v2/torrents/files",
        }
        if hash_specific:
            status, headers, response = backend(RD_BASE, self.command, self.path, request_headers, body)
            if 200 <= status < 300:
                self.respond(status, headers, response, "realdebrid")
                return
            status, headers, response = backend(QBIT_BASE, self.command, self.path, request_headers, body, qbit=True)
            self.respond(status, headers, response, "qbittorrent")
            return

        dual_mutations = {
            "/api/v2/torrents/delete", "/api/v2/torrents/pause", "/api/v2/torrents/resume",
            "/api/v2/torrents/recheck", "/api/v2/torrents/setCategory",
            "/api/v2/torrents/addTags", "/api/v2/torrents/removeTags",
            "/api/v2/torrents/setShareLimits", "/api/v2/torrents/setForceStart",
            "/api/v2/torrents/topPrio",
        }
        if parsed.path in dual_mutations and self.command == "POST":
            if parsed.path == "/api/v2/torrents/delete":
                for info_hash in hashes_from_mutation(body):
                    clear_pending(info_hash)
            rd_result = backend(RD_BASE, self.command, self.path, request_headers, body)
            qbit_result = backend(QBIT_BASE, self.command, self.path, request_headers, body, qbit=True)
            if parsed.path == "/api/v2/torrents/delete" and not 200 <= rd_result[0] < 300:
                for info_hash in hashes_from_mutation(body):
                    schedule_rd_cleanup(info_hash, "client_delete_while_rd_unavailable")
            chosen = rd_result if 200 <= rd_result[0] < 300 else qbit_result
            self.respond(*chosen, "both")
            return

        status, headers, response = backend(RD_BASE, self.command, self.path, request_headers, body)
        if status >= 500 or (status == 404 and os.path.exists(BYPASS_FILE)):
            status, headers, response = backend(QBIT_BASE, self.command, self.path, request_headers, body, qbit=True)
            self.respond(status, headers, response, "qbittorrent")
            return
        self.respond(status, headers, response, "realdebrid")

    do_GET = dispatch
    do_POST = dispatch
    do_DELETE = dispatch
    do_HEAD = dispatch


if __name__ == "__main__":
    STATE_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    PENDING_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    CLEANUP_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    for info_hash in pending_hashes():
        meta = load_meta(info_hash)
        if meta and meta.get("fallback_started"):
            meta["fallback_started"] = False
            meta["fallback_started_at"] = 0
            save_meta(meta)
    monitor = threading.Thread(target=monitor_loop, name="rd-pending-monitor", daemon=True)
    monitor.start()
    server = ThreadingHTTPServer(LISTEN, Handler)
    server.daemon_threads = True
    log.info(
        "listening=%s:%s rd=%s qbit=%s durable=true stall=%ss max_pending=%ss",
        *LISTEN, RD_BASE, QBIT_BASE, STALL_SECONDS, MAX_PENDING_SECONDS,
    )
    server.serve_forever()

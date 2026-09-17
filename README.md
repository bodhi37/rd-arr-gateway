# rd-arr-gateway

Request anything in Seerr. If Real-Debrid already has it, it downloads at full line speed. If not, the normal Sonarr / Radarr + qBittorrent stack picks it up.

No client setup. No trackers. Open Seerr, hit Request, it shows up in Jellyfin.

## Why this exists

Torrents are seeder-dependent: slow, inconsistent, and hard on HDDs.

Real-Debrid is a cheap download cache and the download speed differencess against torrents most of the time is ginormous. One RD subscription replaces private trackers and seedboxes for the common case.

The *arr torrent pipeline stays as backup, so a cache miss still completes — just slower.

## The pipeline
```text
Seerr -> Sonarr / Radarr + Prowlarr -> gateway :8283
  -> RD client :8282 -> SSD staging -> import to HDD library
  -> fallback qBittorrent :8080 -> direct to HDD -> import to library
```

The gateway speaks the qBittorrent API, so the Arrs treat it as an ordinary download client sitting at priority 1. If RD errors, never shows the job, loses it, or stalls, the gateway replays the saved request to real qBittorrent and tags it `source_rd_fallback`. Status polls see both backends merged, so the import works either way and you never care which path yours took.

## Why a small SSD is enough

The SSD is a loading dock, not a warehouse. It only ever holds downloads still in flight — between grabs it sits empty.
RD downloads arrive at full line speed for a few minutes, get imported to the HDD library, and the space is reused. The collection itself lives on cheap HDDs behind a single library path.
Torrent fallback writes straight to HDD and skips the SSD entirely, so the slow path never fights fast ingest for SSD room.

A couple hundred GB of staging comfortably handles several simultaneous RD downloads (the RD client runs max 4 at a time).

## Install

Requires Python 3.10+ (stdlib only, no dependencies), a qBittorrent-compatible Real-Debrid client (e.g. Decypharr) on `:8282`, and qBittorrent on `:8080`.

```bash
sudo cp rd-arr-gateway.py /usr/local/lib/rd-arr-gateway.py
sudo cp rd-arr-gateway.service /etc/systemd/system/rd-arr-gateway.service
sudo systemctl daemon-reload && sudo systemctl enable --now rd-arr-gateway.service
curl -s http://127.0.0.1:8283/_health | jq .
```

In Sonarr / Radarr, add a `qBittorrent` download client pointed at `127.0.0.1:8283` as priority 1. Keep the real qBittorrent as a lower-priority client; categories pass through untouched.

## Settings

| Var | Default |
|---|---|
| `RD_ARR_GATEWAY_STATE_DIR` | `/var/lib/rd-arr-gateway` |
| `RD_ARR_GATEWAY_POLL_SECONDS` | `5` |
| `RD_ARR_GATEWAY_NEVER_SEEN_SECONDS` | `60` |
| `RD_ARR_GATEWAY_MISSING_SECONDS` | `30` |
| `RD_ARR_GATEWAY_STALL_SECONDS` | `120` (code default `900`; the unit overrides it) |
| `RD_ARR_GATEWAY_MAX_PENDING_SECONDS` | `21600` |

Bypass file `/var/lib/rd-capacity-guard/bypass-rd`: present = torrents directly, absent = RD first. The gateway never creates it — `touch` the file to force torrent-only mode, delete it to go back.

State in `/var/lib/rd-arr-gateway`: `pending/<hash>.body+json`, `cleanup/`, `metrics.json`. Atomic `0600` writes, restart-safe.

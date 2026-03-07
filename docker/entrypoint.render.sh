#!/bin/sh
# Render / HuggingFace entrypoint
# ─────────────────────────────────────────────────────────────────────────────
# When DATABASE_URL is set (PostgreSQL) → no config volume needed.
# When DATABASE_URL is absent           → SQLite; writes data/flow.db locally.
#
# In both cases this script generates config/setting.toml from env vars if the
# file is not already present (e.g. from a mounted Render Disk).
# ─────────────────────────────────────────────────────────────────────────────
set -e

CONFIG_DIR=/app/config
SETTING_FILE=$CONFIG_DIR/setting.toml

mkdir -p "$CONFIG_DIR"

if [ ! -f "$SETTING_FILE" ]; then
  cat > "$SETTING_FILE" <<TOML
[global]
api_key        = "${FLOW2API_API_KEY:-han1234}"
admin_username = "${FLOW2API_ADMIN_USERNAME:-admin}"
admin_password = "${FLOW2API_ADMIN_PASSWORD:-admin}"

[flow]
labs_base_url  = "https://labs.google/fx/api"
api_base_url   = "https://aisandbox-pa.googleapis.com/v1"
timeout        = ${FLOW2API_FLOW_TIMEOUT:-120}
max_retries    = 3
image_request_timeout         = 40
image_timeout_retry_count     = 1
image_timeout_retry_delay     = 0.8
image_timeout_use_media_proxy_fallback = true
image_prefer_media_proxy      = false
image_slot_wait_timeout       = 480
image_launch_soft_limit       = 20
image_launch_wait_timeout     = 480
image_launch_stagger_ms       = 0
video_slot_wait_timeout       = 480
video_launch_soft_limit       = 20
video_launch_wait_timeout     = 480
video_launch_stagger_ms       = 0
poll_interval     = 3.0
max_poll_attempts = 200

[server]
host = "0.0.0.0"
port = 8000

[debug]
enabled       = false
log_requests  = true
log_responses = true
mask_token    = true

[proxy]
proxy_enabled = ${FLOW2API_PROXY_ENABLED:-false}
proxy_url     = "${FLOW2API_PROXY_URL:-}"

[generation]
image_timeout = ${FLOW2API_IMAGE_TIMEOUT:-300}
video_timeout = ${FLOW2API_VIDEO_TIMEOUT:-1500}

[admin]
error_ban_threshold = 3

[cache]
enabled  = false
timeout  = 7200
base_url = ""

[captcha]
captcha_method                   = "${FLOW2API_CAPTCHA_METHOD:-yescaptcha}"
browser_recaptcha_settle_seconds = 3.0
yescaptcha_api_key               = "${FLOW2API_YESCAPTCHA_API_KEY:-}"
yescaptcha_base_url              = "https://api.yescaptcha.com"
remote_browser_base_url          = "${FLOW2API_REMOTE_BROWSER_URL:-}"
remote_browser_api_key           = "${FLOW2API_REMOTE_BROWSER_API_KEY:-}"
remote_browser_timeout           = ${FLOW2API_REMOTE_BROWSER_TIMEOUT:-60}
TOML
  echo "[entrypoint] Generated $SETTING_FILE from environment variables."
else
  echo "[entrypoint] Using existing $SETTING_FILE."
fi

if [ -n "$DATABASE_URL" ]; then
  echo "[entrypoint] DATABASE_URL detected — using PostgreSQL (Neon)."
else
  echo "[entrypoint] No DATABASE_URL — using SQLite at /app/data/flow.db."
  mkdir -p /app/data
fi

exec python main.py

---
title: Flow2API
emoji: 🌊
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
---

# Flow2API on HuggingFace Spaces

## Required Secrets (Settings → Repository Secrets)

| Secret | Description |
|---|---|
| `DATABASE_URL` | Neon PostgreSQL URL: `postgresql://user:pass@host/db?sslmode=require` |
| `FLOW2API_API_KEY` | Your API key for client authentication |
| `FLOW2API_ADMIN_PASSWORD` | Admin panel password |
| `FLOW2API_CAPTCHA_METHOD` | `yescaptcha` (browser mode won't work here) |
| `FLOW2API_YESCAPTCHA_API_KEY` | Your YesCaptcha key |

HuggingFace Spaces injects `$PORT=7860` automatically — the app reads it.

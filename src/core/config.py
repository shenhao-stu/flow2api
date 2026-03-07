"""Configuration management for Flow2API"""
import os
import tomli
from pathlib import Path
from typing import Dict, Any, Optional

# ---------------------------------------------------------------------------
# Environment variable helpers
# ---------------------------------------------------------------------------
# When setting.toml is absent (e.g. on Render), the app builds a default
# configuration from these environment variables.
# Pattern:  FLOW2API_<SECTION>_<KEY>  (all uppercase)
# ---------------------------------------------------------------------------


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    val = os.environ.get(key)
    if val is not None:
        try:
            return int(val)
        except ValueError:
            pass
    return default


def _env_bool(key: str, default: bool) -> bool:
    val = os.environ.get(key)
    if val is not None:
        return val.lower() in ("1", "true", "yes")
    return default


class Config:
    """Application configuration — reads setting.toml, then applies env var overrides."""

    def __init__(self):
        self._config = self._load_config()
        self._admin_username: Optional[str] = None
        self._admin_password: Optional[str] = None

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from setting.toml, or return defaults if the file is absent."""
        config_path = Path(__file__).parent.parent.parent / "config" / "setting.toml"
        if config_path.exists():
            with open(config_path, "rb") as f:
                return tomli.load(f)
        # No TOML file present — build minimal defaults from environment variables
        # so the app can start on Render / Railway without a mounted config volume.
        return self._default_config()

    def _default_config(self) -> Dict[str, Any]:
        """Minimal default configuration used when setting.toml is absent."""
        return {
            "global": {
                "api_key": _env("FLOW2API_API_KEY", "han1234"),
                "admin_username": _env("FLOW2API_ADMIN_USERNAME", "admin"),
                "admin_password": _env("FLOW2API_ADMIN_PASSWORD", "admin"),
            },
            "flow": {
                "labs_base_url": "https://labs.google/fx/api",
                "api_base_url": "https://aisandbox-pa.googleapis.com/v1",
                "timeout": _env_int("FLOW2API_FLOW_TIMEOUT", 120),
                "max_retries": _env_int("FLOW2API_FLOW_MAX_RETRIES", 3),
                "image_request_timeout": _env_int("FLOW2API_IMAGE_REQUEST_TIMEOUT", 40),
                "image_timeout_retry_count": 1,
                "image_timeout_retry_delay": 0.8,
                "image_timeout_use_media_proxy_fallback": True,
                "image_prefer_media_proxy": False,
                "image_slot_wait_timeout": 480,
                "image_launch_soft_limit": 20,
                "image_launch_wait_timeout": 480,
                "image_launch_stagger_ms": 0,
                "video_slot_wait_timeout": 480,
                "video_launch_soft_limit": 20,
                "video_launch_wait_timeout": 480,
                "video_launch_stagger_ms": 0,
                "poll_interval": 3.0,
                "max_poll_attempts": 200,
            },
            "server": {
                "host": _env("FLOW2API_SERVER_HOST", "0.0.0.0"),
                # Render injects $PORT automatically; fall back to FLOW2API_SERVER_PORT or 8000
                "port": _env_int("PORT", _env_int("FLOW2API_SERVER_PORT", 8000)),
            },
            "debug": {
                "enabled": _env_bool("FLOW2API_DEBUG", False),
                "log_requests": True,
                "log_responses": True,
                "mask_token": True,
            },
            "proxy": {
                "proxy_enabled": _env_bool("FLOW2API_PROXY_ENABLED", False),
                "proxy_url": _env("FLOW2API_PROXY_URL", ""),
            },
            "generation": {
                "image_timeout": _env_int("FLOW2API_IMAGE_TIMEOUT", 300),
                "video_timeout": _env_int("FLOW2API_VIDEO_TIMEOUT", 1500),
            },
            "admin": {
                "error_ban_threshold": _env_int("FLOW2API_ERROR_BAN_THRESHOLD", 3),
            },
            "cache": {
                "enabled": _env_bool("FLOW2API_CACHE_ENABLED", False),
                "timeout": _env_int("FLOW2API_CACHE_TIMEOUT", 7200),
                "base_url": _env("FLOW2API_CACHE_BASE_URL", ""),
            },
            "captcha": {
                "captcha_method": _env("FLOW2API_CAPTCHA_METHOD", "browser"),
                "browser_recaptcha_settle_seconds": 3.0,
                "yescaptcha_api_key": _env("FLOW2API_YESCAPTCHA_API_KEY", ""),
                "yescaptcha_base_url": "https://api.yescaptcha.com",
                "remote_browser_base_url": _env("FLOW2API_REMOTE_BROWSER_URL", ""),
                "remote_browser_api_key": _env("FLOW2API_REMOTE_BROWSER_API_KEY", ""),
                "remote_browser_timeout": _env_int("FLOW2API_REMOTE_BROWSER_TIMEOUT", 60),
            },
        }

    def reload_config(self):
        """Reload configuration from file (or re-apply defaults)."""
        self._config = self._load_config()

    def get_raw_config(self) -> Dict[str, Any]:
        """Get raw configuration dictionary."""
        return self._config

    @property
    def admin_username(self) -> str:
        if self._admin_username is not None:
            return self._admin_username
        return self._config["global"]["admin_username"]

    @admin_username.setter
    def admin_username(self, value: str):
        self._admin_username = value
        self._config["global"]["admin_username"] = value

    def set_admin_username_from_db(self, username: str):
        """Set admin username from database."""
        self._admin_username = username

    # Flow2API specific properties
    @property
    def flow_labs_base_url(self) -> str:
        return self._config["flow"]["labs_base_url"]

    @property
    def flow_api_base_url(self) -> str:
        return self._config["flow"]["api_base_url"]

    @property
    def flow_timeout(self) -> int:
        timeout = self._config.get("flow", {}).get("timeout", 120)
        try:
            return max(5, int(timeout))
        except Exception:
            return 120

    @property
    def flow_max_retries(self) -> int:
        retries = self._config.get("flow", {}).get("max_retries", 3)
        try:
            return max(1, int(retries))
        except Exception:
            return 3

    @property
    def flow_image_request_timeout(self) -> int:
        default_timeout = min(self.flow_timeout, 40)
        timeout = self._config.get("flow", {}).get("image_request_timeout", default_timeout)
        try:
            return max(5, int(timeout))
        except Exception:
            return self.flow_timeout

    @property
    def flow_image_timeout_retry_count(self) -> int:
        retry_count = self._config.get("flow", {}).get("image_timeout_retry_count", 1)
        try:
            return max(0, min(3, int(retry_count)))
        except Exception:
            return 1

    @property
    def flow_image_timeout_retry_delay(self) -> float:
        delay = self._config.get("flow", {}).get("image_timeout_retry_delay", 0.8)
        try:
            return max(0.0, min(5.0, float(delay)))
        except Exception:
            return 0.8

    @property
    def flow_image_timeout_use_media_proxy_fallback(self) -> bool:
        return bool(
            self._config.get("flow", {}).get("image_timeout_use_media_proxy_fallback", True)
        )

    @property
    def flow_image_prefer_media_proxy(self) -> bool:
        return bool(self._config.get("flow", {}).get("image_prefer_media_proxy", False))

    @property
    def flow_image_slot_wait_timeout(self) -> float:
        timeout = self._config.get("flow", {}).get("image_slot_wait_timeout", 120)
        try:
            return max(1.0, min(600.0, float(timeout)))
        except Exception:
            return 120.0

    @property
    def flow_image_launch_soft_limit(self) -> int:
        value = self._config.get("flow", {}).get("image_launch_soft_limit", 0)
        try:
            return max(0, min(200, int(value)))
        except Exception:
            return 0

    @property
    def flow_image_launch_wait_timeout(self) -> float:
        timeout = self._config.get("flow", {}).get("image_launch_wait_timeout", 180)
        try:
            return max(1.0, min(600.0, float(timeout)))
        except Exception:
            return 180.0

    @property
    def flow_image_launch_stagger_ms(self) -> int:
        value = self._config.get("flow", {}).get("image_launch_stagger_ms", 0)
        try:
            return max(0, min(5000, int(value)))
        except Exception:
            return 0

    @property
    def flow_video_slot_wait_timeout(self) -> float:
        timeout = self._config.get("flow", {}).get("video_slot_wait_timeout", 120)
        try:
            return max(1.0, min(600.0, float(timeout)))
        except Exception:
            return 120.0

    @property
    def flow_video_launch_soft_limit(self) -> int:
        value = self._config.get("flow", {}).get("video_launch_soft_limit", 0)
        try:
            return max(0, min(200, int(value)))
        except Exception:
            return 0

    @property
    def flow_video_launch_wait_timeout(self) -> float:
        timeout = self._config.get("flow", {}).get("video_launch_wait_timeout", 180)
        try:
            return max(1.0, min(600.0, float(timeout)))
        except Exception:
            return 180.0

    @property
    def flow_video_launch_stagger_ms(self) -> int:
        value = self._config.get("flow", {}).get("video_launch_stagger_ms", 0)
        try:
            return max(0, min(5000, int(value)))
        except Exception:
            return 0

    @property
    def poll_interval(self) -> float:
        return self._config["flow"]["poll_interval"]

    @property
    def max_poll_attempts(self) -> int:
        return self._config["flow"]["max_poll_attempts"]

    @property
    def server_host(self) -> str:
        return self._config["server"]["host"]

    @property
    def server_port(self) -> int:
        return self._config["server"]["port"]

    @property
    def debug_enabled(self) -> bool:
        return self._config.get("debug", {}).get("enabled", False)

    @property
    def debug_log_requests(self) -> bool:
        return self._config.get("debug", {}).get("log_requests", True)

    @property
    def debug_log_responses(self) -> bool:
        return self._config.get("debug", {}).get("log_responses", True)

    @property
    def debug_mask_token(self) -> bool:
        return self._config.get("debug", {}).get("mask_token", True)

    # Mutable runtime properties
    @property
    def api_key(self) -> str:
        return self._config["global"]["api_key"]

    @api_key.setter
    def api_key(self, value: str):
        self._config["global"]["api_key"] = value

    @property
    def admin_password(self) -> str:
        if self._admin_password is not None:
            return self._admin_password
        return self._config["global"]["admin_password"]

    @admin_password.setter
    def admin_password(self, value: str):
        self._admin_password = value
        self._config["global"]["admin_password"] = value

    def set_admin_password_from_db(self, password: str):
        """Set admin password from database."""
        self._admin_password = password

    def set_debug_enabled(self, enabled: bool):
        if "debug" not in self._config:
            self._config["debug"] = {}
        self._config["debug"]["enabled"] = enabled

    @property
    def image_timeout(self) -> int:
        return self._config.get("generation", {}).get("image_timeout", 300)

    def set_image_timeout(self, timeout: int):
        if "generation" not in self._config:
            self._config["generation"] = {}
        self._config["generation"]["image_timeout"] = timeout

    @property
    def video_timeout(self) -> int:
        return self._config.get("generation", {}).get("video_timeout", 1500)

    def set_video_timeout(self, timeout: int):
        if "generation" not in self._config:
            self._config["generation"] = {}
        self._config["generation"]["video_timeout"] = timeout

    @property
    def upsample_timeout(self) -> int:
        return self._config.get("generation", {}).get("upsample_timeout", 300)

    def set_upsample_timeout(self, timeout: int):
        if "generation" not in self._config:
            self._config["generation"] = {}
        self._config["generation"]["upsample_timeout"] = timeout

    # Cache configuration
    @property
    def cache_enabled(self) -> bool:
        return self._config.get("cache", {}).get("enabled", False)

    def set_cache_enabled(self, enabled: bool):
        if "cache" not in self._config:
            self._config["cache"] = {}
        self._config["cache"]["enabled"] = enabled

    @property
    def cache_timeout(self) -> int:
        return self._config.get("cache", {}).get("timeout", 7200)

    def set_cache_timeout(self, timeout: int):
        if "cache" not in self._config:
            self._config["cache"] = {}
        self._config["cache"]["timeout"] = timeout

    @property
    def cache_base_url(self) -> str:
        return self._config.get("cache", {}).get("base_url", "")

    def set_cache_base_url(self, base_url: str):
        if "cache" not in self._config:
            self._config["cache"] = {}
        self._config["cache"]["base_url"] = base_url

    # Captcha configuration
    @property
    def captcha_method(self) -> str:
        return self._config.get("captcha", {}).get("captcha_method", "yescaptcha")

    def set_captcha_method(self, method: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["captcha_method"] = method

    @property
    def browser_launch_background(self) -> bool:
        return self._config.get("captcha", {}).get("browser_launch_background", True)

    def set_browser_launch_background(self, enabled: bool):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["browser_launch_background"] = bool(enabled)

    @property
    def browser_recaptcha_settle_seconds(self) -> float:
        value = self._config.get("captcha", {}).get("browser_recaptcha_settle_seconds", 3.0)
        try:
            return max(0.0, min(10.0, float(value)))
        except Exception:
            return 3.0

    @property
    def browser_idle_ttl_seconds(self) -> int:
        value = self._config.get("captcha", {}).get("browser_idle_ttl_seconds", 600)
        try:
            return max(60, int(value))
        except Exception:
            return 600

    @property
    def yescaptcha_api_key(self) -> str:
        return self._config.get("captcha", {}).get("yescaptcha_api_key", "")

    def set_yescaptcha_api_key(self, api_key: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["yescaptcha_api_key"] = api_key

    @property
    def yescaptcha_base_url(self) -> str:
        return self._config.get("captcha", {}).get("yescaptcha_base_url", "https://api.yescaptcha.com")

    def set_yescaptcha_base_url(self, base_url: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["yescaptcha_base_url"] = base_url

    @property
    def capmonster_api_key(self) -> str:
        return self._config.get("captcha", {}).get("capmonster_api_key", "")

    def set_capmonster_api_key(self, api_key: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["capmonster_api_key"] = api_key

    @property
    def capmonster_base_url(self) -> str:
        return self._config.get("captcha", {}).get("capmonster_base_url", "https://api.capmonster.cloud")

    def set_capmonster_base_url(self, base_url: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["capmonster_base_url"] = base_url

    @property
    def ezcaptcha_api_key(self) -> str:
        return self._config.get("captcha", {}).get("ezcaptcha_api_key", "")

    def set_ezcaptcha_api_key(self, api_key: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["ezcaptcha_api_key"] = api_key

    @property
    def ezcaptcha_base_url(self) -> str:
        return self._config.get("captcha", {}).get("ezcaptcha_base_url", "https://api.ez-captcha.com")

    def set_ezcaptcha_base_url(self, base_url: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["ezcaptcha_base_url"] = base_url

    @property
    def capsolver_api_key(self) -> str:
        return self._config.get("captcha", {}).get("capsolver_api_key", "")

    def set_capsolver_api_key(self, api_key: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["capsolver_api_key"] = api_key

    @property
    def capsolver_base_url(self) -> str:
        return self._config.get("captcha", {}).get("capsolver_base_url", "https://api.capsolver.com")

    def set_capsolver_base_url(self, base_url: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["capsolver_base_url"] = base_url

    @property
    def remote_browser_base_url(self) -> str:
        return self._config.get("captcha", {}).get("remote_browser_base_url", "")

    def set_remote_browser_base_url(self, base_url: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["remote_browser_base_url"] = (base_url or "").strip()

    @property
    def remote_browser_api_key(self) -> str:
        return self._config.get("captcha", {}).get("remote_browser_api_key", "")

    def set_remote_browser_api_key(self, api_key: str):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["remote_browser_api_key"] = (api_key or "").strip()

    @property
    def remote_browser_timeout(self) -> int:
        timeout = self._config.get("captcha", {}).get("remote_browser_timeout", 60)
        try:
            return max(5, int(timeout))
        except Exception:
            return 60

    def set_remote_browser_timeout(self, timeout: int):
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        try:
            normalized = max(5, int(timeout))
        except Exception:
            normalized = 60
        self._config["captcha"]["remote_browser_timeout"] = normalized


# Global config instance
config = Config()

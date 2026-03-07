"""Flow2API - Main Entry Point"""
from src.main import app
import uvicorn
import os

if __name__ == "__main__":
    from src.core.config import config

    # Render (and other PaaS) inject $PORT; honour it over TOML/default
    port = int(os.environ.get("PORT", config.server_port))

    uvicorn.run(
        "src.main:app",
        host=config.server_host,
        port=port,
        reload=False
    )

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


@dataclass(frozen=True)
class BotConfig:
    bot_token: str
    comfyui_http_url: str
    comfyui_ws_url: str
    data_dir: Path
    output_dir: Path
    shared_output_dir: Path
    persistence_path: Path
    restart_command: Optional[str] = None
    workflow_templates_dir: Optional[Path] = None


def load_config() -> BotConfig:
    if load_dotenv:
        load_dotenv()

    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required. Provide it via environment or .env file.")

    comfy_host = os.getenv("COMFYUI_HOST", "http://127.0.0.1:8000")
    comfy_ws = os.getenv("COMFYUI_WS", "ws://127.0.0.1:8000/ws")

    base_dir = Path(os.getenv("DATA_DIR", "data")).resolve()
    output_dir = Path(os.getenv("OUTPUT_DIR", "Output")).resolve()
    shared_output = Path(os.getenv("COMFYUI_SHARED_OUTPUT_DIR", str(output_dir))).resolve()
    persistence_name = os.getenv("PERSISTENCE_FILE", "bot_state.pkl")

    restart_cmd = os.getenv("COMFYUI_RESTART_CMD")
    templates_dir_env = os.getenv("COMFYUI_WORKFLOW_TEMPLATES_DIR")
    templates_dir: Optional[Path] = None
    if templates_dir_env:
        templates_dir = Path(templates_dir_env).expanduser().resolve()

    config = BotConfig(
        bot_token=bot_token,
        comfyui_http_url=comfy_host.rstrip("/"),
        comfyui_ws_url=comfy_ws,
        data_dir=base_dir,
        output_dir=output_dir,
        shared_output_dir=shared_output,
        persistence_path=base_dir / persistence_name,
        restart_command=restart_cmd,
        workflow_templates_dir=templates_dir,
    )

    ensure_directories(config)
    return config


def ensure_directories(config: BotConfig) -> None:
    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        config.shared_output_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Shared directories may live on removable drives managed outside the bot
        pass

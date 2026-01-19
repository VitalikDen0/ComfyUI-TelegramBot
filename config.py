import os
import shutil
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
    check_comfy_running: bool = False
    webapp_url: Optional[str] = None
    webapp_api_host: str = "0.0.0.0"
    webapp_api_port: int = 8081
    webapp_api_enabled: bool = True
    webapp_serve_enabled: bool = False
    webapp_serve_path: Optional[Path] = None
    restart_command: Optional[str] = None
    workflow_templates_dir: Optional[Path] = None
    default_workflow_path: Optional[Path] = None


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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
    default_workflow_env = os.getenv("DEFAULT_WORKFLOW_FILE")
    check_comfy_running = _env_bool("CHECK_COMFY_RUNNING", default=False)
    webapp_url = os.getenv("WEBAPP_URL")
    webapp_api_host = os.getenv("WEBAPP_API_HOST", "0.0.0.0")
    webapp_api_port = int(os.getenv("WEBAPP_API_PORT", "8081"))
    webapp_api_enabled = _env_bool("WEBAPP_API_ENABLED", default=True)
    webapp_serve_enabled = _env_bool("WEBAPP_SERVE_ENABLED", default=False)
    webapp_serve_path_env = os.getenv("WEBAPP_SERVE_PATH")

    if templates_dir_env:
        templates_dir = Path(templates_dir_env).expanduser().resolve()
    else:
        templates_dir = (base_dir / "templates").resolve()

    if webapp_serve_path_env:
        webapp_serve_path = Path(webapp_serve_path_env).expanduser().resolve()
    else:
        webapp_serve_path = Path(__file__).resolve().parent / "webapp" / "dist"

    if default_workflow_env:
        default_workflow_path = Path(default_workflow_env).expanduser().resolve()
    else:
        default_workflow_path = (base_dir / "workflows" / "default.json").resolve()

    legacy_default = base_dir / "default.json"
    if not default_workflow_path.exists() and legacy_default.exists():
        default_workflow_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(legacy_default, default_workflow_path)

    config = BotConfig(
        bot_token=bot_token,
        comfyui_http_url=comfy_host.rstrip("/"),
        comfyui_ws_url=comfy_ws,
        data_dir=base_dir,
        output_dir=output_dir,
        shared_output_dir=shared_output,
        persistence_path=base_dir / persistence_name,
        check_comfy_running=check_comfy_running,
        webapp_url=webapp_url,
        webapp_api_host=webapp_api_host,
        webapp_api_port=webapp_api_port,
        webapp_api_enabled=webapp_api_enabled,
        webapp_serve_enabled=webapp_serve_enabled,
        webapp_serve_path=webapp_serve_path,
        restart_command=restart_cmd,
        workflow_templates_dir=templates_dir,
        default_workflow_path=default_workflow_path,
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
    if config.workflow_templates_dir:
        config.workflow_templates_dir.mkdir(parents=True, exist_ok=True)
    if config.default_workflow_path:
        config.default_workflow_path.parent.mkdir(parents=True, exist_ok=True)

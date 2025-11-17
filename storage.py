import json
import logging
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from telegram import Update


LOGGER = logging.getLogger(__name__)


class WorkflowStorage:
    """Filesystem-backed workflow storage per Telegram user."""

    def __init__(self, base_dir: Path, *, default_workflow_path: Optional[Path] = None) -> None:
        self._base_dir = base_dir
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._default_workflow_path = default_workflow_path
        self._default_cache: Optional[Dict] = None

    def user_dir(self, user_id: int) -> Path:
        return self._base_dir / str(user_id)

    def workflow_path(self, user_id: int, name: str = "default") -> Path:
        return self.user_dir(user_id) / f"{name}.json"

    def _version_dir(self, user_id: int, name: str) -> Path:
        return self.user_dir(user_id) / "versions" / name

    def ensure_user_dir(self, user_id: int) -> Path:
        path = self.user_dir(user_id)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def history_path(self, user_id: int) -> Path:
        return self.user_dir(user_id) / "history.json"

    def list_workflows(self, user_id: int) -> Iterable[str]:
        path = self.user_dir(user_id)
        if not path.exists():
            return []
        return sorted(p.stem for p in path.glob("*.json"))

    def has_workflow(self, user_id: int, name: str = "default") -> bool:
        return self.workflow_path(user_id, name).exists()

    def load_workflow(self, user_id: int, name: str = "default") -> Optional[Dict]:
        path = self.workflow_path(user_id, name)
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as fp:
            return json.load(fp)

    def load_default_workflow(self) -> Optional[Dict]:
        path = self._default_workflow_path
        if path is None or not path.exists():
            return None
        if self._default_cache is None:
            try:
                with path.open("r", encoding="utf-8") as fp:
                    payload = json.load(fp)
            except (OSError, json.JSONDecodeError) as exc:
                LOGGER.warning("Не удалось прочитать default workflow из %s", path, exc_info=True)
                return None
            if not isinstance(payload, dict):
                LOGGER.warning("Default workflow %s не является JSON-объектом", path)
                return None
            self._default_cache = payload
        return deepcopy(self._default_cache)

    def save_workflow(self, user_id: int, workflow: Dict, name: str = "default") -> Path:
        path = self.workflow_path(user_id, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            self._snapshot_version(user_id, name, path)
        with path.open("w", encoding="utf-8") as fp:
            json.dump(workflow, fp, ensure_ascii=False, indent=2)
        return path

    def delete_workflow(self, user_id: int, name: str = "default") -> None:
        path = self.workflow_path(user_id, name)
        if path.exists():
            path.unlink()
        version_dir = self._version_dir(user_id, name)
        if version_dir.exists():
            shutil.rmtree(version_dir, ignore_errors=True)

    def ensure_default_workflow_for_user(self, user_id: int, name: str = "default") -> Optional[Dict]:
        existing = self.load_workflow(user_id, name)
        if existing is not None:
            return existing

        default_workflow = self.load_default_workflow()
        if default_workflow is None:
            LOGGER.debug("Default workflow недоступен — пропускаем автоинициализацию")
            return None

        try:
            self.save_workflow(user_id, default_workflow, name)
        except Exception:
            LOGGER.exception("Не удалось сохранить default workflow для пользователя %s", user_id)
            return None

        LOGGER.info("Создан workflow по умолчанию для пользователя %s", user_id)
        return self.load_workflow(user_id, name)

    def append_history(self, user_id: int, entry: Dict, *, limit: int = 100) -> None:
        path = self.history_path(user_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        history = self._load_history(user_id)
        now = datetime.now(timezone.utc)
        item = dict(entry)
        item.setdefault("created_at", now.isoformat(timespec="seconds"))
        item.setdefault("created_at_ts", now.timestamp())
        history.append(item)
        history.sort(key=lambda record: float(record.get("created_at_ts", 0)))
        if limit and len(history) > limit:
            history = history[-limit:]
        with path.open("w", encoding="utf-8") as fp:
            json.dump(history, fp, ensure_ascii=False, indent=2)

    def get_recent_history(self, user_id: int, limit: int = 5) -> Tuple[List[Dict], int]:
        history = self._load_history(user_id)
        history.sort(key=lambda record: float(record.get("created_at_ts", 0)), reverse=True)
        total = len(history)
        if limit:
            history = history[:limit]
        return history, total

    def _load_history(self, user_id: int) -> List[Dict]:
        path = self.history_path(user_id)
        if not path.exists():
            return []
        try:
            with path.open("r", encoding="utf-8") as fp:
                data = json.load(fp)
        except json.JSONDecodeError:
            return []
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def _snapshot_version(self, user_id: int, name: str, path: Path) -> Optional[Path]:
        if not path.exists():
            return None
        version_dir = self._version_dir(user_id, name)
        version_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        version_path = version_dir / f"{timestamp}.json"
        counter = 1
        while version_path.exists():
            version_path = version_dir / f"{timestamp}_{counter}.json"
            counter += 1
        shutil.copy2(path, version_path)
        return version_path


def get_user_id(update: Update) -> int:
    if update.effective_user is None:
        raise RuntimeError("Cannot resolve user id from update")
    return update.effective_user.id

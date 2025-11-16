import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from telegram import Update


class WorkflowStorage:
    """Filesystem-backed workflow storage per Telegram user."""

    def __init__(self, base_dir: Path) -> None:
        self._base_dir = base_dir
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def user_dir(self, user_id: int) -> Path:
        return self._base_dir / str(user_id)

    def workflow_path(self, user_id: int, name: str = "default") -> Path:
        return self.user_dir(user_id) / f"{name}.json"

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

    def save_workflow(self, user_id: int, workflow: Dict, name: str = "default") -> Path:
        path = self.workflow_path(user_id, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fp:
            json.dump(workflow, fp, ensure_ascii=False, indent=2)
        return path

    def delete_workflow(self, user_id: int, name: str = "default") -> None:
        path = self.workflow_path(user_id, name)
        if path.exists():
            path.unlink()

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


def get_user_id(update: Update) -> int:
    if update.effective_user is None:
        raise RuntimeError("Cannot resolve user id from update")
    return update.effective_user.id

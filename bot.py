from __future__ import annotations

import asyncio
import os
import hashlib
import inspect
import json
import logging
import secrets
import shutil
import time
import re
from dataclasses import dataclass
from datetime import datetime
from html import escape
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, MutableMapping, Union, cast, Mapping, Sequence, Tuple

from telegram import Bot, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Message, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PicklePersistence,
    filters,
)

from comfy_client import ComfyUIClient, ExecutionResult, PreviewPayload, ProgressEvent, gather_outputs
from config import BotConfig, load_config
from storage import WorkflowStorage, get_user_id
from workflow_render import format_workflow_summary

LOGGER = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

ACTIVE_TASKS: dict[int, asyncio.Task] = {}

MessageSource = Update | Message | CallbackQuery
UserDataDict = MutableMapping[str, Any]


def get_user_data(context: ContextTypes.DEFAULT_TYPE) -> UserDataDict:
    data = context.user_data
    if data is None:
        raise RuntimeError("user_data не инициализирован")
    return cast(UserDataDict, data)

MENU_START = "menu:start"
MENU_INSTRUCTION = "menu:instruction"
MENU_CREATE = "menu:create"
MENU_OPEN = "menu:open"
MENU_IMPORT = "menu:import"
MENU_STATUS = "menu:status"
MENU_HISTORY = "menu:history"
MENU_GALLERY = "menu:gallery"
MENU_TEMPLATES = "menu:templates"
MENU_WORKFLOWS = "menu:workflows"
MENU_NOTIFICATIONS = "menu:notifications"
MENU_RESTART = "menu:restart"
MENU_BACK = "menu:back"
MENU_CANCEL = "menu:cancel"
QUEUE_STATUS = "queue:status"
QUEUE_REFRESH = "queue:refresh"
QUEUE_CLEAR = "queue:clear"
QUEUE_INTERRUPT = "queue:interrupt"
CATALOG_CATEGORY_PREFIX = "catalog:cat:"
CATALOG_NODE_PREFIX = "catalog:node:"
CATALOG_SEARCH = "catalog:search"
CATALOG_SEARCH_CANCEL = "catalog:search-cancel"
CATALOG_SEARCH_PAGE_PREFIX = "catalog:search-page:"
CATALOG_PAGE_PREFIX = "catalog:page:"
CATALOG_NODE_PAGE_PREFIX = "catalog:npage:"
CATALOG_REFRESH = "catalog:refresh"
CATALOG_BACK = "catalog:back"
CATALOG_NOOP = "catalog:noop"
GALLERY_PAGE_PREFIX = "gallery:page:"
TEMPLATE_CATEGORY_PREFIX = "template:cat:"
TEMPLATE_PAGE_PREFIX = "template:page:"
TEMPLATE_SELECT_PREFIX = "template:select:"
TEMPLATE_REFRESH = "template:refresh"
TEMPLATE_ERROR_KEY = "template_error"
TEMPLATE_BACK = "template:back"
WORKFLOW_SELECT_PREFIX = "workflow:select:"
WORKFLOW_LIBRARY_PAGE_PREFIX = "workflow:page:"
NOTIFY_TOGGLE_PREFIX = "notify:toggle:"

WORKFLOW_LAUNCH = "workflow:launch"
WORKFLOW_NODE_PREFIX = "workflow:node:"  # -> workflow:node:<nodeId>
WORKFLOW_PARAM_PREFIX = "workflow:param:"  # -> workflow:param:<nodeId>:<param>
WORKFLOW_PARAM_QUICK_PREFIX = "workflow:param-quick:"
WORKFLOW_PARAM_PAGE_PREFIX = "workflow:param-page:"
WORKFLOW_ADD_NODE = "workflow:add-node"
WORKFLOW_EXPORT = "workflow:export"
WORKFLOW_REFRESH = "workflow:refresh"
WORKFLOW_CANCEL = "workflow:cancel"
WORKFLOW_CONNECT_NODE_PREFIX = "workflow:connect-node:"
CONNECTION_INPUT_PREFIX = "conn:input:"
CONNECTION_SOURCE_PREFIX = "conn:source:"
CONNECTION_SOURCE_PAGE_PREFIX = "conn:src-page:"
CONNECTION_OUTPUT_PREFIX = "conn:output:"
CONNECTION_BACK = "conn:back"
CONNECTION_CLEAR = "conn:clear"

PROGRESS_UPDATE_INTERVAL_SECONDS = 1.0  # Telegram спокойно переваривает обновления прогресса/латентов раз в секунду

CATEGORY_PAGE_SIZE = 8
NODE_PAGE_SIZE = 6
SEARCH_PAGE_SIZE = 6
GALLERY_PAGE_SIZE = 10
TEMPLATE_PAGE_SIZE = 6
WORKFLOW_LIBRARY_PAGE_SIZE = 9
CONNECTION_PAGE_SIZE = 6
PARAM_CHOICES_PAGE_SIZE = 10

GALLERY_STATE_KEY = "gallery_state"

MODEL_PARAM_TYPE_BY_NODE: Dict[tuple[str, str], str] = {
    ("CheckpointLoaderSimple", "ckpt_name"): "checkpoints",
    ("CheckpointLoader", "ckpt_name"): "checkpoints",
    ("CheckpointLoaderSimple", "vae_name"): "vae",
    ("CheckpointLoader", "vae_name"): "vae",
    ("CheckpointLoaderSimple", "clip_name"): "clip",
    ("CheckpointLoader", "clip_name"): "clip",
    ("LoraLoader", "lora_name"): "loras",
    ("LoRALoader", "lora_name"): "loras",
}

GENERIC_MODEL_PARAM_TYPES: Dict[str, str] = {
    "ckpt_name": "checkpoints",
    "vae_name": "vae",
    "clip_name": "clip",
    "lora_name": "loras",
}

CATALOG_CACHE_FILE = "object_info_cache.json"
CATALOG_CACHE_TTL_SECONDS = 3600

MAX_COMFY_SEED_VALUE = 2**64 - 1

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif"}
TELEGRAM_PHOTO_SIZE_LIMIT = 10 * 1024 * 1024

ButtonAction = Tuple[Any, ...]


def _set_dynamic_buttons(context: ContextTypes.DEFAULT_TYPE, mapping: Mapping[str, ButtonAction]) -> None:
    get_user_data(context)["dynamic_buttons"] = dict(mapping)


def _get_dynamic_action(context: ContextTypes.DEFAULT_TYPE, text: str) -> Optional[ButtonAction]:
    actions = get_user_data(context).get("dynamic_buttons")
    if isinstance(actions, dict):
        return actions.get(text)
    return None


def _clear_dynamic_buttons(context: ContextTypes.DEFAULT_TYPE) -> None:
    get_user_data(context).pop("dynamic_buttons", None)


async def _dispatch_dynamic_action(
    source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    action: ButtonAction,
) -> bool:
    kind = action[0] if action else None
    if kind is None:
        return False
    if kind == "node_param":
        _, node_id, parameter = action
        await prompt_param_update(source, context, str(node_id), str(parameter))
        return True
    if kind == "node_connections":
        _, node_id = action
        await show_connection_inputs(source, context, str(node_id))
        return True
    if kind == "node_back":
        user_id = get_user_id_from_source(source)
        _clear_dynamic_buttons(context)
        await show_workflow_overview(source, context, node_message=True)
        await _ensure_keyboard_mode(source, context, user_id, "workflow")
        return True
    if kind == "node_delete":
        _, node_id = action
        await _prompt_node_delete(source, context, str(node_id))
        return True
    if kind == "node_delete_confirm":
        _, node_id = action
        await _delete_node_confirmed(source, context, str(node_id))
        return True
    if kind == "node_delete_cancel":
        _, node_id = action
        await show_node_details(source, context, str(node_id))
        return True
    if kind == "node_details":
        _, node_id = action
        await show_node_details(source, context, str(node_id))
        return True
    if kind == "cancel_input":
        await cancel_pending_input(source, context)
        return True
    if kind == "param_manual":
        _, node_id, parameter = action
        await prompt_manual_param_input(source, context, str(node_id), str(parameter))
        return True
    if kind == "param_quick":
        _, node_id, parameter, index = action
        await apply_quick_param_choice(source, context, str(node_id), str(parameter), int(index))
        return True
    if kind == "conn_input":
        _, node_id, input_name = action
        await start_connection_selection(source, context, str(node_id), str(input_name))
        return True
    if kind == "conn_back":
        await connection_back(source, context)
        return True
    if kind == "conn_clear":
        await clear_connection_choice(source, context)
        return True
    if kind == "conn_source":
        _, index = action
        await pick_connection_source(source, context, int(index))
        return True
    if kind == "conn_source_page":
        _, page = action
        await show_connection_source_picker(source, context, page= int(page))
        return True
    if kind == "conn_output":
        _, index = action
        await apply_connection_choice(source, context, int(index))
        return True
    if kind == "conn_output_back":
        await connection_back(source, context)
        return True
    if kind == "noop":
        return True
    if kind == "run_cancel":
        await cancel_workflow(source, context)
        return True
    if kind == "run_refresh":
        await show_workflow_overview(source, context, refresh=True)
        return True
    if kind == "run_launch":
        await launch_workflow(source, context)
        return True
    if kind == "catalog_category":
        _, category_index = action
        await show_catalog_nodes(source, context, int(category_index))
        return True
    if kind == "catalog_page":
        _, page = action
        await show_node_categories(source, context, page=int(page))
        return True
    if kind == "catalog_refresh":
        last_page = int(get_user_data(context).get("catalog_last_page") or 0)
        await show_node_categories(source, context, page=last_page, refresh=True)
        return True
    if kind == "catalog_search":
        await prompt_catalog_search(source, context)
        return True
    if kind == "catalog_back":
        last_page = int(get_user_data(context).get("catalog_last_page") or 0)
        await show_node_categories(source, context, page=last_page)
        return True
    if kind == "workflow_overview":
        await show_workflow_overview(source, context)
        return True
    if kind == "catalog_node":
        _, category_index, node_index = action
        await add_catalog_node(source, context, int(category_index), int(node_index))
        return True
    if kind == "catalog_node_page":
        _, category_index, page = action
        await show_catalog_nodes(source, context, int(category_index), page=int(page))
        return True
    if kind == "catalog_search_page":
        _, page = action
        await show_catalog_search_results(source, context, page=int(page))
        return True
    if kind == "catalog_search_cancel":
        get_user_data(context).pop("awaiting_catalog_search", None)
        get_user_data(context).pop("catalog_search_results", None)
        last_page = int(get_user_data(context).get("catalog_last_page") or 0)
        await show_node_categories(source, context, page=last_page)
        return True
    if kind == "import_cancel":
        user_data = get_user_data(context)
        user_data.pop("awaiting_import", None)
        await send_main_menu(source, context, get_user_id_from_source(source))
        return True
    return False

def _format_size(value: Optional[int]) -> str:
    if value is None or value < 0:
        return "неизвестный размер"
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "Б":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} ТБ"


def _photo_too_large_message(filename: str, size: Optional[int]) -> str:
    limit_text = _format_size(TELEGRAM_PHOTO_SIZE_LIMIT)
    if size and size > 0:
        size_text = _format_size(size)
        return (
            f"⚠️ Изображение {filename} весит {size_text}, что превышает лимит {limit_text} для фото в Telegram. "
            "Отправляю файл как документ без сжатия."
        )
    return (
        f"⚠️ Telegram не принял изображение {filename} как фото (лимит {limit_text}). "
        "Отправляю файл как документ без сжатия."
    )


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "item"
    text = _SLUG_RE.sub("-", text)
    text = text.strip("-")
    return text or "item"


def _sanitize_filename(value: str) -> str:
    text = str(value or "workflow").strip()
    if not text:
        text = "workflow"
    cleaned = re.sub(r"[^0-9A-Za-z_.-]+", "_", text)
    cleaned = cleaned.strip("._")
    return cleaned or "workflow"


def _copy_workflow_graph(graph: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(graph, ensure_ascii=False))


def _unique_workflow_name(storage: WorkflowStorage, user_id: int, base_name: str) -> str:
    sanitized = _sanitize_filename(base_name)
    existing = set(storage.list_workflows(user_id))
    candidate = sanitized
    counter = 1
    while candidate in existing:
        candidate = f"{sanitized}_{counter}"
        counter += 1
    return candidate


def _template_error_keyboard(*, include_categories: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [[InlineKeyboardButton("🔄 Повторить", callback_data=TEMPLATE_REFRESH)]]
    if include_categories:
        rows.append([InlineKeyboardButton("⬅️ Категории", callback_data=TEMPLATE_BACK)])
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data=MENU_BACK)])
    return InlineKeyboardMarkup(rows)


def _format_template_error(exc: Exception) -> str:
    if isinstance(exc, PermissionError):
        details = escape(str(exc)) if str(exc) else ""
        return (
            "<b>Шаблоны недоступны</b>\n"
            "Сервер ComfyUI отклонил запрос <code>/templates</code> (403).\n"
            "Убедитесь, что API шаблонов включён (актуальная сборка ComfyUI, установлен менеджер Workflow Templates "
            "и разрешён доступ с этого хоста)."
            + (f"\n<code>{details}</code>" if details else "")
        )
    if isinstance(exc, FileNotFoundError):
        details = escape(str(exc)) if str(exc) else ""
        return (
            "<b>Шаблоны недоступны</b>\n"
            "Текущая сборка ComfyUI не поддерживает endpoint <code>/templates</code>. "
            "Обновите ComfyUI или активируйте расширение с библиотекой шаблонов."
            + (f"\n<code>{details}</code>" if details else "")
        )
    return f"⚠️ Не удалось получить шаблоны:\n<code>{escape(str(exc))}</code>"


TEMPLATE_CATEGORY_LABELS: dict[str, str] = {
    "image": "🖼️ Изображения",
    "video": "🎬 Видео",
    "audio": "🎧 Аудио",
    "animation": "🎞️ Анимация",
    "other": "📦 Прочее",
}


VIDEO_NODE_TYPES = {"SaveVideo", "SaveWEBM", "SaveAnimatedWEBP", "SaveAnimatedPNG"}
AUDIO_NODE_TYPES = {"SaveAudio", "AudioOutput"}


def _collect_node_class_types(workflow: Dict[str, Any]) -> set[str]:
    node_types: set[str] = set()
    nodes = workflow.get("nodes")
    if isinstance(nodes, dict):
        iterator = nodes.values()
    elif isinstance(nodes, list):
        iterator = (node for node in nodes if isinstance(node, dict))
    else:
        return node_types

    for node in iterator:
        class_type = node.get("class_type") or node.get("type")
        if isinstance(class_type, str):
            node_types.add(class_type)
    return node_types


def _infer_template_category(raw: Mapping[str, Any], workflow: Optional[Dict[str, Any]]) -> tuple[str, str]:
    for key in ("category", "type", "group", "mode"):
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            slug = _slugify(value)
            label = value.strip()
            mapped = TEMPLATE_CATEGORY_LABELS.get(slug)
            return (slug, mapped or label)

    tags = raw.get("tags")
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, str) and tag.strip():
                slug = _slugify(tag)
                label = tag.strip()
                mapped = TEMPLATE_CATEGORY_LABELS.get(slug)
                return (slug, mapped or label)

    if isinstance(workflow, dict):
        node_types = _collect_node_class_types(workflow)
        if node_types & VIDEO_NODE_TYPES:
            return ("video", TEMPLATE_CATEGORY_LABELS["video"])
        if node_types & AUDIO_NODE_TYPES:
            return ("audio", TEMPLATE_CATEGORY_LABELS["audio"])
        if any("Animate" in node_type for node_type in node_types):
            return ("animation", TEMPLATE_CATEGORY_LABELS["animation"])

    return ("image", TEMPLATE_CATEGORY_LABELS["image"])


def _is_photo_too_large_error(error: BadRequest) -> bool:
    text = (error.message or str(error)).lower()
    return "too big" in text or "too large" in text or "smaller than 10 mb" in text


def _is_message_not_modified_error(error: BadRequest) -> bool:
    text = (error.message or str(error)).lower()
    return "message is not modified" in text


def _safe_file_size(path: Path) -> Optional[int]:
    try:
        return path.stat().st_size
    except OSError:
        return None


INSTRUCTION_TEXT = (
    "<b>📘 Инструкция</b>\n\n"
    "• <b>Создать Workflow</b> — создаёт пустой шаблон, который можно заполнить нодами.\n"
    "• <b>Текущий workflow</b> — открывает активный граф и позволяет редактировать ноды.\n"
    "• <b>Все workflow</b> — список всех ранее открытых и сохранённых графов.\n"
    "• <b>Шаблоны</b> — библиотека готовых конфигураций из ComfyUI.\n"
    "• <b>Открыть из JSON</b> — загрузите файл, сохранённый в ComfyUI, чтобы редактировать его здесь.\n"
    "• <b>Запустить</b> — отправляет текущий workflow в ComfyUI и показывает прогресс.\n"
    "• <b>📤 Экспорт</b> — сохраните текущий workflow в JSON-файл.\n"
    "• <b>Restart ComfyUI</b> — если настроена команда, перезапускает локальный сервер.\n"
    "• Все изменения сохраняются автоматически и хранятся локально для каждого пользователя.\n"
    "• Параметры редактируются через кнопки под сообщением: нажмите на параметр, отправьте новое значение."
)

KEYBOARD_UPDATED_TEXT = "⌨️ Доступные действия обновлены."

MAIN_MENU_ACTIONS: tuple[str, ...] = (
    MENU_INSTRUCTION,
    MENU_TEMPLATES,
    MENU_CREATE,
    MENU_OPEN,
    MENU_WORKFLOWS,
    MENU_IMPORT,
    QUEUE_STATUS,
    MENU_STATUS,
    MENU_GALLERY,
    MENU_NOTIFICATIONS,
    MENU_HISTORY,
    MENU_RESTART,
)

MENU_DISPLAY_TEXT: dict[str, str] = {
    MENU_INSTRUCTION: "ℹ️ Инструкция",
    MENU_TEMPLATES: "📚 Шаблоны",
    MENU_CREATE: "🎨 Создать Workflow",
    MENU_OPEN: "📂 Текущий workflow",
    MENU_WORKFLOWS: "🗃️ Все workflow",
    MENU_IMPORT: "📥 Открыть из JSON",
    QUEUE_STATUS: "🗂 Очередь",
    MENU_STATUS: "📊 Статус",
    MENU_GALLERY: "🖼️ Галерея",
    MENU_NOTIFICATIONS: "🔔 Уведомления",
    MENU_HISTORY: "🕓 История",
    MENU_RESTART: "🔄 Restart ComfyUI",
}

MENU_TEXT_TO_ACTION: dict[str, str] = {label: action for action, label in MENU_DISPLAY_TEXT.items()}

WORKFLOW_ACTIONS: tuple[str, ...] = (
    WORKFLOW_ADD_NODE,
    WORKFLOW_LAUNCH,
    WORKFLOW_EXPORT,
    MENU_BACK,
)

WORKFLOW_DISPLAY_TEXT: dict[str, str] = {
    WORKFLOW_ADD_NODE: "➕ Создать ноду",
    WORKFLOW_LAUNCH: "🚀 Запустить",
    WORKFLOW_EXPORT: "📤 Экспорт",
    MENU_BACK: "⬅️ В меню",
}

WORKFLOW_TEXT_TO_ACTION: dict[str, str] = {label: action for action, label in WORKFLOW_DISPLAY_TEXT.items()}

SAVE_OUTPUT_NODE_TYPES: set[str] = {
    "SaveImage",
    "SaveAnimatedPNG",
    "SaveAnimatedWEBP",
    "SaveVideo",
    "SaveWEBM",
}

DEFAULT_FILENAME_PREFIX = "ComfyUI\\temp"

QUEUE_ACTIONS: tuple[str, ...] = (
    QUEUE_REFRESH,
    QUEUE_INTERRUPT,
    QUEUE_CLEAR,
    MENU_BACK,
)

QUEUE_DISPLAY_TEXT: dict[str, str] = {
    QUEUE_REFRESH: "🔄 Обновить очередь",
    QUEUE_INTERRUPT: "⛔️ Остановить текущий",
    QUEUE_CLEAR: "🧹 Очистить очередь",
    MENU_BACK: WORKFLOW_DISPLAY_TEXT[MENU_BACK],
}

QUEUE_TEXT_TO_ACTION: dict[str, str] = {label: action for action, label in QUEUE_DISPLAY_TEXT.items()}

HISTORY_ACTIONS: tuple[str, ...] = (
    MENU_HISTORY,
    MENU_BACK,
)

HISTORY_DISPLAY_TEXT: dict[str, str] = {
    MENU_HISTORY: "🔄 Обновить историю",
    MENU_BACK: WORKFLOW_DISPLAY_TEXT[MENU_BACK],
}

HISTORY_TEXT_TO_ACTION: dict[str, str] = {label: action for action, label in HISTORY_DISPLAY_TEXT.items()}

STATUS_ACTIONS: tuple[str, ...] = (
    MENU_STATUS,
    MENU_BACK,
)

STATUS_DISPLAY_TEXT: dict[str, str] = {
    MENU_STATUS: "🔄 Обновить статус",
    MENU_BACK: WORKFLOW_DISPLAY_TEXT[MENU_BACK],
}

STATUS_TEXT_TO_ACTION: dict[str, str] = {label: action for action, label in STATUS_DISPLAY_TEXT.items()}


@dataclass(slots=True)
class BotResources:
    config: BotConfig
    storage: WorkflowStorage
    client: ComfyUIClient

    async def shutdown(self) -> None:
        await self.client.close()


def _apply_default_filename_prefix(workflow: Dict[str, Any], prefix: str = DEFAULT_FILENAME_PREFIX) -> None:
    nodes = workflow.get("nodes")
    if isinstance(nodes, dict):
        iterator = nodes.values()
    elif isinstance(nodes, list):
        iterator = (item for item in nodes if isinstance(item, dict))
    else:
        return

    for node in iterator:
        class_type = node.get("class_type") or node.get("type")
        if not isinstance(class_type, str) or class_type not in SAVE_OUTPUT_NODE_TYPES:
            continue
        inputs = node.setdefault("inputs", {})
        if isinstance(inputs, dict):
            inputs["filename_prefix"] = prefix


def _persist_workflow(
    resources: BotResources,
    user_id: int,
    workflow: Dict[str, Any],
    name: Optional[str] = None,
) -> Path:
    target_name = name or "default"
    _apply_default_filename_prefix(workflow)
    return resources.storage.save_workflow(user_id, workflow, target_name)


@dataclass(slots=True)
class ConnectionInputInfo:
    name: str
    spec: Any
    optional: bool
    multi: bool


@dataclass(slots=True)
class ConnectionOutputInfo:
    index: int
    label: str
    name: Optional[str] = None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = get_user_id(update)
    user_data = get_user_data(context)
    user_data.setdefault("workflow_name", "default")
    await _remove_reply_keyboard(update)
    await send_main_menu(update, context, user_id)


async def handle_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()

    data = query.data or ""
    user_data = get_user_data(context)

    if data in MAIN_MENU_ACTIONS:
        if await _dispatch_menu_action(query, context, data, via_callback=True):
            return

    if data == QUEUE_REFRESH:
        await show_queue(query, context)
    elif data == QUEUE_CLEAR:
        await clear_queue(query, context)
    elif data == QUEUE_INTERRUPT:
        await interrupt_queue(query, context)
    elif data == CATALOG_REFRESH:
        last_page = user_data.get("catalog_last_page", 0)
        await show_node_categories(query, context, page=last_page, refresh=True)
    elif data == CATALOG_SEARCH:
        await prompt_catalog_search(query, context)
    elif data == CATALOG_SEARCH_CANCEL:
        user_data.pop("awaiting_catalog_search", None)
        user_data.pop("catalog_search_results", None)
        last_page = user_data.get("catalog_last_page", 0)
        await show_node_categories(query, context, page=last_page)
    elif data.startswith(CATALOG_SEARCH_PAGE_PREFIX):
        parts = data.split(":")
        try:
            page = int(parts[2])
        except (ValueError, IndexError):
            await query.answer("Страница не найдена", show_alert=True)
            return
        await show_catalog_search_results(query, context, page=page)
    elif data.startswith(CATALOG_PAGE_PREFIX):
        page_text = data[len(CATALOG_PAGE_PREFIX) :]
        try:
            page = int(page_text)
        except ValueError:
            await query.answer("Страница не найдена", show_alert=True)
            return
        await show_node_categories(query, context, page=page)
    elif data.startswith(CATALOG_NODE_PAGE_PREFIX):
        parts = data.split(":")
        try:
            category_index = int(parts[2])
            page = int(parts[3])
        except (ValueError, IndexError):
            await query.answer("Страница не найдена", show_alert=True)
            return
        await show_catalog_nodes(query, context, category_index, page=page)
    elif data == CATALOG_NOOP:
        await query.answer()
        return
    elif data == CATALOG_BACK:
        last_page = user_data.get("catalog_last_page", 0)
        await show_node_categories(query, context, page=last_page)
    elif data.startswith(GALLERY_PAGE_PREFIX):
        page_text = data[len(GALLERY_PAGE_PREFIX) :]
        try:
            page = int(page_text)
        except ValueError:
            await query.answer("Страница не найдена", show_alert=True)
            return
        await show_gallery(query, context, page=page, via_callback=True)
    elif data.startswith(NOTIFY_TOGGLE_PREFIX):
        option = data[len(NOTIFY_TOGGLE_PREFIX) :]
        await _toggle_notification_setting(query, context, option)
    elif data == TEMPLATE_REFRESH:
        await show_template_categories(query, context, via_callback=True, refresh=True)
    elif data == TEMPLATE_BACK:
        await show_template_categories(query, context, via_callback=True)
    elif data.startswith(TEMPLATE_CATEGORY_PREFIX):
        slug = data[len(TEMPLATE_CATEGORY_PREFIX) :]
        await show_template_list(query, context, slug, via_callback=True)
    elif data.startswith(TEMPLATE_PAGE_PREFIX):
        payload = data[len(TEMPLATE_PAGE_PREFIX) :]
        if ":" in payload:
            slug, page_text = payload.split(":", 1)
        else:
            slug, page_text = payload, "0"
        try:
            page = int(page_text)
        except ValueError:
            await query.answer("Страница недоступна", show_alert=True)
            return
        await show_template_list(query, context, slug, page=page, via_callback=True)
    elif data.startswith(TEMPLATE_SELECT_PREFIX):
        template_id = data[len(TEMPLATE_SELECT_PREFIX) :]
        await apply_template(query, context, template_id)
    elif data.startswith(WORKFLOW_LIBRARY_PAGE_PREFIX):
        page_text = data[len(WORKFLOW_LIBRARY_PAGE_PREFIX) :]
        try:
            page = int(page_text)
        except ValueError:
            await query.answer("Страница недоступна", show_alert=True)
            return
        await show_workflow_library(query, context, page=page, via_callback=True)
    elif data.startswith(WORKFLOW_SELECT_PREFIX):
        index_text = data[len(WORKFLOW_SELECT_PREFIX) :]
        try:
            index = int(index_text)
        except ValueError:
            await query.answer("Некорректный выбор", show_alert=True)
            return
        await _load_saved_workflow(query, context, index)
    elif data == MENU_BACK:
        await send_main_menu(query, context, query.from_user.id, edit=True)
    elif data == WORKFLOW_LAUNCH:
        await launch_workflow(query, context)
    elif data == WORKFLOW_CANCEL:
        await cancel_workflow(query, context)
    elif data == WORKFLOW_ADD_NODE:
        last_page = user_data.get("catalog_last_page", 0)
        await show_node_categories(query, context, page=last_page)
    elif data == WORKFLOW_EXPORT:
        await export_current_workflow(query, context)
    elif data.startswith(WORKFLOW_NODE_PREFIX):
        node_id = data[len(WORKFLOW_NODE_PREFIX) :]
        await show_node_details(query, context, node_id)
    elif data.startswith(WORKFLOW_PARAM_PREFIX):
        parts = data.split(":")
        node_id = parts[2]
        param = parts[3]
        await prompt_param_update(query, context, node_id, param)
    elif data.startswith(WORKFLOW_PARAM_QUICK_PREFIX):
        parts = data.split(":")
        if len(parts) < 5:
            await query.answer("Некорректный выбор", show_alert=True)
            return
        node_id = parts[2]
        param = parts[3]
        try:
            index = int(parts[4])
        except ValueError:
            await query.answer("Некорректный выбор", show_alert=True)
            return
        await apply_quick_param_choice(query, context, node_id, param, index)
    elif data.startswith(WORKFLOW_PARAM_PAGE_PREFIX):
        parts = data.split(":")
        if len(parts) < 5:
            await query.answer("Некорректная страница", show_alert=True)
            return
        node_id = parts[2]
        param = parts[3]
        try:
            page = int(parts[4])
        except ValueError:
            await query.answer("Некорректная страница", show_alert=True)
            return
        try:
            await query.answer()
        except Exception:
            LOGGER.debug("Failed to answer callback for param page", exc_info=True)
        await show_param_choice_page(query, context, node_id, param, page)
    elif data == "param:cancel":
        try:
            await query.answer()
        except Exception:
            LOGGER.debug("Failed to answer callback for param cancel", exc_info=True)
        await cancel_pending_input(query, context)
    elif data.startswith("param:manual:"):
        parts = data.split(":")
        if len(parts) < 4:
            await query.answer("Некорректные данные", show_alert=True)
            return
        node_id = parts[2]
        param = parts[3]
        try:
            await query.answer()
        except Exception:
            LOGGER.debug("Failed to answer callback for param manual", exc_info=True)
        await prompt_manual_param_input(query, context, node_id, param)
    elif data.startswith(WORKFLOW_CONNECT_NODE_PREFIX):
        node_id = data[len(WORKFLOW_CONNECT_NODE_PREFIX) :]
        await show_connection_inputs(query, context, node_id)
    elif data.startswith(CONNECTION_INPUT_PREFIX):
        payload = data[len(CONNECTION_INPUT_PREFIX) :]
        pieces = payload.split(":", 1)
        if len(pieces) != 2:
            await query.answer("Некорректные данные", show_alert=True)
            return
        target_node, input_name = pieces
        await start_connection_selection(query, context, target_node, input_name)
    elif data.startswith(CONNECTION_SOURCE_PREFIX):
        index_text = data[len(CONNECTION_SOURCE_PREFIX) :]
        try:
            index = int(index_text)
        except ValueError:
            await query.answer("Некорректный выбор", show_alert=True)
            return
        await pick_connection_source(query, context, index)
    elif data.startswith(CONNECTION_SOURCE_PAGE_PREFIX):
        page_text = data[len(CONNECTION_SOURCE_PAGE_PREFIX) :]
        try:
            page = int(page_text)
        except ValueError:
            await query.answer("Страница недоступна", show_alert=True)
            return
        await show_connection_source_picker(query, context, page=page)
    elif data.startswith(CONNECTION_OUTPUT_PREFIX):
        index_text = data[len(CONNECTION_OUTPUT_PREFIX) :]
        try:
            index = int(index_text)
        except ValueError:
            await query.answer("Некорректный выход", show_alert=True)
            return
        await apply_connection_choice(query, context, index)
    elif data == CONNECTION_BACK:
        await connection_back(query, context)
    elif data == CONNECTION_CLEAR:
        await clear_connection_choice(query, context)
    elif data.startswith(CATALOG_CATEGORY_PREFIX):
        parts = data.split(":")
        try:
            category_index = int(parts[2])
            page = int(parts[3]) if len(parts) > 3 else 0
        except (ValueError, IndexError):
            await query.answer("Категория не найдена", show_alert=True)
            return
        await show_catalog_nodes(query, context, category_index, page=page)
    elif data.startswith(CATALOG_NODE_PREFIX):
        parts = data.split(":")
        try:
            category_index = int(parts[2])
            node_index = int(parts[3])
        except (ValueError, IndexError):
            await query.answer("Нода не найдена", show_alert=True)
            return
        await add_catalog_node(query, context, category_index, node_index)
    elif data == MENU_CANCEL:
        await cancel_pending_input(query, context)
    elif data == WORKFLOW_REFRESH:
        await show_workflow_overview(query, context, refresh=True)


async def create_workflow(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    workflow = {"nodes": {}}
    user_data = get_user_data(context)
    user_data["workflow_name"] = "default"
    user_data["workflow"] = workflow
    _persist_workflow(resources, user_id, workflow)
    await _flush_persistence(context)

    text = (
        "✅ Создан новый workflow.\n"
        "Добавьте ноды или импортируйте из JSON, чтобы начать работу."
    )

    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "workflow")

    await respond(
        message_source,
        text,
        _workflow_markup_for_source(context, message_source, user_id),
        parse_mode=ParseMode.HTML,
    )


async def show_workflow_overview(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    node_message: bool = False,
    *,
    refresh: bool = False,
) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    workflow = await ensure_workflow_loaded(context, resources, user_id, refresh=refresh)

    if workflow is None:
        await _ensure_keyboard_mode(message_source, context, user_id, "menu")
        await respond(
            message_source,
            "ℹ️ У вас пока нет сохранённого workflow. Создайте новый или импортируйте JSON.",
            _menu_reply_keyboard(context, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    summary = format_workflow_summary(workflow)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "workflow")

    await respond(
        message_source,
        summary,
        _workflow_markup_for_source(context, message_source, user_id),
        parse_mode=ParseMode.HTML,
    )


async def ensure_catalog(context: ContextTypes.DEFAULT_TYPE, *, refresh: bool = False) -> Dict[str, Any]:
    cache_key = "catalog_cache"
    if not refresh:
        cached = context.application.bot_data.get(cache_key)
        nodes = cached.get("nodes") if isinstance(cached, dict) else None
        if isinstance(nodes, dict) and nodes:
            return cached  # type: ignore[return-value]
        if cached is not None:
            context.application.bot_data.pop(cache_key, None)

    resources = require_resources(context)
    object_info: Optional[Dict[str, Any]] = None

    if not refresh:
        object_info = _load_catalog_cache(resources.config)

    if object_info is None:
        object_info = await resources.client.get_object_info(refresh=refresh)
        _store_catalog_cache(resources.config, object_info)

    catalog = build_catalog(object_info)
    context.application.bot_data[cache_key] = catalog
    nodes = catalog.get("nodes") if isinstance(catalog, dict) else None
    if not refresh and (not isinstance(nodes, dict) or not nodes):
        return await ensure_catalog(context, refresh=True)

    if isinstance(nodes, dict) and nodes:
        return catalog

    raise RuntimeError("Каталог нод ComfyUI пуст — обновление не помогло")


def _catalog_cache_path(config: BotConfig) -> Path:
    cache_dir = config.data_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / CATALOG_CACHE_FILE


def _load_catalog_cache(config: BotConfig) -> Optional[Dict[str, Any]]:
    path = _catalog_cache_path(config)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        LOGGER.debug("catalog cache is invalid JSON", exc_info=True)
        return None
    except OSError:
        LOGGER.debug("failed to read catalog cache", exc_info=True)
        return None

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return None
    if time.time() - timestamp > CATALOG_CACHE_TTL_SECONDS:
        try:
            path.unlink()
        except OSError:
            LOGGER.debug("failed to remove stale catalog cache", exc_info=True)
        return None

    object_info = payload.get("object_info")
    if not isinstance(object_info, dict):
        return None
    return object_info


def _store_catalog_cache(config: BotConfig, object_info: Dict[str, Any]) -> None:
    path = _catalog_cache_path(config)
    payload = {"timestamp": time.time(), "object_info": object_info}
    try:
        path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    except OSError:
        LOGGER.debug("failed to write catalog cache", exc_info=True)


def build_catalog(object_info: Dict[str, Any]) -> Dict[str, Any]:
    def _extract_nodes_map(payload: Dict[str, Any]) -> Dict[str, Any]:
        section = payload.get("nodes")
        if isinstance(section, dict) and section:
            return section

        # Fallback for legacy ComfyUI versions where object_info is already a flat map
        # of node name -> definition without wrapping inside the "nodes" key.
        node_like_keys = {
            "input",
            "inputs",
            "output",
            "outputs",
            "return",
            "returns",
            "category",
            "python_module",
            "display_name",
        }
        skip_keys = {"categories", "extra", "config", "system", "__metadata__", "version"}

        fallback: Dict[str, Any] = {}
        for key, value in payload.items():
            if key in skip_keys:
                continue
            if not isinstance(value, dict):
                continue
            if any(hint in value for hint in node_like_keys):
                fallback[str(key)] = value
        return fallback

    nodes = _extract_nodes_map(object_info)
    categories_map = object_info.get("categories") if isinstance(object_info.get("categories"), dict) else {}

    nodes_by_category: Dict[str, List[str]] = {}
    if isinstance(categories_map, dict):
        for category, node_list in categories_map.items():
            if isinstance(node_list, list):
                clean = sorted({str(node) for node in node_list})
                if clean:
                    nodes_by_category[str(category)] = clean

    if not nodes_by_category and isinstance(nodes, dict):
        for node_name, node_data in nodes.items():
            if not isinstance(node_data, dict):
                continue
            category = node_data.get("category") or node_data.get("category_path")
            if isinstance(category, list) and category:
                category = category[0]
            category_name = str(category or "Прочее")
            nodes_by_category.setdefault(category_name, []).append(str(node_name))

        for category_name, node_list in nodes_by_category.items():
            nodes_by_category[category_name] = sorted({str(node) for node in node_list})

    display_names: Dict[str, str] = {}
    if isinstance(nodes, dict):
        for node_name, node_data in nodes.items():
            if not isinstance(node_data, dict):
                continue
            display = (
                node_data.get("display_name")
                or node_data.get("name")
                or node_data.get("title")
                or str(node_name)
            )
            display_names[str(node_name)] = str(display)

    categories = sorted(nodes_by_category.keys(), key=str.lower)

    return {
        "raw": object_info,
        "nodes": nodes,
        "nodes_by_category": nodes_by_category,
        "display_names": display_names,
        "categories": categories,
    }


async def begin_import(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = get_user_id_from_source(message_source)
    user_data = get_user_data(context)
    user_data["awaiting_import"] = True
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "menu")

    cancel_label = "❎ Отмена"
    mapping = {cancel_label: ("import_cancel",)}
    _set_dynamic_buttons(context, mapping)

    await respond(
        message_source,
        "📥 Отправьте JSON файл workflow (как документ).",
        ReplyKeyboardMarkup([[cancel_label]], resize_keyboard=True),
        parse_mode=ParseMode.HTML,
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_data = get_user_data(context)
    if not user_data.get("awaiting_import"):
        return

    message = update.message
    document = message.document if message else None
    if not message or not document:
        return

    filename = document.file_name or "workflow.json"
    if not filename.lower().endswith(".json"):
        await message.reply_text("⚠️ Пришлите файл в формате JSON.")
        return

    try:
        file = await document.get_file()
        content = await file.download_as_bytearray()
    except Exception as exc:  # pragma: no cover - network/io failures
        LOGGER.exception("Failed to download workflow JSON")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {escape(str(exc))}")
        return

    try:
        workflow = json.loads(content)
    except json.JSONDecodeError:
        await message.reply_text("⚠️ Не удалось прочитать JSON. Проверьте файл.")
        return

    resources = require_resources(context)
    user_id = get_user_id(update)
    name = filename[:-5] or "default"

    catalog: Optional[Dict[str, Any]] = None
    try:
        catalog = await ensure_catalog(context)
    except Exception:  # pragma: no cover - network failure during import
        LOGGER.warning("Failed to fetch catalog during workflow import", exc_info=True)

    missing_types = normalize_workflow_structure(workflow, catalog)
    conversion_notes: list[str] = []
    if missing_types:
        preview = ", ".join(f"#{escape(str(node_id))}" for node_id in missing_types[:5])
        conversion_notes.append(
            f"⚠️ Не удалось определить тип нод: {preview}{'…' if len(missing_types) > 5 else ''}."
        )
        conversion_notes.append("При запуске workflow проверьте эти ноды вручную.")

    user_data["workflow_name"] = name
    user_data["workflow"] = workflow
    user_data.pop("awaiting_import", None)

    _persist_workflow(resources, user_id, workflow, name)
    await _flush_persistence(context)

    await _ensure_keyboard_mode(message, context, user_id, "workflow")

    await message.reply_text(
        "✅ Workflow импортирован." if not conversion_notes else "✅ Workflow импортирован.\n" + "\n".join(conversion_notes),
        reply_markup=_workflow_markup_for_source(context, message, user_id),
        parse_mode=ParseMode.HTML,
    )
    await show_workflow_overview(message, context, refresh=True)


async def show_status(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "status")

    try:
        stats = await resources.client.get_system_stats()
    except Exception as exc:  # pragma: no cover - network failure
        LOGGER.exception("Failed to fetch system stats")
        await respond(
            message_source,
            f"⚠️ Не удалось получить статус ComfyUI:\n<code>{escape(str(exc))}</code>",
            _status_reply_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return

    text = ["<b>📊 Статус ComfyUI</b>"]
    for key, value in stats.items():
        text.append(f"• <code>{escape(str(key))}</code>: {escape(str(value))}")
    await respond(
        message_source,
        "\n".join(text),
        _status_reply_keyboard(),
        parse_mode=ParseMode.HTML,
    )


def _status_reply_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [STATUS_DISPLAY_TEXT[MENU_STATUS]],
        [STATUS_DISPLAY_TEXT[MENU_BACK]],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _status_markup_for_source(source: MessageSource) -> ReplyMarkupType:
    return _status_reply_keyboard()


async def show_history(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "history")

    entries, total = resources.storage.get_recent_history(user_id, limit=5)

    lines = ["<b>🕓 История запусков</b>"]
    if not entries:
        lines.append("<i>Здесь появятся результаты после первой генерации.</i>")
    else:
        for entry in entries:
            status = str(entry.get("status", ""))
            status_icon = {
                "success": "✅",
                "error": "❌",
                "no_output": "⚠️",
                "cancelled": "⛔️",
            }.get(status, "•")
            timestamp = _format_history_timestamp(entry.get("created_at"))
            name = escape(str(entry.get("workflow_name", "default")))
            file_count = int(entry.get("file_count", 0) or 0)
            duration = entry.get("duration")
            parts = [f"{status_icon} {timestamp} — <code>{name}</code>"]
            if file_count:
                parts.append(f"файлов: {file_count}")
            if duration:
                parts.append(f"время: {duration}")
            lines.append(" ".join(parts))

            files = entry.get("files")
            if isinstance(files, list) and files:
                sample = ", ".join(escape(str(item)) for item in files[:3])
                if len(files) > 3:
                    sample += "…"
                lines.append(f"   <i>{sample}</i>")

            error = entry.get("error")
            if error:
                lines.append(f"   <code>{escape(str(error))}</code>")

        if total > len(entries):
            lines.append("")
            lines.append(f"… и ещё {total - len(entries)} запуск(ов) в истории.")

    await respond(
        message_source,
        "\n".join(lines),
        _history_reply_keyboard(),
        parse_mode=ParseMode.HTML,
    )


def _history_reply_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [HISTORY_DISPLAY_TEXT[MENU_HISTORY]],
        [HISTORY_DISPLAY_TEXT[MENU_BACK]],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _history_markup_for_source(source: MessageSource) -> ReplyMarkupType:
    return _history_reply_keyboard()


def _queue_reply_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [
            QUEUE_DISPLAY_TEXT[QUEUE_REFRESH],
            QUEUE_DISPLAY_TEXT[QUEUE_INTERRUPT],
            QUEUE_DISPLAY_TEXT[QUEUE_CLEAR],
        ],
        [QUEUE_DISPLAY_TEXT[MENU_BACK]],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _format_history_timestamp(raw: Any) -> str:
    if not raw:
        return "—"
    text = str(raw)
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return escape(text)
    return moment.strftime("%Y-%m-%d %H:%M")


async def show_queue(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "queue")

    try:
        state = await resources.client.get_queue_state()
    except Exception as exc:  # pragma: no cover - network failure
        LOGGER.exception("Failed to fetch queue state")
        await respond(
            message_source,
            f"⚠️ Не удалось получить очередь:\n<code>{escape(str(exc))}</code>",
            _queue_reply_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return

    text = format_queue_state(state)
    await respond(
        message_source,
        text,
        _queue_reply_keyboard(),
        parse_mode=ParseMode.HTML,
    )


def _collect_gallery_files(root: Path) -> list[Path]:
    if not root.exists():
        return []

    entries: list[tuple[float, Path]] = []
    seen: set[Path] = set()
    for candidate in root.rglob("*"):
        if not candidate.is_file():
            continue
        if candidate.suffix.lower() not in IMAGE_EXTENSIONS:
            continue
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate
        if resolved in seen:
            continue
        try:
            mtime = resolved.stat().st_mtime
        except OSError:
            continue
        entries.append((mtime, resolved))
        seen.add(resolved)

    entries.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in entries]


def _gallery_keyboard(page: int, total_pages: int) -> Optional[InlineKeyboardMarkup]:
    if total_pages <= 1:
        return None

    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []

    if page > 0:
        row.append(InlineKeyboardButton("Предыдущая страница", callback_data=f"{GALLERY_PAGE_PREFIX}{page - 1}"))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton("Следующая страница", callback_data=f"{GALLERY_PAGE_PREFIX}{page + 1}"))

    if row:
        buttons.append(row)

    return InlineKeyboardMarkup(buttons) if buttons else None


def _format_gallery_caption(index: int, total: int, file_path: Path, root: Path) -> str:
    try:
        relative = file_path.relative_to(root)
        location = relative.as_posix()
    except ValueError:
        location = file_path.name
    return f"{index}/{total}: {location}"


async def show_gallery(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    page: int = 0,
    *,
    via_callback: bool = False,
    refresh: bool = False,
) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    user_data = get_user_data(context)

    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "menu")

    if via_callback and isinstance(message_source, CallbackQuery):
        try:
            await message_source.answer()
        except Exception:  # pragma: no cover - optional ack
            LOGGER.debug("Failed to answer gallery callback", exc_info=True)

    root = resources.config.output_dir / str(user_id)
    state = user_data.get(GALLERY_STATE_KEY)
    files: list[Path] = []
    previous_message_id: Optional[int] = None

    if isinstance(state, dict):
        raw_message_id = state.get("message_id")
        if isinstance(raw_message_id, int):
            previous_message_id = raw_message_id

    if refresh or not isinstance(state, dict) or not via_callback:
        files = _collect_gallery_files(root)
        state = {"files": [str(path) for path in files]}
        user_data[GALLERY_STATE_KEY] = state
    else:
        raw_files = state.get("files")
        if isinstance(raw_files, list):
            ordered: list[Path] = []
            for item in raw_files:
                if not isinstance(item, str):
                    continue
                path = Path(item)
                if path.exists():
                    ordered.append(path)
            files = ordered
            if len(files) != len(raw_files):
                state["files"] = [str(path) for path in files]

        if not files:
            files = _collect_gallery_files(root)
            state["files"] = [str(path) for path in files]

    total = len(files)

    chat_id = get_chat_id_from_source(message_source)

    if total == 0:
        if via_callback and previous_message_id is not None:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=previous_message_id)
            except Exception:  # pragma: no cover - best-effort cleanup
                LOGGER.debug("Failed to delete previous gallery message", exc_info=True)

        summary_message = await context.bot.send_message(
            chat_id=chat_id,
            text="<b>🖼️ Галерея пуста</b>\nСгенерированных изображений пока нет.",
            parse_mode=ParseMode.HTML,
        )
        user_data[GALLERY_STATE_KEY] = {"files": [], "page": 0, "message_id": summary_message.message_id}
        return

    total_pages = (total + GALLERY_PAGE_SIZE - 1) // GALLERY_PAGE_SIZE
    page = max(0, min(page, total_pages - 1))
    start = page * GALLERY_PAGE_SIZE
    end = min(start + GALLERY_PAGE_SIZE, total)

    lines = [
        "<b>🖼️ Галерея</b>",
        f"Показаны {start + 1}–{end} из {total}.",
        "Изображения отправлены от новых к старым.",
    ]
    if total_pages > 1:
        lines.append(f"Страница {page + 1} из {total_pages}.")
        if page < total_pages - 1:
            lines.append("Нажмите «Следующая страница», чтобы увидеть остальные.")

    keyboard = _gallery_keyboard(page, total_pages)

    if via_callback and previous_message_id is not None:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=previous_message_id)
        except Exception:  # pragma: no cover - best-effort cleanup
            LOGGER.debug("Failed to delete previous gallery message", exc_info=True)

    state = user_data.setdefault(GALLERY_STATE_KEY, {})
    state["files"] = [str(path) for path in files]
    state["page"] = page

    segment = files[start:end]
    delivered = 0
    for offset, file_path in enumerate(segment, start=start + 1):
        if not file_path.exists():
            continue
        caption = _format_gallery_caption(offset, total, file_path, root)
        await _send_generated_file(context.bot, chat_id, file_path, caption=caption)
        delivered += 1

    if delivered == 0:
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ Файлы для этой страницы не найдены на диске.",
        )

    summary_message = await context.bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    state["message_id"] = summary_message.message_id


def _get_notification_settings(user_data: UserDataDict) -> Dict[str, bool]:
    settings = user_data.setdefault("settings", {})
    if not isinstance(settings, dict):  # pragma: no cover - defensive recovery
        settings = {}
        user_data["settings"] = settings

    notifications = settings.setdefault("notifications", {})
    if not isinstance(notifications, dict):  # pragma: no cover - defensive recovery
        notifications = {}
        settings["notifications"] = notifications

    defaults = {"success": True, "failure": True}
    for key, default in defaults.items():
        value = notifications.get(key)
        notifications[key] = bool(value) if isinstance(value, bool) else default

    return notifications  # type: ignore[return-value]


NOTIFICATION_LABELS: Dict[str, str] = {
    "success": "Успешное завершение",
    "failure": "Ошибки и отсутствие результата",
}


async def show_notification_settings(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    via_callback: bool = False,
) -> None:
    user_id = get_user_id_from_source(message_source)
    user_data = get_user_data(context)
    notifications = _get_notification_settings(user_data)

    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "menu")

    if isinstance(message_source, CallbackQuery):
        try:
            await message_source.answer()
        except Exception:  # pragma: no cover - optional ack
            LOGGER.debug("Failed to acknowledge notification callback", exc_info=True)
        via_callback = True

    lines = [
        "<b>🔔 Настройки уведомлений</b>",
        "Включите отправку дополнительных сообщений после завершения генерации.",
        "",
    ]

    for key, label in NOTIFICATION_LABELS.items():
        enabled = notifications.get(key, True)
        status = "включено" if enabled else "выключено"
        lines.append(f"• <b>{escape(label)}</b>: {status}")

    keyboard_rows: list[list[InlineKeyboardButton]] = []
    for key, label in NOTIFICATION_LABELS.items():
        enabled = notifications.get(key, True)
        icon = "✅" if enabled else "🚫"
        keyboard_rows.append(
            [InlineKeyboardButton(f"{icon} {label}", callback_data=f"{NOTIFY_TOGGLE_PREFIX}{key}")]
        )
    keyboard_rows.append([InlineKeyboardButton("⬅️ В меню", callback_data=MENU_BACK)])

    keyboard = InlineKeyboardMarkup(keyboard_rows)

    await respond(
        message_source,
        "\n".join(lines),
        keyboard,
        parse_mode=ParseMode.HTML,
        edit=via_callback,
    )


async def _toggle_notification_setting(
    query: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    option: str,
) -> None:
    user_data = get_user_data(context)
    notifications = _get_notification_settings(user_data)

    if option not in NOTIFICATION_LABELS:
        try:
            await query.answer("Недоступно", show_alert=True)
        except Exception:  # pragma: no cover - optional ack
            LOGGER.debug("Failed to send invalid notification toggle alert", exc_info=True)
        return

    current = notifications.get(option, True)
    notifications[option] = not current

    try:
        await query.answer("Включено" if notifications[option] else "Отключено")
    except Exception:  # pragma: no cover - optional ack
        LOGGER.debug("Failed to acknowledge notification toggle", exc_info=True)

    await show_notification_settings(query, context, via_callback=True)


def _extract_workflow_from_template(data: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(data, Mapping):
        return None
    for key in ("workflow", "prompt", "graph"):
        candidate = data.get(key)
        if isinstance(candidate, dict):
            return candidate
    return None


def _normalize_template_catalog(raw_templates: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    catalog: Dict[str, Any] = {
        "templates": {},
        "by_category": {},
        "categories": [],
    }

    for index, item in enumerate(raw_templates):
        if not isinstance(item, dict):
            continue

        workflow = _extract_workflow_from_template(item)
        slug, inferred_label = _infer_template_category(item, workflow)
        label = TEMPLATE_CATEGORY_LABELS.get(slug, inferred_label)

        raw_name = item.get("name") or item.get("title") or item.get("id")
        name = str(raw_name).strip() if raw_name is not None else "Шаблон"
        if not name:
            name = f"Шаблон {index + 1}"

        base_identifier = item.get("id") if isinstance(item.get("id"), str) else None
        base_slug_source = base_identifier or name
        base_slug = _slugify(base_slug_source)
        template_id = base_slug
        suffix = 1
        while template_id in catalog["templates"]:
            template_id = f"{base_slug}-{suffix}"
            suffix += 1

        source_id = str(base_identifier or name or template_id)

        description_value = item.get("description") or item.get("notes")
        description = description_value.strip() if isinstance(description_value, str) else ""

        template_entry = {
            "id": template_id,
            "source_id": source_id,
            "name": name,
            "description": description,
            "category": slug,
            "category_label": label,
            "workflow": workflow if isinstance(workflow, dict) else None,
        }

        source_info = item.get("_source_info")
        if isinstance(source_info, dict):
            template_entry["source_info"] = source_info

        catalog["templates"][template_id] = template_entry
        catalog["by_category"].setdefault(slug, []).append(template_id)

    categories: list[dict[str, Any]] = []
    for slug, template_ids in catalog["by_category"].items():
        label = TEMPLATE_CATEGORY_LABELS.get(slug)
        if not label and template_ids:
            label = catalog["templates"][template_ids[0]].get("category_label")
        categories.append({
            "slug": slug,
            "label": label or slug.title(),
            "count": len(template_ids),
        })

    categories.sort(key=lambda item: item["label"].lower())
    catalog["categories"] = categories
    return catalog


async def _ensure_template_catalog(context: ContextTypes.DEFAULT_TYPE, *, refresh: bool = False) -> Dict[str, Any]:
    application = getattr(context, "application", None)

    if application is not None and refresh:
        application.bot_data.pop(TEMPLATE_ERROR_KEY, None)

    if not refresh and application is not None:
        cached_catalog = application.bot_data.get("template_catalog")
        if isinstance(cached_catalog, dict) and cached_catalog.get("templates"):
            return cached_catalog

    resources = require_resources(context)
    templates = await resources.client.get_templates(refresh=refresh)
    catalog = _normalize_template_catalog(templates)

    if application is not None:
        application.bot_data["template_catalog"] = catalog
        application.bot_data.pop(TEMPLATE_ERROR_KEY, None)

    return catalog


def _template_description(template: Dict[str, Any], *, max_length: int = 160) -> Optional[str]:
    description = template.get("description")
    if not isinstance(description, str) or not description.strip():
        return None
    text = description.strip()
    if len(text) > max_length:
        text = text[: max_length - 1].rstrip() + "…"
    return text


async def show_template_categories(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    refresh: bool = False,
    via_callback: bool = False,
) -> None:
    user_id = get_user_id_from_source(message_source)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "menu")

    if isinstance(message_source, CallbackQuery):
        try:
            await message_source.answer()
        except Exception:  # pragma: no cover - optional ack
            LOGGER.debug("Failed to acknowledge template callback", exc_info=True)
        via_callback = True

    application = getattr(context, "application", None)
    cached_error = None
    if application is not None and not refresh:
        cached_error = application.bot_data.get(TEMPLATE_ERROR_KEY)

    if isinstance(cached_error, str) and cached_error and not refresh:
        await respond(
            message_source,
            cached_error,
            _template_error_keyboard(),
            parse_mode=ParseMode.HTML,
            edit=via_callback,
        )
        return

    try:
        catalog = await _ensure_template_catalog(context, refresh=refresh)
    except Exception as exc:
        LOGGER.exception("Failed to fetch templates")
        message = _format_template_error(exc)
        if application is not None:
            application.bot_data[TEMPLATE_ERROR_KEY] = message
        await respond(
            message_source,
            message,
            _template_error_keyboard(),
            parse_mode=ParseMode.HTML,
            edit=via_callback,
        )
        return

    categories = catalog.get("categories", [])
    if not categories:
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🔄 Обновить", callback_data=TEMPLATE_REFRESH)],
                [InlineKeyboardButton("⬅️ В меню", callback_data=MENU_BACK)],
            ]
        )
        await respond(
            message_source,
            "⚠️ Шаблоны недоступны. Попробуйте обновить позже.",
            keyboard,
            parse_mode=ParseMode.HTML,
            edit=via_callback,
        )
        return

    lines = [
        "<b>📚 Шаблоны workflow</b>",
        "Выберите категорию, чтобы загрузить готовый шаблон.",
        "Загрузка шаблона заменит текущий workflow.",
        "",
    ]

    keyboard_rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for category in categories:
        label = f"{category['label']} ({category['count']})"
        row.append(InlineKeyboardButton(label, callback_data=f"{TEMPLATE_CATEGORY_PREFIX}{category['slug']}"))
        if len(row) == 2:
            keyboard_rows.append(row)
            row = []

    if row:
        keyboard_rows.append(row)

    keyboard_rows.append([InlineKeyboardButton("🔄 Обновить", callback_data=TEMPLATE_REFRESH)])
    keyboard_rows.append([InlineKeyboardButton("⬅️ В меню", callback_data=MENU_BACK)])

    keyboard = InlineKeyboardMarkup(keyboard_rows)

    await respond(
        message_source,
        "\n".join(lines),
        keyboard,
        parse_mode=ParseMode.HTML,
        edit=via_callback,
    )


async def show_template_list(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    category_slug: str,
    *,
    page: int = 0,
    via_callback: bool = False,
) -> None:
    user_id = get_user_id_from_source(message_source)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "menu")

    if isinstance(message_source, CallbackQuery):
        try:
            await message_source.answer()
        except Exception:  # pragma: no cover - optional ack
            LOGGER.debug("Failed to acknowledge template list callback", exc_info=True)
        via_callback = True

    catalog = await _ensure_template_catalog(context)
    templates_map: Dict[str, Dict[str, Any]] = catalog.get("templates", {})
    template_ids: list[str] = catalog.get("by_category", {}).get(category_slug, [])

    if not template_ids:
        await respond(
            message_source,
            "⚠️ В категории нет шаблонов.",
            InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("⬅️ Категории", callback_data=TEMPLATE_BACK)],
                    [InlineKeyboardButton("⬅️ В меню", callback_data=MENU_BACK)],
                ]
            ),
            parse_mode=ParseMode.HTML,
            edit=via_callback,
        )
        return

    category_label = next((cat["label"] for cat in catalog.get("categories", []) if cat.get("slug") == category_slug), category_slug)

    total = len(template_ids)
    total_pages = max(1, (total + TEMPLATE_PAGE_SIZE - 1) // TEMPLATE_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * TEMPLATE_PAGE_SIZE
    end = min(start + TEMPLATE_PAGE_SIZE, total)

    user_data = get_user_data(context)
    user_data["template_last_category"] = category_slug

    lines = [
        f"<b>{escape(category_label)}</b>",
        f"Шаблонов в категории: {total}.",
        "Выберите шаблон, чтобы заменить текущий workflow.",
        "",
    ]

    subset = template_ids[start:end]
    buttons: list[list[InlineKeyboardButton]] = []
    for offset, template_id in enumerate(subset, start=start + 1):
        template = templates_map.get(template_id)
        if not template:
            continue
        name = template.get("name") or template_id
        lines.append(f"{offset}. <b>{escape(str(name))}</b>")
        description = _template_description(template)
        if description:
            lines.append(f"   {escape(description)}")
        buttons.append([
            InlineKeyboardButton(str(name), callback_data=f"{TEMPLATE_SELECT_PREFIX}{template_id}")
        ])

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"{TEMPLATE_PAGE_PREFIX}{category_slug}:{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"{TEMPLATE_PAGE_PREFIX}{category_slug}:{page + 1}"))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton("⬅️ Категории", callback_data=TEMPLATE_BACK)])
    buttons.append([InlineKeyboardButton("⬅️ В меню", callback_data=MENU_BACK)])

    keyboard = InlineKeyboardMarkup(buttons)

    await respond(
        message_source,
        "\n".join(lines),
        keyboard,
        parse_mode=ParseMode.HTML,
        edit=via_callback,
    )


async def apply_template(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    template_id: str,
) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    user_data = get_user_data(context)

    catalog = await _ensure_template_catalog(context)
    template = catalog.get("templates", {}).get(template_id)
    if not template:
        if isinstance(message_source, CallbackQuery):
            try:
                await message_source.answer("Шаблон не найден", show_alert=True)
            except Exception:
                LOGGER.debug("Failed to notify missing template", exc_info=True)
        else:
            await respond(message_source, "⚠️ Шаблон не найден.", parse_mode=ParseMode.HTML)
        return

    workflow = template.get("workflow") if isinstance(template.get("workflow"), dict) else None
    if workflow is None:
        try:
            source_ref = template.get("source_info") or template.get("source_id") or template_id
            template_data = await resources.client.get_template(source_ref)
        except Exception as exc:
            LOGGER.exception("Failed to download template contents")
            reply_markup: Optional[InlineKeyboardMarkup] = None
            if isinstance(message_source, CallbackQuery):
                reply_markup = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Категории", callback_data=TEMPLATE_BACK)]]
                )
            await respond(
                message_source,
                f"⚠️ Не удалось загрузить шаблон:\n<code>{escape(str(exc))}</code>",
                reply_markup,
                parse_mode=ParseMode.HTML,
                edit=isinstance(message_source, CallbackQuery),
            )
            return

        workflow = _extract_workflow_from_template(template_data)
        if not isinstance(workflow, dict):
            await respond(
                message_source,
                "⚠️ Шаблон не содержит workflow.",
                parse_mode=ParseMode.HTML,
            )
            return

        template["workflow"] = workflow

    workflow_copy = _copy_workflow_graph(workflow)
    preferred_name = template.get("name") or "template"
    workflow_name = _unique_workflow_name(resources.storage, user_id, preferred_name)

    user_data["workflow"] = workflow_copy
    user_data["workflow_name"] = workflow_name

    _persist_workflow(resources, user_id, workflow_copy, workflow_name)
    await _flush_persistence(context)

    if isinstance(message_source, CallbackQuery):
        try:
            await message_source.answer("Шаблон применён")
        except Exception:  # pragma: no cover - optional ack
            LOGGER.debug("Failed to acknowledge template apply", exc_info=True)

    await respond(
        message_source,
        f"✅ Шаблон <b>{escape(str(template.get('name') or workflow_name))}</b> загружен как <code>{escape(workflow_name)}</code>.",
        _workflow_markup_for_source(context, message_source, user_id),
        parse_mode=ParseMode.HTML,
        edit=isinstance(message_source, CallbackQuery),
    )

    await show_workflow_overview(message_source, context, refresh=True)


async def show_workflow_library(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    page: int = 0,
    via_callback: bool = False,
) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)

    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "menu")

    if isinstance(message_source, CallbackQuery):
        try:
            await message_source.answer()
        except Exception:  # pragma: no cover - optional ack
            LOGGER.debug("Failed to acknowledge workflow library callback", exc_info=True)
        via_callback = True

    names = list(resources.storage.list_workflows(user_id))
    names.sort(key=lambda value: value.lower())

    total = len(names)
    if total == 0:
        await respond(
            message_source,
            "ℹ️ У вас пока нет сохранённых workflow.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data=MENU_BACK)]]),
            parse_mode=ParseMode.HTML,
            edit=via_callback,
        )
        return

    user_data = get_user_data(context)
    user_data["workflow_library_names"] = names

    total_pages = max(1, (total + WORKFLOW_LIBRARY_PAGE_SIZE - 1) // WORKFLOW_LIBRARY_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * WORKFLOW_LIBRARY_PAGE_SIZE
    end = min(start + WORKFLOW_LIBRARY_PAGE_SIZE, total)

    current_name = str(user_data.get("workflow_name") or "")

    lines = [
        "<b>🗃️ Ваши workflow</b>",
        f"Всего: {total}.",
        "Выберите запись, чтобы открыть и продолжить работу.",
        "",
    ]

    buttons: list[list[InlineKeyboardButton]] = []

    for offset, name in enumerate(names[start:end], start=start + 1):
        display = name
        if name == current_name:
            display = f"{name} (текущий)"
        lines.append(f"{offset}. <b>{escape(name)}</b>{' — текущий' if name == current_name else ''}")
        buttons.append([
            InlineKeyboardButton(display, callback_data=f"{WORKFLOW_SELECT_PREFIX}{offset - 1}")
        ])

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"{WORKFLOW_LIBRARY_PAGE_PREFIX}{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"{WORKFLOW_LIBRARY_PAGE_PREFIX}{page + 1}"))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"{WORKFLOW_LIBRARY_PAGE_PREFIX}{page}")])
    buttons.append([InlineKeyboardButton("⬅️ В меню", callback_data=MENU_BACK)])

    keyboard = InlineKeyboardMarkup(buttons)

    await respond(
        message_source,
        "\n".join(lines),
        keyboard,
        parse_mode=ParseMode.HTML,
        edit=via_callback,
    )


async def _load_saved_workflow(
    query: CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    index: int,
) -> None:
    user_id = query.from_user.id if query.from_user else None
    if user_id is None:
        await query.answer("Пользователь не найден", show_alert=True)
        return

    user_data = get_user_data(context)
    names = user_data.get("workflow_library_names")
    if not isinstance(names, list) or index < 0 or index >= len(names):
        await query.answer("Workflow недоступен", show_alert=True)
        return

    name = names[index]
    resources = require_resources(context)
    workflow = resources.storage.load_workflow(user_id, name)
    if not isinstance(workflow, dict):
        await query.answer("Не удалось открыть", show_alert=True)
        return

    user_data["workflow_name"] = name
    user_data["workflow"] = workflow

    try:
        await query.answer("Открыто")
    except Exception:  # pragma: no cover - optional ack
        LOGGER.debug("Failed to acknowledge workflow load", exc_info=True)

    await show_workflow_overview(query, context, refresh=True)


async def export_current_workflow(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)

    workflow = await ensure_workflow_loaded(context, resources, user_id)
    if not isinstance(workflow, dict):
        await respond(
            message_source,
            "ℹ️ Нет workflow для экспорта.",
            _workflow_markup_for_source(context, message_source, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    user_data = get_user_data(context)
    workflow_name = str(user_data.get("workflow_name") or "workflow")
    filename = _sanitize_filename(workflow_name) or "workflow"

    export_bytes = json.dumps(workflow, ensure_ascii=False, indent=2).encode("utf-8")
    buffer = BytesIO(export_bytes)
    buffer.name = f"{filename}.json"

    chat_id = get_chat_id_from_source(message_source)
    caption = f"📤 Экспорт workflow <code>{escape(workflow_name)}</code>"

    await context.bot.send_document(
        chat_id=chat_id,
        document=buffer,
        caption=caption,
        parse_mode=ParseMode.HTML,
    )


async def clear_queue(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    try:
        await resources.client.clear_queue()
    except Exception as exc:  # pragma: no cover - network failure
        LOGGER.exception("Failed to clear queue")
        await respond(
            message_source,
            f"⚠️ Не удалось очистить очередь:\n<code>{escape(str(exc))}</code>",
            _queue_reply_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        await _ensure_keyboard_mode(message_source, context, user_id, "queue")
        return

    await show_queue(message_source, context)


async def interrupt_queue(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    try:
        await resources.client.interrupt()
    except Exception as exc:  # pragma: no cover - network failure
        LOGGER.exception("Failed to interrupt queue")
        await respond(
            message_source,
            f"⚠️ Не удалось прервать выполнение:\n<code>{escape(str(exc))}</code>",
            _queue_reply_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        await _ensure_keyboard_mode(message_source, context, user_id, "queue")
        return

    task = ACTIVE_TASKS.get(user_id)
    if task and not task.done():
        task.cancel()

    active_run = get_user_data(context).get("active_run")
    prompt_id = active_run.get("prompt_id") if isinstance(active_run, dict) else None
    _log_history_entry(context, resources, user_id, prompt_id, status="cancelled")

    await show_queue(message_source, context)


async def restart_comfyui(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    command = resources.config.restart_command

    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "menu")

    if not command:
        await respond(
            message_source,
            "⚙️ Команда перезапуска не настроена. Укажите COMFYUI_RESTART_CMD в .env.",
            _menu_reply_keyboard(context, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    await respond(
        message_source,
        "🔄 Выполняю перезапуск ComfyUI…",
        _menu_reply_keyboard(context, user_id),
        parse_mode=ParseMode.HTML,
    )

    async def _restart() -> None:
        code = await resources.client.restart(command)
        status_text = "✅ ComfyUI перезапущен." if code == 0 else f"⚠️ Перезапуск завершился с кодом {code}."
        await respond(message_source, status_text, _menu_reply_keyboard(context, user_id), parse_mode=ParseMode.HTML)

    asyncio.create_task(_restart())


async def launch_workflow(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)

    user_data = get_user_data(context)
    workflow = user_data.get("workflow")
    if not workflow:
        await respond(
            message_source,
            "ℹ️ Сначала создайте или импортируйте workflow.",
            _workflow_markup_for_source(context, message_source, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        catalog = await ensure_catalog(context)
    except Exception as exc:  # pragma: no cover - network failure when fetching catalog
        LOGGER.exception("Failed to load catalog before launch")
        await respond(
            message_source,
            f"⚠️ Не удалось получить описание нод ComfyUI:\n<code>{escape(str(exc))}</code>",
            _workflow_markup_for_source(context, message_source, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    missing_types = normalize_workflow_structure(workflow, catalog)
    if missing_types:
        lines = ["<b>⚠️ Workflow содержит узлы без class_type</b>", ""]
        lines.extend(f"• Нода #{escape(node_id)}" for node_id in missing_types[:10])
        if len(missing_types) > 10:
            lines.append(f"• … и ещё {len(missing_types) - 10} узлов")
        lines.append("Добавьте class_type вручную или переимпортируйте корректный JSON.")
        await respond(
            message_source,
            "\n".join(lines),
            _workflow_markup_for_source(context, message_source, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    workflow_name = str(user_data.get("workflow_name") or "default")
    _persist_workflow(resources, user_id, workflow, workflow_name)
    await _flush_persistence(context)

    existing_task = ACTIVE_TASKS.get(user_id)
    if existing_task and not existing_task.done():
        run_info = user_data.get("active_run")
        chat_id = run_info.get("chat_id") if isinstance(run_info, dict) else None
        message_id = run_info.get("message_id") if isinstance(run_info, dict) else None
        if chat_id is not None and message_id is not None:
            LOGGER.debug("Workflow already running for user_id=%s, message_id=%s", user_id, message_id)

        await respond(
            message_source,
            "⏳ Выполнение уже запущено. Используйте кнопку ниже, чтобы остановить его.",
            _progress_reply_keyboard(context),
            parse_mode=ParseMode.HTML,
        )
        return

    validation_errors, validation_warnings = validate_workflow(workflow, catalog)
    if validation_errors:
        lines = ["<b>⚠️ Workflow не готов к запуску</b>", ""]
        lines.extend(f"• {escape(err)}" for err in validation_errors[:10])
        if len(validation_errors) > 10:
            lines.append(f"• … и ещё {len(validation_errors) - 10} ошибок")
        await respond(
            message_source,
            "\n".join(lines),
            _workflow_markup_for_source(context, message_source, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    warning_block = ""
    if validation_warnings:
        preview = validation_warnings[:5]
        warning_lines = ["<b>⚠️ Предупреждения каталога</b>"]
        warning_lines.extend(f"• {escape(item)}" for item in preview)
        if len(validation_warnings) > len(preview):
            warning_lines.append(f"• … и ещё {len(validation_warnings) - len(preview)} предупреждений")
        warning_block = "\n".join(warning_lines) + "\n\n"

    try:
        prompt_payload = build_prompt_payload(workflow)
    except ValueError as exc:
        await respond(
            message_source,
            f"⚠️ Не удалось подготовить workflow:\n<code>{escape(str(exc))}</code>",
            _workflow_markup_for_source(context, message_source, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    seed_overrides = _randomize_seed_inputs(prompt_payload)

    progress_labels = _build_progress_labels(workflow, prompt_payload)
    expected_outputs = _estimate_expected_outputs(prompt_payload)

    await respond(
        message_source,
        f"{warning_block}🚀 Отправляю workflow в ComfyUI…",
        _workflow_markup_for_source(context, message_source, user_id),
        parse_mode=ParseMode.HTML,
    )

    async def _run() -> None:
        status_message: Optional[Message] = None
        prompt_id: Optional[str] = None
        client_id: Optional[str] = None
        try:
            prompt_id, client_id = await resources.client.submit_workflow(prompt_payload)
        except Exception as exc:  # pragma: no cover - network failure
            LOGGER.exception("Failed to submit workflow")
            _log_history_entry(context, resources, user_id, None, status="error", error=str(exc))
            await respond(
                message_source,
                f"⚠️ Не удалось отправить workflow:\n<code>{escape(str(exc))}</code>",
                _workflow_markup_for_source(context, message_source, user_id),
                parse_mode=ParseMode.HTML,
            )
            return

        status_message = await respond(
            message_source,
            "⏳ Ожидаем прогресс от ComfyUI…",
            parse_mode=ParseMode.HTML,
        )

        if status_message is None:
            LOGGER.warning("Failed to obtain status message for user_id=%s", user_id)
            return

        await _ensure_keyboard_mode(status_message, context, user_id, "progress_active", ensure_message=True, force_send=True)

        initial_text = _format_progress_text(0, None, progress_labels)
        initial_message = await edit_message(status_message, initial_text)
        if isinstance(initial_message, Message):
            status_message = initial_message

        shared_dirs = _expand_shared_directories(resources.config.shared_output_dir)
        user_data["active_run"] = {
            "prompt_id": prompt_id,
            "client_id": client_id,
            "chat_id": status_message.chat_id,
            "message_id": status_message.message_id,
            "preview_message_id": None,
            "last_preview_digest": None,
            "expected_outputs": expected_outputs,
            "seed_overrides": seed_overrides,
            "shared_dirs": [str(path) for path in shared_dirs],
            "shared_snapshot": _snapshot_directories(shared_dirs),
            "output_scan_started": time.time(),
        }

        pending_progress_text: Optional[str] = None
        pending_preview: Optional[PreviewPayload] = None
        last_progress_broadcast = time.time() - PROGRESS_UPDATE_INTERVAL_SECONDS

        async def flush_progress(*, force: bool = False) -> None:
            nonlocal status_message, pending_progress_text, pending_preview, last_progress_broadcast
            if pending_progress_text is None:
                return
            if status_message is None:
                return
            now = time.time()
            if not force and now - last_progress_broadcast < PROGRESS_UPDATE_INTERVAL_SECONDS:
                return
            edited = await edit_message(status_message, pending_progress_text)
            if isinstance(edited, Message):
                status_message = edited
            if pending_preview is not None:
                await _update_preview_message(
                    context,
                    user_data,
                    status_message.chat_id,
                    pending_preview,
                    pending_progress_text,
                )
            last_progress_broadcast = now
            pending_progress_text = None
            pending_preview = None

        try:
            async for result in resources.client.track_progress(client_id, prompt_id):
                if isinstance(result, ProgressEvent):
                    progress = result.value
                    maximum = result.maximum or 100
                    pct = 0 if maximum == 0 else min(int(progress / maximum * 100), 100)
                    text = _format_progress_text(pct, result.node_id, progress_labels)
                    LOGGER.debug("Обновление прогресса: node=%s value=%s%% raw=(%s/%s)", result.node_id, pct, progress, maximum)
                    pending_progress_text = text
                    if result.preview:
                        pending_preview = result.preview
                    await flush_progress(force=pct >= 100)
                elif isinstance(result, ExecutionResult):
                    await flush_progress(force=True)
                    await handle_execution_result(
                        message_source,
                        context,
                        user_id,
                        resources,
                        prompt_id,
                        result,
                        status_message,
                    )
                    return
        except asyncio.CancelledError:
            await flush_progress(force=True)
            if status_message:
                edited = await edit_message(
                    status_message,
                    "⛔️ Выполнение остановлено.",
                )
                if isinstance(edited, Message):
                    status_message = edited
                await _ensure_keyboard_mode(status_message, context, user_id, "workflow", ensure_message=True, force_send=True)
            return
        except Exception as exc:  # pragma: no cover - unexpected runtime failure
            await flush_progress(force=True)
            LOGGER.exception("Error while tracking workflow progress")
            _log_history_entry(context, resources, user_id, prompt_id, status="error", error=str(exc))
            if status_message:
                edited = await edit_message(
                    status_message,
                    f"⚠️ Ошибка при получении прогресса:\n<code>{escape(str(exc))}</code>",
                )
                if isinstance(edited, Message):
                    status_message = edited
                await _ensure_keyboard_mode(status_message, context, user_id, "workflow", ensure_message=True, force_send=True)
            return
        finally:
            ACTIVE_TASKS.pop(user_id, None)
            user_data.pop("active_run", None)

    task = context.application.create_task(_run())
    ACTIVE_TASKS[user_id] = task


async def cancel_workflow(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)

    user_data = get_user_data(context)
    run_info = user_data.get("active_run")
    if not run_info:
        if isinstance(message_source, CallbackQuery):
            await message_source.answer("Нет активного запуска.", show_alert=True)
        else:
            await respond(
                message_source,
                "ℹ️ Нет активного запуска.",
                _workflow_markup_for_source(context, message_source, user_id),
                parse_mode=ParseMode.HTML,
            )
            await _ensure_keyboard_mode(message_source, context, user_id, "workflow")
        return

    if isinstance(message_source, CallbackQuery):
        await message_source.answer("Останавливаю…")

    chat_id = run_info.get("chat_id")
    message_id = run_info.get("message_id")

    if chat_id is not None and message_id is not None:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⏹ Запрошена остановка…",
                parse_mode=ParseMode.HTML,
            )
        except Exception:  # pragma: no cover - message may already be edited
            LOGGER.debug("Failed to update progress message before interrupt", exc_info=True)

    try:
        await resources.client.interrupt()
    except Exception as exc:  # pragma: no cover - network failure
        LOGGER.exception("Failed to interrupt workflow")
        await respond(
            message_source,
            f"⚠️ Не удалось остановить: <code>{escape(str(exc))}</code>",
            _progress_reply_keyboard(context),
            parse_mode=ParseMode.HTML,
        )
        await _ensure_keyboard_mode(message_source, context, user_id, "workflow")
        return

    task = ACTIVE_TASKS.get(user_id)
    if task and not task.done():
        task.cancel()

    await respond(
        message_source,
        "⏹ Запрос на остановку отправлен. Ожидайте завершения.",
        parse_mode=ParseMode.HTML,
    )
    await _ensure_keyboard_mode(message_source, context, user_id, "progress_active", ensure_message=True, force_send=True)

async def handle_execution_result(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    resources: BotResources,
    prompt_id: str,
    result: ExecutionResult,
    status_message: Message,
) -> None:
    user_data = get_user_data(context)
    run_state = user_data.get("active_run")
    expected_outputs: Optional[int] = None
    preview_message_id: Optional[int] = None
    seed_overrides: Optional[Dict[str, Dict[str, Any]]] = None
    if isinstance(run_state, dict):
        raw_expected = run_state.get("expected_outputs")
        if isinstance(raw_expected, int):
            expected_outputs = raw_expected
        raw_preview = run_state.get("preview_message_id")
        if isinstance(raw_preview, int):
            preview_message_id = raw_preview
        raw_seeds = run_state.get("seed_overrides")
        if isinstance(raw_seeds, dict):
            seed_overrides = raw_seeds

    if result.data.get("type") == "execution_error":
        error = result.data.get("data", {}).get("error", "Неизвестная ошибка")
        _log_history_entry(context, resources, user_id, prompt_id, status="error", error=str(error))
        edited = await edit_message(
            status_message,
            f"❌ Ошибка при выполнении:\n<code>{escape(str(error))}</code>",
        )
        if isinstance(edited, Message):
            status_message = edited
        await _ensure_keyboard_mode(status_message, context, user_id, "workflow", ensure_message=True, force_send=True)
        if preview_message_id:
            try:
                await context.bot.edit_message_caption(
                    chat_id=status_message.chat_id,
                    message_id=preview_message_id,
                    caption="❌ Выполнение завершилось ошибкой.",
                )
            except Exception:  # pragma: no cover - preview updates are best-effort
                LOGGER.debug("Failed to update preview caption on error", exc_info=True)
        notifications = _get_notification_settings(user_data)
        if notifications.get("failure", True):
            workflow_name = user_data.get("workflow_name", "default")
            await context.bot.send_message(
                chat_id=status_message.chat_id,
                text=(
                    "🔔 Генерация завершилась ошибкой."
                    f"\nWorkflow: <code>{escape(str(workflow_name))}</code>"
                    f"\n<code>{escape(str(error))}</code>"
                ),
                parse_mode=ParseMode.HTML,
            )
        return

    outputs = await _fetch_outputs_with_retry(
        resources.client,
        prompt_id,
        expected_outputs=expected_outputs,
    )

    if not outputs:
        _log_history_entry(context, resources, user_id, prompt_id, status="no_output")
        message_text = "⚠️ Выполнение завершено, но результаты не найдены."
        if expected_outputs:
            message_text += f"\nОжидалось изображений: {expected_outputs}."
        edited = await edit_message(
            status_message,
            message_text,
        )
        if isinstance(edited, Message):
            status_message = edited
        await _ensure_keyboard_mode(status_message, context, user_id, "workflow", ensure_message=True, force_send=True)
        if preview_message_id:
            try:
                await context.bot.edit_message_caption(
                    chat_id=status_message.chat_id,
                    message_id=preview_message_id,
                    caption=message_text[:1024],
                )
            except Exception:  # pragma: no cover - preview updates are best-effort
                LOGGER.debug("Failed to update preview caption when outputs missing", exc_info=True)
        notifications = _get_notification_settings(user_data)
        if notifications.get("failure", True):
            workflow_name = user_data.get("workflow_name", "default")
            await context.bot.send_message(
                chat_id=status_message.chat_id,
                text=(
                    "🔔 Генерация завершена без результатов."
                    f"\nWorkflow: <code>{escape(str(workflow_name))}</code>"
                ),
                parse_mode=ParseMode.HTML,
            )
        return

    raw_dirs = run_state.get("shared_dirs") if isinstance(run_state, dict) else None
    shared_dirs: List[Path] = []
    if isinstance(raw_dirs, list):
        for item in raw_dirs:
            if isinstance(item, str):
                shared_dirs.append(Path(item).resolve())

    if not shared_dirs:
        shared_dirs = _expand_shared_directories(resources.config.shared_output_dir)

    snapshot_data = run_state.get("shared_snapshot") if isinstance(run_state, dict) else None
    snapshot: Optional[Mapping[str, float]] = None
    if isinstance(snapshot_data, dict):
        snapshot = {
            str(key): float(value)
            for key, value in snapshot_data.items()
            if isinstance(key, str) and isinstance(value, (int, float))
        }

    scan_anchor = run_state.get("output_scan_started") if isinstance(run_state, dict) else None
    scan_started = float(scan_anchor) if isinstance(scan_anchor, (int, float)) else None
    if scan_started is None:
        scan_started = time.time() - 600

    target_dir = resources.config.output_dir / str(user_id) / prompt_id

    new_sources = _collect_new_shared_files(shared_dirs, snapshot, scan_started)
    files: List[Path] = []
    for source, base in new_sources:
        moved = _move_file_to_directory(source, target_dir, cleanup_root=base)
        if moved is not None:
            files.append(moved)

    if not files:
        downloaded = await resources.client.fetch_images(outputs, target_dir=target_dir)
        files.extend(downloaded)

    if not files:
        for candidate in shared_dirs:
            located = resources.client.locate_output_files(outputs, candidate)
            transfers: List[Path] = []
            for source in located:
                moved = _move_file_to_directory(source, target_dir, cleanup_root=candidate)
                if moved is not None:
                    transfers.append(moved)
            if transfers:
                files.extend(transfers)
                break

    if not files:
        fallback_sources = _collect_new_shared_files(shared_dirs, None, scan_started)
        for source, base in fallback_sources:
            moved = _move_file_to_directory(source, target_dir, cleanup_root=base)
            if moved is not None:
                files.append(moved)
        if not files:
            LOGGER.debug("Fallback scan did not locate any generated files in shared outputs")

    if not files:
        names = _extract_output_filenames(outputs)
        located = _search_shared_outputs_by_name(names, shared_dirs)
        for source, base in located:
            moved = _move_file_to_directory(source, target_dir, cleanup_root=base)
            if moved is not None:
                files.append(moved)

    if files:
        unique: List[Path] = []
        seen: set[Path] = set()
        for path in files:
            if path in seen:
                continue
            unique.append(path)
            seen.add(path)
        files = unique
    edited_status = await edit_message(
        status_message,
        "✅ Готово! Отправляю результаты…",
    )
    if isinstance(edited_status, Message):
        status_message = edited_status
    await _ensure_keyboard_mode(status_message, context, user_id, "workflow", ensure_message=True, force_send=True)

    bot = context.bot
    chat_id = status_message.chat_id

    for file_path in files:
        caption = f"Файл: {file_path.name}"
        await _send_generated_file(bot, chat_id, file_path, caption=caption)

    summary_text = "✅ Генерация завершена."
    if files:
        summary_text += " Результаты отправлены."
    else:
        summary_text += " Файлы не найдены."
    if expected_outputs:
        summary_text += f"\nОтправлено {len(files)} из ожидаемых {expected_outputs} изображений."
        if len(files) < expected_outputs:
            summary_text += " Проверьте каталог вывода ComfyUI."

    if seed_overrides:
        formatted = _format_seed_overrides(seed_overrides)
        if formatted:
            summary_text += "\n\n🎲 Использованные сиды:\n" + formatted

    final_status = await edit_message(
        status_message,
        summary_text,
    )
    if isinstance(final_status, Message):
        status_message = final_status
    await _ensure_keyboard_mode(status_message, context, user_id, "workflow", ensure_message=True, force_send=True)

    if preview_message_id:
        try:
            await context.bot.edit_message_caption(
                chat_id=status_message.chat_id,
                message_id=preview_message_id,
                caption="✅ Генерация завершена.",
            )
        except Exception:  # pragma: no cover - preview updates are best-effort
            LOGGER.debug("Failed to update preview caption on success", exc_info=True)

    _log_history_entry(
        context,
        resources,
        user_id,
        prompt_id,
        status="success",
        files=[str(path.name) for path in files],
        file_count=len(files),
    )

    notifications = _get_notification_settings(user_data)
    if notifications.get("success", True):
        note_lines = ["🔔 Генерация завершена успешно."]
        if files:
            note_lines.append(f"Файлов: {len(files)}")
        workflow_name = user_data.get("workflow_name", "default")
        note_lines.append(f"Workflow: <code>{escape(str(workflow_name))}</code>")
        await context.bot.send_message(
            chat_id=status_message.chat_id,
            text="\n".join(note_lines),
            parse_mode=ParseMode.HTML,
        )

    if isinstance(run_state, dict):
        run_state.pop("preview_message_id", None)
        run_state.pop("last_preview_digest", None)
        run_state.pop("expected_outputs", None)
        run_state.pop("seed_overrides", None)
        run_state.pop("shared_dirs", None)
        run_state.pop("shared_snapshot", None)
        run_state.pop("output_scan_started", None)


def _count_output_images(outputs: Dict[str, Any]) -> int:
    total = 0
    for node_outputs in outputs.values():
        images = node_outputs.get("images") if isinstance(node_outputs, dict) else None
        if isinstance(images, list):
            total += len(images)
    return total


def _move_file_to_directory(source: Path, target_dir: Path, *, cleanup_root: Optional[Path] = None) -> Optional[Path]:
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except Exception:  # pragma: no cover - filesystem issues
        LOGGER.warning("Failed to prepare target directory", exc_info=True)
        return None

    destination = target_dir / source.name
    if destination.exists():
        stem = destination.stem
        suffix = destination.suffix
        counter = 1
        while destination.exists():
            destination = target_dir / f"{stem}_{counter}{suffix}"
            counter += 1

    try:
        moved_path = Path(shutil.move(str(source), str(destination)))
    except Exception:  # pragma: no cover - filesystem issues
        LOGGER.warning("Failed to move generated file", exc_info=True)
        return None

    if cleanup_root is not None:
        _cleanup_empty_parents(source.parent, cleanup_root)

    return moved_path


async def _send_generated_file(bot: Bot, chat_id: int, file_path: Path, *, caption: Optional[str] = None) -> None:
    caption_text = caption or f"Файл: {file_path.name}"
    size = _safe_file_size(file_path)
    suffix = file_path.suffix.lower()
    is_image = suffix in IMAGE_EXTENSIONS
    force_document = not is_image
    warning_text: Optional[str] = None

    if is_image and size is not None and size > TELEGRAM_PHOTO_SIZE_LIMIT:
        force_document = True
        warning_text = _photo_too_large_message(file_path.name, size)

    try:
        with file_path.open("rb") as fp:
            if is_image and not force_document:
                try:
                    await bot.send_photo(chat_id=chat_id, photo=fp, caption=caption_text)
                    return
                except BadRequest as exc:
                    if _is_photo_too_large_error(exc):
                        force_document = True
                        if warning_text is None:
                            warning_text = _photo_too_large_message(file_path.name, size)
                    else:
                        LOGGER.warning("Failed to send photo %s as image: %s", file_path, exc)
                        force_document = True
                except Exception:  # pragma: no cover - delivery best-effort
                    LOGGER.warning("Unexpected error during photo delivery for %s", file_path, exc_info=True)
                    force_document = True

            if force_document and warning_text:
                await bot.send_message(chat_id=chat_id, text=warning_text)
                warning_text = None

            fp.seek(0)
            await bot.send_document(
                chat_id=chat_id,
                document=fp,
                filename=file_path.name,
                caption=caption_text,
            )
    except BadRequest as exc:  # pragma: no cover - delivery best-effort
        LOGGER.warning("Telegram API rejected generated file %s: %s", file_path, exc)
    except Exception:  # pragma: no cover - delivery best-effort
        LOGGER.warning("Failed to send generated file %s", file_path, exc_info=True)


def _cleanup_empty_parents(start: Path, stop: Path) -> None:
    try:
        current = start.resolve()
        stop_resolved = stop.resolve()
    except Exception:  # pragma: no cover - filesystem issues
        return

    if current == stop_resolved:
        return

    try:
        current.relative_to(stop_resolved)
    except ValueError:
        return

    while True:
        if current == stop_resolved:
            break
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent

    try:
        stop_resolved.rmdir()
    except OSError:
        pass


def _scan_directory(directory: Path) -> List[Tuple[Path, float]]:
    files: List[Tuple[Path, float]] = []
    try:
        iterator = directory.glob("**/*")
    except Exception:  # pragma: no cover - filesystem issues
        return files

    for entry in iterator:
        if not entry.is_file():
            continue
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            continue
        files.append((entry, mtime))

    return files


def _expand_shared_directories(primary: Path) -> List[Path]:
    candidates: List[Path] = []
    seen: set[str] = set()

    def _add(path: Path) -> None:
        resolved = path.resolve()
        key = str(resolved)
        if key in seen:
            return
        seen.add(key)
        candidates.append(resolved)

    _add(primary)

    if primary.name.lower() == "temp":
        parent = primary.parent
        if parent.name:
            alt = parent / "output" / parent.name
            _add(alt)

    return candidates


def _snapshot_directories(directories: Sequence[Path]) -> Dict[str, float]:
    snapshot: Dict[str, float] = {}
    for directory in directories:
        try:
            entries = _scan_directory(directory)
        except Exception:  # pragma: no cover - filesystem issues
            continue
        for path, mtime in entries:
            try:
                snapshot[str(path.resolve())] = mtime
            except OSError:
                continue
    return snapshot


def _collect_new_shared_files(
    directories: Sequence[Path],
    snapshot: Optional[Mapping[str, float]],
    min_mtime: Optional[float],
) -> List[Tuple[Path, Path]]:
    collected: List[Tuple[Path, float, Path]] = []
    for directory in directories:
        for path, mtime in _scan_directory(directory):
            collected.append((path, mtime, directory))

    collected.sort(key=lambda item: item[1])

    if snapshot is None and min_mtime is None:
        unique: List[Tuple[Path, Path]] = []
        seen_keys: set[str] = set()
        for path, _, base in collected:
            key = str(path.resolve())
            if key in seen_keys:
                continue
            seen_keys.add(key)
            unique.append((path, base))
        return unique

    results: List[Tuple[Path, Path]] = []
    seen: set[str] = set()
    for path, mtime, base in collected:
        if min_mtime is not None and mtime < min_mtime:
            continue
        key = str(path.resolve())
        if key in seen:
            continue
        previous = snapshot.get(key) if snapshot else None
        if previous is None or mtime > previous:
            results.append((path, base))
            seen.add(key)

    return results


def _extract_output_filenames(outputs: Mapping[str, Any]) -> List[str]:
    names: List[str] = []
    for node_outputs in outputs.values():
        if not isinstance(node_outputs, Mapping):
            continue
        images = node_outputs.get("images")
        if not isinstance(images, Sequence):
            continue
        for entry in images:
            if not isinstance(entry, Mapping):
                continue
            filename = entry.get("filename")
            if isinstance(filename, str) and filename:
                names.append(filename)
    return names


def _search_shared_outputs_by_name(
    filenames: Sequence[str],
    directories: Sequence[Path],
) -> List[Tuple[Path, Path]]:
    targets = [name for name in filenames if isinstance(name, str) and name]
    if not targets:
        return []

    results: List[Tuple[Path, Path]] = []
    seen_paths: set[str] = set()

    for directory in directories:
        try:
            dir_resolved = directory.resolve()
        except Exception:
            dir_resolved = directory

        for name in targets:
            direct_candidate = dir_resolved / name
            candidates = [direct_candidate]
            found_for_name = False

            if not direct_candidate.exists():
                try:
                    matches = list(dir_resolved.glob(f"**/{name}"))
                except Exception:
                    matches = []
                candidates.extend(matches)

            for candidate in candidates:
                try:
                    resolved = candidate.resolve()
                except Exception:
                    continue
                key = str(resolved)
                if key in seen_paths or not resolved.exists():
                    continue
                seen_paths.add(key)
                results.append((resolved, dir_resolved))
                found_for_name = True
                break

            if found_for_name:
                break

    return results


async def _fetch_outputs_with_retry(
    client: ComfyUIClient,
    prompt_id: str,
    *,
    expected_outputs: Optional[int] = None,
    idle_timeout: float = 60.0,
    poll_interval: float = 1.5,
) -> Dict[str, Any]:
    last_outputs: Dict[str, Any] = {}
    last_count = 0
    idle_anchor: Optional[float] = None
    start_time = time.monotonic()
    completed_states = {"completed", "success", "finished", "succeeded"}
    failed_states = {"failed", "error", "stopped", "cancelled", "canceled"}

    while True:
        history = await client.get_history(prompt_id)
        outputs = await gather_outputs(history, prompt_id)
        prompt_data = history.get("history", {}).get(prompt_id, {})
        status_info = prompt_data.get("status") if isinstance(prompt_data, dict) else None
        status_token = ""
        completed_flag = False
        job_failed = False
        if isinstance(status_info, dict):
            raw_status = status_info.get("status") or status_info.get("status_str")
            if isinstance(raw_status, str):
                status_token = raw_status.lower()
            completed_flag = bool(status_info.get("completed"))
            messages = status_info.get("messages")
            if isinstance(messages, list):
                for record in messages:
                    if isinstance(record, (list, tuple)) and record:
                        code = record[0]
                        if isinstance(code, str) and code.lower() in {"execution_error", "execution_failed"}:
                            job_failed = True
                            break
        job_finished = completed_flag or status_token in completed_states
        if not job_failed and status_token in failed_states:
            job_failed = True
        current_count = _count_output_images(outputs)

        if outputs:
            last_outputs = outputs

        if current_count > last_count:
            last_count = current_count
            idle_anchor = None
            if expected_outputs is not None and current_count >= expected_outputs:
                return outputs
        else:
            should_start_timer = job_finished or current_count > 0 or expected_outputs == 0
            if should_start_timer and idle_anchor is None:
                idle_anchor = time.monotonic()
            if idle_anchor is not None and time.monotonic() - idle_anchor >= idle_timeout:
                return outputs if outputs else last_outputs

        if expected_outputs == 0 and outputs:
            return outputs

        if job_failed:
            return outputs if outputs else last_outputs

        if idle_anchor is None and time.monotonic() - start_time >= idle_timeout:
            return outputs if outputs else last_outputs

        await asyncio.sleep(poll_interval)


async def show_node_details(update: MessageSource, context: ContextTypes.DEFAULT_TYPE, node_id: str) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return

    node = get_node(workflow, node_id)
    if not node:
        await respond(
            update,
            f"❗️ Нода #{node_id} не найдена.",
            _workflow_reply_keyboard(context, get_user_id_from_source(update)),
        )
        return

    node_type = node.get("class_type") or node.get("type") or "Unknown"
    title = node.get("_meta", {}).get("title") if isinstance(node.get("_meta"), dict) else None
    header = f"<b>Нода #{node_id}</b> — {escape(title)} ({escape(node_type)})" if title else f"<b>Нода #{node_id}</b> — {escape(node_type)}"

    catalog = await ensure_catalog(context)
    node_info = _get_catalog_node_info(catalog, node_type)
    connection_infos = _gather_connection_inputs(node_info)
    connection_keys = {info.name for info in connection_infos}

    lines = [header]
    raw_inputs = node.get("inputs")
    inputs: dict[str, Any] = raw_inputs if isinstance(raw_inputs, dict) else {}
    assert isinstance(inputs, dict)
    param_lines_added = False
    if inputs:
        param_keys = [key for key in inputs.keys() if key not in connection_keys]
        if param_keys:
            param_lines_added = True
            lines.append("<b>Параметры:</b>")
            for key in param_keys:
                value = inputs.get(key)
                lines.append(f"• <code>{escape(str(key))}</code>: {escape(repr(value))}")
        if connection_infos:
            lines.append("<b>Соединения:</b>")
            for info in connection_infos:
                value = inputs.get(info.name)
                status = _describe_connection_value(value)
                lines.append(f"• <code>{escape(info.name)}</code>: {escape(status)}")
    if not param_lines_added and not connection_infos:
        lines.append("<i>Параметров нет</i>")

    user_data = get_user_data(context)
    user_data["active_node_id"] = node_id

    buttons: list[list[str]] = []
    mapping: dict[str, ButtonAction] = {}

    if connection_infos:
        conn_label = "🔌 Соединения"
        buttons.append([conn_label])
        mapping[conn_label] = ("node_connections", node_id)

    if inputs:
        for key in [k for k in inputs.keys() if k not in connection_keys]:
            value = inputs.get(key)
            label = f"⚙️ {key} → {_shorten(value)}"
            buttons.append([label])
            mapping[label] = ("node_param", node_id, key)

    delete_label = "🗑 Удалить ноду"
    back_label = "⬅️ Назад"
    buttons.append([delete_label])
    buttons.append([back_label])
    mapping[delete_label] = ("node_delete", node_id)
    mapping[back_label] = ("node_back",)

    _set_dynamic_buttons(context, mapping)

    await respond(update, "\n".join(lines), ReplyKeyboardMarkup(buttons, resize_keyboard=True), parse_mode=ParseMode.HTML)


async def show_connection_inputs(update: MessageSource, context: ContextTypes.DEFAULT_TYPE, node_id: str) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return

    node = get_node(workflow, node_id)
    if not node:
        await respond(
            update,
            "❗️ Нода не найдена.",
            _workflow_reply_keyboard(context, get_user_id_from_source(update)),
            parse_mode=ParseMode.HTML,
        )
        return

    catalog = await ensure_catalog(context)
    node_type = node.get("class_type") or node.get("type") or "Unknown"
    connection_infos = _gather_connection_inputs(_get_catalog_node_info(catalog, node_type))

    raw_inputs = node.get("inputs")
    inputs: dict[str, Any] = raw_inputs if isinstance(raw_inputs, dict) else {}
    lines = [f"<b>Соединения ноды #{node_id}</b>"]

    if not connection_infos:
        lines.append("<i>Для этой ноды нет входов, требующих подключения.</i>")
        _reset_connection_state(context)
        mapping = {
            "⬅️ Назад": ("node_back",),
            "⬅️ К ноде": ("node_details", node_id),
        }
        buttons = [["⬅️ К ноде"], ["⬅️ Назад"]]
        _set_dynamic_buttons(context, mapping)
        await respond(update, "\n".join(lines), ReplyKeyboardMarkup(buttons, resize_keyboard=True), parse_mode=ParseMode.HTML)
        return

    _reset_connection_state(context)

    mapping: dict[str, ButtonAction] = {}
    buttons: list[list[str]] = []

    for info in connection_infos:
        value = inputs.get(info.name)
        prefix = "✅" if _is_connection_filled(value) else ("➖" if info.optional else "⚠️")
        lines.append(f"{prefix} <code>{escape(info.name)}</code>: {escape(_describe_connection_value(value))}")

    for info in connection_infos:
        value = inputs.get(info.name)
        label_prefix = "✅" if _is_connection_filled(value) else "🔌"
        button_text = f"{label_prefix} {info.name}"
        buttons.append([button_text])
        mapping[button_text] = ("conn_input", node_id, info.name)

    back_to_node = "⬅️ К ноде"
    back_text = "⬅️ Назад"
    buttons.append([back_to_node])
    buttons.append([back_text])
    mapping[back_to_node] = ("node_details", node_id)
    mapping[back_text] = ("node_back",)

    _set_dynamic_buttons(context, mapping)

    await respond(update, "\n".join(lines), ReplyKeyboardMarkup(buttons, resize_keyboard=True), parse_mode=ParseMode.HTML)


async def _prompt_node_delete(source: MessageSource, context: ContextTypes.DEFAULT_TYPE, node_id: str) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return

    node = get_node(workflow, node_id)
    if not node:
        await respond(source, f"❗️ Нода #{node_id} не найдена.")
        return

    text = (
        f"🗑 Вы уверены, что хотите удалить ноду #{node_id}?\n"
        "Все связи с другими нодами будут удалены."
    )

    confirm_label = "✅ Подтвердить удаление"
    cancel_label = "❌ Отмена"
    mapping = {
        confirm_label: ("node_delete_confirm", node_id),
        cancel_label: ("node_delete_cancel", node_id),
    }
    _set_dynamic_buttons(context, mapping)

    await respond(
        source,
        text,
        ReplyKeyboardMarkup([[confirm_label], [cancel_label]], resize_keyboard=True),
        parse_mode=ParseMode.HTML,
    )


async def _delete_node_confirmed(source: MessageSource, context: ContextTypes.DEFAULT_TYPE, node_id: str) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return

    if not _remove_node_from_workflow(workflow, node_id):
        await respond(source, f"⚠️ Нода #{node_id} уже удалена.")
        return

    _remove_node_references(workflow, node_id)
    _clear_node_related_state(context, node_id)

    resources = require_resources(context)
    user_id = get_user_id_from_source(source)
    name = get_user_data(context).get("workflow_name", "default")
    _persist_workflow(resources, user_id, workflow, name)
    await _flush_persistence(context)

    await respond(source, f"🗑 Нода #{node_id} удалена.")
    await show_workflow_overview(source, context, refresh=True)


def _remove_node_from_workflow(workflow: Dict[str, Any], node_id: str) -> bool:
    key = str(node_id)
    nodes = workflow.get("nodes")
    if isinstance(nodes, dict):
        removed = nodes.pop(key, None)
        if removed is None and key.isdigit():
            removed = nodes.pop(int(key), None)
        return removed is not None
    if isinstance(nodes, list):
        remaining: list[Any] = []
        removed = False
        for node in nodes:
            if isinstance(node, dict) and str(node.get("id")) == key:
                removed = True
                continue
            remaining.append(node)
        if removed:
            workflow["nodes"] = remaining
        return removed
    return False


def _remove_node_references(workflow: Dict[str, Any], node_id: str) -> None:
    node_key = str(node_id)
    nodes = workflow.get("nodes")
    items: Iterable[tuple[str, Dict[str, Any]]] = []
    if isinstance(nodes, dict):
        items = [(str(k), v) for k, v in nodes.items() if isinstance(v, dict)]
    elif isinstance(nodes, list):
        collected: list[tuple[str, Dict[str, Any]]] = []
        for entry in nodes:
            if isinstance(entry, dict):
                identifier = entry.get("id")
                collected.append((str(identifier), entry))
        items = collected

    for other_id, node in items:
        if other_id == node_key:
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue
        for key, value in list(inputs.items()):
            new_value, changed = _prune_connection_value(value, node_key)
            if not changed:
                continue
            inputs[key] = new_value

    links = workflow.get("links")
    if isinstance(links, list):
        filtered: list[Any] = []
        for entry in links:
            keep = True
            if isinstance(entry, list) and len(entry) >= 4:
                source = str(entry[1])
                target = str(entry[3])
                if source == node_key or target == node_key:
                    keep = False
            elif isinstance(entry, dict):
                source = entry.get("origin_id") or entry.get("source") or entry.get("from_node")
                target = entry.get("target_id") or entry.get("to_node")
                if str(source) == node_key or str(target) == node_key:
                    keep = False
            if keep:
                filtered.append(entry)
        if len(filtered) != len(links):
            workflow["links"] = filtered


def _prune_connection_value(value: Any, node_key: str) -> tuple[Any, bool]:
    if isinstance(value, list):
        if _is_multi_connection_value(value) or (value and isinstance(value[0], (list, tuple))):
            filtered = [entry for entry in value if not _connection_entry_matches(entry, node_key)]
            if len(filtered) != len(value):
                return filtered, True
            return value, False
        if value and isinstance(value[0], (str, int)) and str(value[0]) == node_key:
            return "", True
    elif isinstance(value, tuple):
        converted = list(value)
        new_value, changed = _prune_connection_value(converted, node_key)
        if changed:
            if isinstance(new_value, list):
                return new_value, True
            return new_value, True
    return value, False


def _connection_entry_matches(entry: Any, node_key: str) -> bool:
    if isinstance(entry, (list, tuple)) and entry:
        return str(entry[0]) == node_key
    return False


def _clear_node_related_state(context: ContextTypes.DEFAULT_TYPE, node_id: str) -> None:
    user_data = get_user_data(context)
    key = str(node_id)

    pending = user_data.get("pending_input")
    if isinstance(pending, dict) and str(pending.get("node_id")) == key:
        user_data.pop("pending_input", None)
        user_data.pop("pending_input_choices", None)

    queue = user_data.get("pending_required_links")
    if isinstance(queue, list):
        filtered = [item for item in queue if str(item.get("node_id")) != key]
        if filtered:
            user_data["pending_required_links"] = filtered
        else:
            user_data.pop("pending_required_links", None)

    required_params = user_data.get("pending_required_params")
    if isinstance(required_params, list):
        filtered_params = [item for item in required_params if str(item.get("node_id")) != key]
        if filtered_params:
            user_data["pending_required_params"] = filtered_params
        else:
            user_data.pop("pending_required_params", None)

    state = _get_connection_state(context)
    if state and (str(state.get("target_node")) == key or str(state.get("source_node")) == key):
        _reset_connection_state(context)

    if user_data.get("active_node_id") == key:
        user_data.pop("active_node_id", None)

    _clear_dynamic_buttons(context)


async def start_connection_selection(
    update: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    target_node_id: str,
    input_name: str,
    *,
    required: bool = False,
) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return

    node = get_node(workflow, target_node_id)
    if not node:
        await respond(
            update,
            "❗️ Нода не найдена.",
            _workflow_reply_keyboard(context, get_user_id_from_source(update)),
            parse_mode=ParseMode.HTML,
        )
        return

    catalog = await ensure_catalog(context)
    node_type = node.get("class_type") or node.get("type") or "Unknown"
    connection_infos = _gather_connection_inputs(_get_catalog_node_info(catalog, node_type))
    info = next((item for item in connection_infos if item.name == input_name), None)
    if not info:
        await respond(update, "⚠️ Вход не найден.")
        await show_connection_inputs(update, context, target_node_id)
        return

    candidates = _list_connection_candidates(workflow, target_node_id)
    state = {
        "target_node": str(target_node_id),
        "input_name": input_name,
        "multi": info.multi,
        "spec": info.spec,
        "required": required,
        "page": 0,
        "stage": "source",
        "candidates": candidates,
    }
    _set_connection_state(context, state)

    await show_connection_source_picker(update, context, page=0)


async def show_connection_source_picker(
    update: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    page: Optional[int] = None,
) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return
    state = _get_connection_state(context)
    if not state:
        await respond(
            update,
            "⚠️ Нет активного подключения.",
            _workflow_reply_keyboard(context, get_user_id_from_source(update)),
            parse_mode=ParseMode.HTML,
        )
        return

    target_node_id = state.get("target_node")
    target_node = get_node(workflow, target_node_id) if target_node_id else None
    input_name = state.get("input_name", "")
    candidates = state.get("candidates", [])

    if page is None:
        page = int(state.get("page", 0))
    pages = max((len(candidates) - 1) // CONNECTION_PAGE_SIZE + 1, 1)
    page = max(0, min(page, pages - 1))
    state["page"] = page
    state["stage"] = "source"

    slice_start = page * CONNECTION_PAGE_SIZE
    slice_end = slice_start + CONNECTION_PAGE_SIZE
    subset = candidates[slice_start:slice_end]

    inputs = target_node.get("inputs") if isinstance(target_node, dict) and isinstance(target_node.get("inputs"), dict) else {}
    current_value = inputs.get(input_name) if isinstance(inputs, dict) else None

    lines = ["<b>Выберите источник</b>"]
    if target_node_id:
        lines.append(f"Для ноды #{target_node_id}, вход <code>{escape(str(input_name))}</code>.")
    lines.append(f"Текущее значение: {escape(_describe_connection_value(current_value))}")
    if pages > 1:
        lines.append(f"Страница {page + 1}/{pages}")
    if not subset:
        lines.append("<i>Подходящих нод не найдено. Создайте дополнительные ноды.</i>")

    mapping: dict[str, ButtonAction] = {}
    buttons: list[list[str]] = []
    for idx, candidate in enumerate(subset, start=slice_start):
        label = f"{idx + 1}. {candidate['label']}"
        buttons.append([label])
        mapping[label] = ("conn_source", idx)

    if pages > 1:
        nav_row: list[str] = []
        if page > 0:
            prev_label = "⬅️ Предыдущая"
            nav_row.append(prev_label)
            mapping[prev_label] = ("conn_source_page", page - 1)
        if page < pages - 1:
            next_label = "➡️ Следующая"
            nav_row.append(next_label)
            mapping[next_label] = ("conn_source_page", page + 1)
        if nav_row:
            buttons.append(nav_row)

    if _is_connection_filled(current_value):
        clear_label = "❎ Очистить"
        buttons.append([clear_label])
        mapping[clear_label] = ("conn_clear",)

    back_label = "⬅️ Назад"
    buttons.append([back_label])
    mapping[back_label] = ("conn_back",)

    _set_dynamic_buttons(context, mapping)

    await respond(update, "\n".join(lines), ReplyKeyboardMarkup(buttons, resize_keyboard=True), parse_mode=ParseMode.HTML)


async def pick_connection_source(source: MessageSource, context: ContextTypes.DEFAULT_TYPE, index: int) -> None:
    state = _get_connection_state(context)
    if not state:
        await respond(source, "⚠️ Нет активного выбора.")
        return

    candidates = state.get("candidates", [])
    if index < 0 or index >= len(candidates):
        await respond(source, "⚠️ Выбор устарел.")
        return

    candidate = candidates[index]
    state["source_node"] = candidate["node_id"]
    state["source_label"] = candidate["label"]
    state["stage"] = "output"
    await show_connection_output_picker(source, context)


async def show_connection_output_picker(update: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return
    state = _get_connection_state(context)
    if not state:
        await respond(
            update,
            "⚠️ Нет активного подключения.",
            _workflow_reply_keyboard(context, get_user_id_from_source(update)),
            parse_mode=ParseMode.HTML,
        )
        return

    source_node_id = state.get("source_node")
    if not source_node_id:
        await respond(update, "⚠️ Сначала выберите источник.")
        return

    source_node = get_node(workflow, source_node_id)
    if not source_node:
        await respond(update, "⚠️ Источник не найден.")
        return

    catalog = await ensure_catalog(context)
    outputs = _gather_connection_outputs(catalog, source_node)
    state["outputs"] = outputs
    state["stage"] = "output"

    input_name = state.get("input_name", "")
    target_node_id = state.get("target_node", "")

    lines = ["<b>Выберите выход</b>"]
    lines.append(f"Источник: {escape(_format_node_label(source_node, str(source_node_id)))}")
    lines.append(f"Вход: <code>{escape(str(input_name))}</code> ноды #{target_node_id}")

    mapping: dict[str, ButtonAction] = {}
    buttons: list[list[str]] = []
    for idx, output in enumerate(outputs):
        label = f"{idx + 1}. {output.label}"
        buttons.append([label])
        mapping[label] = ("conn_output", idx)

    back_label = "⬅️ Назад"
    buttons.append([back_label])
    mapping[back_label] = ("conn_output_back",)

    _set_dynamic_buttons(context, mapping)

    await respond(update, "\n".join(lines), ReplyKeyboardMarkup(buttons, resize_keyboard=True), parse_mode=ParseMode.HTML)


async def apply_connection_choice(source: MessageSource, context: ContextTypes.DEFAULT_TYPE, output_index: int) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return
    state = _get_connection_state(context)
    if not state:
        await respond(source, "⚠️ Нет активного выбора.")
        return

    outputs: list[ConnectionOutputInfo] = state.get("outputs", [])
    if output_index < 0 or output_index >= len(outputs):
        await respond(source, "⚠️ Выход устарел.")
        return

    target_node_id = state.get("target_node")
    source_node_id = state.get("source_node")
    input_name = state.get("input_name")
    if not target_node_id or not source_node_id or not input_name:
        await respond(source, "⚠️ Недостаточно данных.")
        return

    target_node = get_node(workflow, target_node_id)
    if not target_node:
        await respond(source, "⚠️ Нода устарела.")
        _reset_connection_state(context)
        return

    inputs = target_node.setdefault("inputs", {})
    existing = inputs.get(input_name)
    selected_output = outputs[output_index]
    new_value = _build_connection_value(existing, source_node_id, selected_output, multi=bool(state.get("multi")))
    inputs[input_name] = new_value

    resources = require_resources(context)
    user_id = get_user_id_from_source(source)
    name = get_user_data(context).get("workflow_name", "default")
    _persist_workflow(resources, user_id, workflow, name)
    await _flush_persistence(context)

    await respond(source, "✅ Соединение обновлено.")

    if state.get("required"):
        queue = get_user_data(context).get("pending_required_links")
        if isinstance(queue, list) and queue and queue[0].get("node_id") == target_node_id and queue[0].get("link") == input_name:
            queue.pop(0)
            if not queue:
                get_user_data(context).pop("pending_required_links", None)
    _reset_connection_state(context)

    if await prompt_next_required_link(source, context):
        return

    await show_connection_inputs(source, context, target_node_id)


async def connection_back(source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = _get_connection_state(context)
    if not state:
        await respond(source, "⚠️ Нет активной операции.")
        return

    stage = state.get("stage")
    if stage == "output":
        state.pop("outputs", None)
        state["stage"] = "source"
        await show_connection_source_picker(source, context)
        return

    target_node_id = state.get("target_node")
    _reset_connection_state(context)
    if target_node_id:
        await show_connection_inputs(source, context, target_node_id)
    else:
        await respond(source, "⚠️ Нет активной операции.")


async def clear_connection_choice(source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return
    state = _get_connection_state(context)
    if not state:
        await respond(source, "⚠️ Нет активного выбора.")
        return

    target_node_id = state.get("target_node")
    input_name = state.get("input_name")
    if not target_node_id or not input_name:
        await respond(source, "⚠️ Недостаточно данных.")
        return

    target_node = get_node(workflow, target_node_id)
    if not target_node:
        await respond(source, "⚠️ Нода устарела.")
        _reset_connection_state(context)
        return

    inputs = target_node.setdefault("inputs", {})
    existing = inputs.get(input_name)
    if bool(state.get("multi")) or _is_multi_connection_value(existing):
        inputs[input_name] = []
    else:
        inputs[input_name] = ""

    resources = require_resources(context)
    user_id = get_user_id_from_source(source)
    name = get_user_data(context).get("workflow_name", "default")
    _persist_workflow(resources, user_id, workflow, name)
    await _flush_persistence(context)

    await respond(source, "✅ Связь удалена.")
    _reset_connection_state(context)
    if await prompt_next_required_link(source, context):
        return
    await show_connection_inputs(source, context, target_node_id)


async def prompt_next_required_link(update: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> bool:
    queue = get_user_data(context).get("pending_required_links")
    if not isinstance(queue, list) or not queue:
        return False
    if _get_connection_state(context):
        return False
    item = queue[0]
    target_node = item.get("node_id")
    link_name = item.get("link")
    if not target_node or not link_name:
        queue.pop(0)
        return False
    await start_connection_selection(update, context, str(target_node), str(link_name), required=True)
    return True


async def prompt_param_update(
    update: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    node_id: str,
    parameter: str,
) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return

    node = get_node(workflow, node_id)
    if not node:
        return

    _clear_dynamic_buttons(context)

    raw_inputs = node.get("inputs")
    inputs: dict[str, Any] = raw_inputs if isinstance(raw_inputs, dict) else {}
    current_value = inputs.get(parameter)

    catalog = await ensure_catalog(context)
    node_type = node.get("class_type") or node.get("type") or "Unknown"
    node_info = _get_catalog_node_info(catalog, node_type)
    quick_choices = await _collect_param_choices(context, node, node_info, parameter, current_value)

    get_user_data(context)["pending_input"] = {
        "node_id": node_id,
        "parameter": parameter,
        "original": current_value,
    }
    if quick_choices:
        get_user_data(context)["pending_input_choices"] = {
            "node_id": node_id,
            "parameter": parameter,
            "choices": quick_choices,
            "page": 0,
        }
    else:
        get_user_data(context).pop("pending_input_choices", None)

    if quick_choices and len(quick_choices) > PARAM_CHOICES_PAGE_SIZE:
        await _show_param_choices_page(update, context, node_id, parameter, current_value, quick_choices, page=0)
        return

    text_lines = [
        f"✏️ Введите новое значение для <b>{escape(parameter)}</b>.",
        f"Текущее значение: <code>{escape(repr(current_value))}</code>",
    ]
    if quick_choices:
        text_lines.append("Можно выбрать кнопку или отправить текст.")

    mapping: dict[str, ButtonAction] = {}
    rows: list[list[str]] = []
    if quick_choices:
        current_row: list[str] = []
        for idx, choice in enumerate(quick_choices, start=1):
            label = f"{idx}. {choice['label']}"
            current_row.append(label)
            mapping[label] = ("param_quick", node_id, parameter, idx - 1)
            if len(current_row) == 2:
                rows.append(current_row)
                current_row = []
        if current_row:
            rows.append(current_row)
        manual_label = "✏️ Ввести вручную"
        rows.append([manual_label])
        mapping[manual_label] = ("param_manual", node_id, parameter)
    cancel_label = "❎ Отменить"
    rows.append([cancel_label])
    mapping[cancel_label] = ("cancel_input",)

    _set_dynamic_buttons(context, mapping)

    await respond(update, "\n".join(text_lines), ReplyKeyboardMarkup(rows, resize_keyboard=True), parse_mode=ParseMode.HTML)


async def cancel_pending_input(update: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    get_user_data(context).pop("pending_input", None)
    get_user_data(context).pop("pending_required_params", None)
    get_user_data(context).pop("pending_input_choices", None)
    _clear_dynamic_buttons(context)
    await respond(
        update,
        "❎ Изменение отменено.",
        _workflow_markup_for_source(context, update, get_user_id_from_source(update)),
        parse_mode=ParseMode.HTML,
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message:
        return

    text = message.text
    if text is None:
        return

    user_data = get_user_data(context)

    pending = user_data.get("pending_input")
    if pending:
        workflow = user_data.get("workflow")
        if not workflow:
            return

        cleaned_text = text.strip()

        dynamic_action = _get_dynamic_action(context, cleaned_text)
        if dynamic_action and await _dispatch_dynamic_action(message, context, dynamic_action):
            return

        if cleaned_text in {"⬅️ Назад", "⬅️ В меню", "⬅️ Назад⬅️"}:
            await cancel_pending_input(message, context)
            return

        new_value_text = cleaned_text
        node_id = pending["node_id"]
        parameter = pending["parameter"]

        node = get_node(workflow, node_id)
        if not node:
            await message.reply_text("⚠️ Нода не найдена.")
            return

        inputs = node.setdefault("inputs", {})
        original = inputs.get(parameter)

        try:
            converted = convert_value(new_value_text, original)
        except ValueError as exc:
            await message.reply_text(f"⚠️ {exc}")
            return

        await _set_node_parameter_value(message, context, node_id, parameter, converted)
        return

    if user_data.get("awaiting_catalog_search"):
        await process_catalog_search_input(update, context)
        return

    action = _menu_action_from_text(text)
    if action and await _dispatch_menu_action(message, context, action):
        return

    workflow_action = _workflow_action_from_text(text)
    if workflow_action and await _dispatch_workflow_action(message, context, workflow_action):
        return

    queue_action = _queue_action_from_text(text)
    if queue_action and await _dispatch_queue_action(message, context, queue_action):
        return

    history_action = _history_action_from_text(text)
    if history_action and await _dispatch_history_action(message, context, history_action):
        return

    status_action = _status_action_from_text(text)
    if status_action and await _dispatch_status_action(message, context, status_action):
        return

    cleaned_text = text.strip()
    dynamic_action = _get_dynamic_action(context, cleaned_text)
    if dynamic_action and await _dispatch_dynamic_action(message, context, dynamic_action):
        return

    node_choice = _parse_workflow_node_selection(text)
    if node_choice:
        await show_node_details(message, context, node_choice)
        return


def convert_value(text: str, original: Any) -> Any:
    if _looks_like_connection_value(original):
        raise ValueError("Этот параметр представляет соединение. Используйте редактор связей.")
    if isinstance(original, bool):
        lowered = text.lower()
        if lowered in {"true", "1", "on", "yes", "да"}:
            return True
        if lowered in {"false", "0", "off", "no", "нет"}:
            return False
        raise ValueError("Введите true/false")
    if isinstance(original, int) and not isinstance(original, bool):
        return int(text)
    if isinstance(original, float):
        return float(text)
    if isinstance(original, list):
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("Для списков отправьте JSON массив.") from exc
        if not isinstance(data, list):
            raise ValueError("Нужно передать список в формате JSON.")
        return data
    if isinstance(original, dict):
        raise ValueError("Этот параметр представляет соединение. Используйте редактор связей.")
    return text


def _extract_param_spec(node_info: Optional[Dict[str, Any]], parameter: str) -> Any:
    if not isinstance(node_info, dict):
        return None
    raw_inputs = node_info.get("input") or node_info.get("inputs")
    if isinstance(raw_inputs, dict):
        return raw_inputs.get(parameter)
    return None


def _extract_spec_meta(spec: Any) -> Optional[Dict[str, Any]]:
    if isinstance(spec, dict):
        return spec
    if isinstance(spec, list) and len(spec) > 1 and isinstance(spec[1], dict):
        return spec[1]
    return None


def _is_boolean_spec(spec: Any, current_value: Any) -> bool:
    if isinstance(current_value, bool):
        return True
    head: Any = None
    if isinstance(spec, list) and spec:
        head = spec[0]
    elif isinstance(spec, dict):
        head = spec.get("type")
    if isinstance(head, str) and head.upper() == "BOOLEAN":
        return True
    meta = _extract_spec_meta(spec)
    if isinstance(meta, dict) and isinstance(meta.get("default"), bool):
        return True
    return False


def _normalize_choice_entries(raw: Any) -> list[tuple[str, Any]]:
    options: list[tuple[str, Any]] = []
    data = raw
    if isinstance(data, dict):
        data = list(data.items())
    if not isinstance(data, (list, tuple)):
        return options
    for item in data:
        label: Any
        value: Any
        if isinstance(item, (list, tuple)):
            length = len(item)
            if length == 0:
                continue
            if length >= 2:
                label, value = item[0], item[1]
            else:
                first = item[0]
                label = value = first
        elif isinstance(item, dict):
            value = item.get("value")
            if value is None:
                value = item.get("name") or item.get("id")
            label = item.get("label") or item.get("name") or item.get("title") or value
        else:
            label = value = item
        if value is None:
            value = label
        options.append((str(label), value))
    return options


def _build_quick_choices(node_info: Optional[Dict[str, Any]], parameter: str, current_value: Any) -> list[Dict[str, Any]]:
    spec = _extract_param_spec(node_info, parameter)
    if spec is None or _is_connection_spec(spec):
        return []
    choices: list[Dict[str, Any]] = []
    if _is_boolean_spec(spec, current_value):
        base = [("✔️ Да", True), ("✖️ Нет", False)]
        for label, value in base:
            prefix = "✅ " if value == current_value else ""
            choices.append({"label": f"{prefix}{label}", "value": value})
        return choices

    meta = _extract_spec_meta(spec)
    options = None
    if isinstance(meta, dict):
        options = meta.get("choices") or meta.get("enum") or meta.get("options")
    normalized = _normalize_choice_entries(options)
    if not normalized:
        return []
    for label, value in normalized:
        prefix = "✅ " if value == current_value else ""
        choices.append({"label": f"{prefix}{label}", "value": value})
    return choices


async def _collect_param_choices(
    context: ContextTypes.DEFAULT_TYPE,
    node: Dict[str, Any],
    node_info: Optional[Dict[str, Any]],
    parameter: str,
    current_value: Any,
) -> list[Dict[str, Any]]:
    static_choices = _build_quick_choices(node_info, parameter, current_value)
    if static_choices:
        return static_choices

    dynamic_choices = await _build_dynamic_model_choices(context, node, parameter, current_value)
    if dynamic_choices:
        return dynamic_choices

    return []


async def _build_dynamic_model_choices(
    context: ContextTypes.DEFAULT_TYPE,
    node: Dict[str, Any],
    parameter: str,
    current_value: Any,
) -> list[Dict[str, Any]]:
    node_type = node.get("class_type") or node.get("type") or ""
    model_type = _resolve_model_type(str(node_type), parameter)
    if not model_type:
        return []

    resources = require_resources(context)
    try:
        models = await resources.client.list_models(model_type, refresh=True)
    except Exception:  # pragma: no cover - best effort helper
        LOGGER.warning("Не удалось получить список моделей типа %s", model_type, exc_info=True)
        return []

    if not models:
        return []

    return _model_choices_from_names(models, current_value)


def _resolve_model_type(node_type: str, parameter: str) -> Optional[str]:
    key = (node_type, parameter)
    if key in MODEL_PARAM_TYPE_BY_NODE:
        return MODEL_PARAM_TYPE_BY_NODE[key]
    return GENERIC_MODEL_PARAM_TYPES.get(parameter)


def _model_choices_from_names(models: Iterable[str], current_value: Any) -> list[Dict[str, Any]]:
    normalized_current = str(current_value) if current_value is not None else None
    choices: list[Dict[str, Any]] = []
    seen: set[str] = set()

    def _display_name(name: str) -> str:
        sanitized = name.replace("\\", "/")
        if "/" in sanitized:
            return sanitized.split("/")[-1]
        return sanitized

    for name in models:
        if not isinstance(name, str):
            continue
        trimmed = name.strip()
        if not trimmed:
            continue
        lowered = trimmed.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        display = _display_name(trimmed)
        label = display
        if normalized_current and normalized_current.lower() == lowered:
            label = f"✅ {display}"
        elif normalized_current and normalized_current.lower() == display.lower():
            label = f"✅ {display}"
        choices.append({"label": label, "value": trimmed})

    return choices


async def _set_node_parameter_value(
    source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    node_id: str,
    parameter: str,
    new_value: Any,
) -> None:
    workflow = get_user_data(context).get("workflow")
    if not workflow:
        return

    node = get_node(workflow, node_id)
    if not node:
        await respond(
            source,
            "⚠️ Нода не найдена.",
            _workflow_markup_for_source(context, source, get_user_id_from_source(source), prefer_inline=True),
            parse_mode=ParseMode.HTML,
            edit=isinstance(source, CallbackQuery),
        )
        return

    inputs = node.setdefault("inputs", {})
    inputs[parameter] = new_value

    resources = require_resources(context)
    user_id = get_user_id_from_source(source)
    name = get_user_data(context).get("workflow_name", "default")
    _persist_workflow(resources, user_id, workflow, name)
    await _flush_persistence(context)

    await _after_parameter_update(source, context, node_id)


async def _after_parameter_update(
    source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    node_id: str,
) -> None:
    get_user_data(context).pop("pending_input", None)
    get_user_data(context).pop("pending_input_choices", None)

    user_id = get_user_id_from_source(source)
    await respond(
        source,
        "✅ Значение обновлено.",
        _workflow_markup_for_source(context, source, user_id),
        parse_mode=ParseMode.HTML,
        edit=isinstance(source, CallbackQuery),
    )

    queue = get_user_data(context).get("pending_required_params")
    if isinstance(queue, list) and queue:
        next_item = queue.pop(0)
        if queue:
            get_user_data(context)["pending_required_params"] = queue
        else:
            get_user_data(context).pop("pending_required_params", None)
        await prompt_param_update(source, context, next_item["node_id"], next_item["parameter"])
        return

    if await prompt_next_required_link(source, context):
        return

    await show_node_details(source, context, node_id)


async def show_param_choice_page(
    source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    node_id: str,
    parameter: str,
    page: int,
) -> None:
    choices_state = get_user_data(context).get("pending_input_choices")
    if not isinstance(choices_state, dict):
        await respond(source, "⚠️ Выбор устарел.", edit=isinstance(source, CallbackQuery))
        return
    if choices_state.get("node_id") != node_id or choices_state.get("parameter") != parameter:
        await respond(source, "⚠️ Выбор устарел.", edit=isinstance(source, CallbackQuery))
        return

    all_choices = choices_state.get("choices")
    if not isinstance(all_choices, list):
        await respond(source, "⚠️ Выбор устарел.", edit=isinstance(source, CallbackQuery))
        return

    pending_input = get_user_data(context).get("pending_input", {})
    current_value = pending_input.get("original")

    await _show_param_choices_page(source, context, node_id, parameter, current_value, all_choices, page)


async def _show_param_choices_page(
    source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    node_id: str,
    parameter: str,
    current_value: Any,
    all_choices: list[Dict[str, Any]],
    page: int,
) -> None:
    total = len(all_choices)
    total_pages = (total + PARAM_CHOICES_PAGE_SIZE - 1) // PARAM_CHOICES_PAGE_SIZE
    page = max(0, min(page, total_pages - 1))

    choices_state = get_user_data(context).get("pending_input_choices")
    if isinstance(choices_state, dict):
        choices_state["page"] = page

    start = page * PARAM_CHOICES_PAGE_SIZE
    end = start + PARAM_CHOICES_PAGE_SIZE
    page_choices = all_choices[start:end]

    text_lines = [
        f"✏️ Выберите значение для <b>{escape(parameter)}</b>",
        f"Текущее: <code>{escape(repr(current_value))}</code>",
        f"Страница {page + 1}/{total_pages}",
    ]

    buttons: list[list[InlineKeyboardButton]] = []
    for idx, choice in enumerate(page_choices):
        global_idx = start + idx
        label = choice["label"]
        buttons.append([InlineKeyboardButton(label, callback_data=f"{WORKFLOW_PARAM_QUICK_PREFIX}{node_id}:{parameter}:{global_idx}")])

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"{WORKFLOW_PARAM_PAGE_PREFIX}{node_id}:{parameter}:{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"{WORKFLOW_PARAM_PAGE_PREFIX}{node_id}:{parameter}:{page + 1}"))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton("✏️ Ввести вручную", callback_data=f"param:manual:{node_id}:{parameter}")])
    buttons.append([InlineKeyboardButton("❎ Отменить", callback_data="param:cancel")])

    markup = InlineKeyboardMarkup(buttons)
    await respond(source, "\n".join(text_lines), markup, parse_mode=ParseMode.HTML, edit=isinstance(source, CallbackQuery))


async def prompt_manual_param_input(
    source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    node_id: str,
    parameter: str,
) -> None:
    if isinstance(source, CallbackQuery):
        try:
            await source.answer()
        except Exception:
            LOGGER.debug("Failed to answer callback for manual param input", exc_info=True)
    pending_input = get_user_data(context).get("pending_input")
    if not isinstance(pending_input, dict):
        await respond(source, "⚠️ Ввод устарел.", edit=isinstance(source, CallbackQuery))
        return

    current_value = pending_input.get("original")
    text_lines = [
        f"✏️ Введите новое значение для <b>{escape(parameter)}</b>.",
        f"Текущее значение: <code>{escape(repr(current_value))}</code>",
    ]

    mapping: dict[str, ButtonAction] = {}
    cancel_label = "❎ Отменить"
    rows = [[cancel_label]]
    mapping[cancel_label] = ("cancel_input",)

    _set_dynamic_buttons(context, mapping)
    await respond(source, "\n".join(text_lines), ReplyKeyboardMarkup(rows, resize_keyboard=True), parse_mode=ParseMode.HTML)


async def apply_quick_param_choice(
    source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    node_id: str,
    parameter: str,
    index: int,
) -> None:
    choices_state = get_user_data(context).get("pending_input_choices")
    if not isinstance(choices_state, dict):
        await respond(source, "⚠️ Выбор устарел.")
        return
    if choices_state.get("node_id") != node_id or choices_state.get("parameter") != parameter:
        await respond(source, "⚠️ Выбор устарел.")
        return

    choices = choices_state.get("choices")
    if not isinstance(choices, list) or index < 0 or index >= len(choices):
        await respond(source, "⚠️ Выбор недоступен.")
        return

    choice = choices[index]
    value = choice.get("value")

    if isinstance(source, CallbackQuery):
        try:
            await source.answer()
        except Exception:  # pragma: no cover - optional ack
            LOGGER.debug("Failed to answer callback for quick param choice", exc_info=True)
    await _set_node_parameter_value(source, context, node_id, parameter, value)


def _log_history_entry(
    context: ContextTypes.DEFAULT_TYPE,
    resources: BotResources,
    user_id: int,
    prompt_id: Optional[str],
    *,
    status: str,
    files: Optional[List[str]] = None,
    file_count: Optional[int] = None,
    error: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    workflow = get_user_data(context).get("workflow") if hasattr(context, "user_data") else None
    workflow_name = get_user_data(context).get("workflow_name", "default") if hasattr(context, "user_data") else "default"

    if file_count is None:
        file_count = len(files) if files else 0

    entry: Dict[str, Any] = {
        "prompt_id": prompt_id,
        "workflow_name": workflow_name,
        "status": status,
        "file_count": file_count,
    }
    if files:
        entry["files"] = files
    if error:
        entry["error"] = error

    if isinstance(workflow, dict):
        try:
            entry["node_count"] = len(get_node_ids(workflow))
        except Exception:  # pragma: no cover - defensive path
            pass

    if extra:
        entry.update({k: v for k, v in extra.items() if v is not None})

    if hasattr(context, "user_data"):
        active_run = get_user_data(context).get("active_run")
        if isinstance(active_run, dict):
            seed_overrides = active_run.get("seed_overrides")
            if isinstance(seed_overrides, dict) and seed_overrides:
                entry.setdefault("seed_overrides", seed_overrides)

    resources.storage.append_history(user_id, entry)
async def ensure_workflow_loaded(
    context: ContextTypes.DEFAULT_TYPE,
    resources: BotResources,
    user_id: int,
    *,
    refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    cached = get_user_data(context).get("workflow")
    if cached and not refresh:
        return cached

    name = get_user_data(context).get("workflow_name", "default")
    workflow = resources.storage.load_workflow(user_id, name)
    if workflow is not None:
        get_user_data(context)["workflow"] = workflow
    return workflow


def _summarize_jobs(jobs: list[Any], *, limit: int = 5) -> list[str]:
    if not jobs:
        return ["  └ —"]

    lines: list[str] = []
    for job in jobs[:limit]:
        description = escape(_describe_job(job))
        lines.append(f"  └ <code>{description}</code>")
    remaining = len(jobs) - limit
    if remaining > 0:
        lines.append(f"  └ … и ещё {remaining}")
    return lines


async def show_node_categories(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    page: int = 0,
    *,
    refresh: bool = False,
    notice: Optional[str] = None,
) -> None:
    catalog = await ensure_catalog(context, refresh=refresh)
    categories: List[str] = catalog.get("categories", [])
    total = len(categories)
    user_id = get_user_id_from_source(message_source)

    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "workflow")

    lines: list[str] = []
    if notice:
        lines.append(notice)
        lines.append("")

    mapping: dict[str, ButtonAction] = {}
    buttons: list[list[str]] = []

    if not categories:
        lines.append("⚠️ Каталог нод пуст. Обновите список позже.")
        refresh_label = "🔄 Обновить"
        workflow_label = "⬅️ В workflow"
        buttons.append([refresh_label])
        buttons.append([workflow_label])
        mapping[refresh_label] = ("catalog_refresh",)
        mapping[workflow_label] = ("workflow_overview",)
        _set_dynamic_buttons(context, mapping)
        await respond(
            message_source,
            "\n".join(lines),
            ReplyKeyboardMarkup(buttons, resize_keyboard=True),
            parse_mode=ParseMode.HTML,
        )
        return

    total_pages = max(1, (total + CATEGORY_PAGE_SIZE - 1) // CATEGORY_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    get_user_data(context)["catalog_last_page"] = page

    start = page * CATEGORY_PAGE_SIZE
    end = min(start + CATEGORY_PAGE_SIZE, total)

    lines.extend(
        [
            "<b>Выберите категорию нод</b>",
            f"Всего категорий: {total}",
            f"Страница {page + 1} из {total_pages}.",
            "",
            "Нажмите на категорию, чтобы увидеть доступные ноды или воспользуйтесь поиском.",
        ]
    )

    row: list[str] = []
    for idx in range(start, end):
        label_text = _short_label(categories[idx])
        button_text = f"{idx + 1}. {label_text}"
        row.append(button_text)
        mapping[button_text] = ("catalog_category", idx)
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    if total_pages > 1:
        nav_row: list[str] = []
        if page > 0:
            prev_label = "◀️ Предыдущая"
            nav_row.append(prev_label)
            mapping[prev_label] = ("catalog_page", page - 1)
        page_label = f"📄 {page + 1}/{total_pages}"
        nav_row.append(page_label)
        mapping[page_label] = ("noop",)
        if page < total_pages - 1:
            next_label = "▶️ Следующая"
            nav_row.append(next_label)
            mapping[next_label] = ("catalog_page", page + 1)
        buttons.append(nav_row)

    search_label = "🔍 Поиск"
    refresh_label = "🔄 Обновить"
    workflow_label = "⬅️ В workflow"
    buttons.append([search_label, refresh_label])
    buttons.append([workflow_label])
    mapping[search_label] = ("catalog_search",)
    mapping[refresh_label] = ("catalog_refresh",)
    mapping[workflow_label] = ("workflow_overview",)

    _set_dynamic_buttons(context, mapping)

    await respond(
        message_source,
        "\n".join(lines),
        ReplyKeyboardMarkup(buttons, resize_keyboard=True),
        parse_mode=ParseMode.HTML,
    )


async def show_catalog_nodes(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    category_index: int,
    *,
    page: int = 0,
    refresh: bool = False,
) -> None:
    catalog = await ensure_catalog(context, refresh=refresh)
    categories: List[str] = catalog.get("categories", [])
    if category_index < 0 or category_index >= len(categories):
        await _query_back_to_catalog(message_source, context, "Категория устарела. Обновите каталог.")
        return

    category_name = categories[category_index]
    nodes_by_category: Dict[str, List[str]] = catalog.get("nodes_by_category", {})
    node_names = nodes_by_category.get(category_name, [])

    if not node_names:
        await _query_back_to_catalog(
            message_source,
            context,
            f"⚠️ В категории <b>{escape(category_name)}</b> пока нет нод.",
        )
        return

    total = len(node_names)
    total_pages = max(1, (total + NODE_PAGE_SIZE - 1) // NODE_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))

    get_user_data(context)["catalog_last_page"] = get_user_data(context).get("catalog_last_page", 0)
    get_user_data(context)["catalog_last_category"] = category_index
    get_user_data(context)["catalog_last_node_page"] = page

    start = page * NODE_PAGE_SIZE
    end = min(start + NODE_PAGE_SIZE, total)

    display_names: Dict[str, str] = catalog.get("display_names", {})
    user_id = get_user_id_from_source(message_source)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "workflow")

    lines = [
        f"<b>{escape(category_name)}</b>",
        f"Всего нод: {total}",
        f"Страница {page + 1} из {total_pages}.",
        "",
        "Выберите ноду, чтобы добавить её в текущий workflow.",
    ]

    mapping: dict[str, ButtonAction] = {}
    buttons: list[list[str]] = []
    row: list[str] = []

    for idx in range(start, end):
        node_key = node_names[idx]
        display = _short_label(display_names.get(node_key, node_key))
        button_text = f"{idx + 1}. {display}"
        row.append(button_text)
        mapping[button_text] = ("catalog_node", category_index, idx)
        if len(row) == 2:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    if total_pages > 1:
        nav_row: list[str] = []
        if page > 0:
            prev_label = "◀️ Предыдущая"
            nav_row.append(prev_label)
            mapping[prev_label] = ("catalog_node_page", category_index, page - 1)
        page_label = f"📄 {page + 1}/{total_pages}"
        nav_row.append(page_label)
        mapping[page_label] = ("noop",)
        if page < total_pages - 1:
            next_label = "▶️ Следующая"
            nav_row.append(next_label)
            mapping[next_label] = ("catalog_node_page", category_index, page + 1)
        buttons.append(nav_row)

    categories_label = "🗂 Категории"
    search_label = "🔍 Поиск"
    workflow_label = "⬅️ В workflow"
    buttons.append([categories_label, search_label])
    buttons.append([workflow_label])
    mapping[categories_label] = ("catalog_back",)
    mapping[search_label] = ("catalog_search",)
    mapping[workflow_label] = ("workflow_overview",)

    _set_dynamic_buttons(context, mapping)

    await respond(
        message_source,
        "\n".join(lines),
        ReplyKeyboardMarkup(buttons, resize_keyboard=True),
        parse_mode=ParseMode.HTML,
    )


async def _query_back_to_catalog(
    message_source: Message | Update | CallbackQuery,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
) -> None:
    page = int(get_user_data(context).get("catalog_last_page") or 0)
    await show_node_categories(message_source, context, page=page, notice=text)


async def prompt_catalog_search(message_source: MessageSource, context: ContextTypes.DEFAULT_TYPE) -> None:
    get_user_data(context)["awaiting_catalog_search"] = True
    get_user_data(context).pop("catalog_search_results", None)
    user_id = get_user_id_from_source(message_source)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "workflow")

    cancel_label = "❎ Отмена"
    categories_label = "🗂 Категории"
    buttons = [[categories_label, cancel_label]]
    mapping = {
        categories_label: ("catalog_back",),
        cancel_label: ("catalog_search_cancel",),
    }
    _set_dynamic_buttons(context, mapping)

    await respond(
        message_source,
        "🔍 Введите текст для поиска нод.\nИспользуйте часть названия или класса.",
        ReplyKeyboardMarkup(buttons, resize_keyboard=True),
        parse_mode=ParseMode.HTML,
    )


async def show_catalog_search_results(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    page: int = 0,
) -> None:
    data = get_user_data(context).get("catalog_search_results")
    if not isinstance(data, dict) or "matches" not in data:
        last_page = get_user_data(context).get("catalog_last_page", 0)
        await show_node_categories(message_source, context, page=last_page)
        return

    matches: list[dict[str, Any]] = data.get("matches", [])  # type: ignore[assignment]
    query = str(data.get("query", ""))

    total = len(matches)
    if total == 0:
        await _query_back_to_catalog(
            message_source,
            context,
            f"❗️ По запросу <code>{escape(query)}</code> ничего не найдено.",
        )
        return

    total_pages = max(1, (total + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    get_user_data(context)["catalog_search_page"] = page

    start = page * SEARCH_PAGE_SIZE
    end = min(start + SEARCH_PAGE_SIZE, total)

    user_id = get_user_id_from_source(message_source)
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(message_source, context, user_id, "workflow")

    buttons: list[list[str]] = []
    mapping: dict[str, ButtonAction] = {}
    lines = [
        f"<b>Результаты поиска</b> по <code>{escape(query)}</code>",
        f"Найдено совпадений: {total}",
        "",
    ]

    for offset, match in enumerate(matches[start:end], start=1):
        display = str(match.get("display"))
        category_name = str(match.get("category_name"))
        category_index = int(match.get("category_index", 0))
        node_index = int(match.get("node_index", 0))

        lines.append(f"{start + offset}. <code>{escape(display)}</code> — {escape(category_name)}")
        button_text = _short_label(display)
        buttons.append([button_text])
        mapping[button_text] = ("catalog_node", category_index, node_index)

    if total_pages > 1:
        nav_row: list[str] = []
        if page > 0:
            prev_label = "◀️ Предыдущая"
            nav_row.append(prev_label)
            mapping[prev_label] = ("catalog_search_page", page - 1)
        page_label = f"📄 {page + 1}/{total_pages}"
        nav_row.append(page_label)
        mapping[page_label] = ("noop",)
        if page < total_pages - 1:
            next_label = "▶️ Следующая"
            nav_row.append(next_label)
            mapping[next_label] = ("catalog_search_page", page + 1)
        buttons.append(nav_row)

    categories_label = "🗂 Категории"
    new_search_label = "🔍 Новый поиск"
    cancel_label = "❎ Отмена"
    buttons.append([categories_label, new_search_label])
    buttons.append([cancel_label])
    mapping[categories_label] = ("catalog_back",)
    mapping[new_search_label] = ("catalog_search",)
    mapping[cancel_label] = ("catalog_search_cancel",)

    _set_dynamic_buttons(context, mapping)

    await respond(
        message_source,
        "\n".join(lines),
        ReplyKeyboardMarkup(buttons, resize_keyboard=True),
        parse_mode=ParseMode.HTML,
    )


async def process_catalog_search_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if not message:
        return

    query_text = (message.text or "").strip()
    if not query_text:
        await message.reply_text("⚠️ Введите текст для поиска.")
        return

    catalog = await ensure_catalog(context)
    categories: List[str] = catalog.get("categories", [])
    nodes_by_category: Dict[str, List[str]] = catalog.get("nodes_by_category", {})
    display_names: Dict[str, str] = catalog.get("display_names", {})

    needle = query_text.casefold()
    matches: list[dict[str, Any]] = []

    for category_index, category_name in enumerate(categories):
        node_keys = nodes_by_category.get(category_name, [])
        for node_index, node_key in enumerate(node_keys):
            display = display_names.get(node_key, node_key)
            if needle in display.casefold() or needle in node_key.casefold():
                matches.append(
                    {
                        "category_index": category_index,
                        "category_name": category_name,
                        "node_index": node_index,
                        "display": display,
                        "class_type": node_key,
                    }
                )
                if len(matches) >= 200:
                    break
        if len(matches) >= 200:
            break

    get_user_data(context)["catalog_search_results"] = {
        "query": query_text,
        "matches": matches,
    }
    get_user_data(context).pop("awaiting_catalog_search", None)

    await show_catalog_search_results(message, context, page=0)


async def add_catalog_node(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    category_index: int,
    node_index: int,
) -> None:
    resources = require_resources(context)
    user_id = get_user_id_from_source(message_source)
    workflow = await ensure_workflow_loaded(context, resources, user_id)

    if workflow is None:
        await respond(
            message_source,
            "⚠️ Сначала создайте или импортируйте workflow.",
            _menu_reply_keyboard(context, user_id),
            parse_mode=ParseMode.HTML,
        )
        return

    catalog = await ensure_catalog(context)
    categories: List[str] = catalog.get("categories", [])
    if category_index < 0 or category_index >= len(categories):
        await _query_back_to_catalog(message_source, context, "Категория устарела. Обновите каталог.")
        return

    category_name = categories[category_index]
    node_names = catalog.get("nodes_by_category", {}).get(category_name, [])
    if node_index < 0 or node_index >= len(node_names):
        await _query_back_to_catalog(message_source, context, "Нода устарела. Обновите каталог.")
        return

    node_key = node_names[node_index]
    node_info = catalog.get("nodes", {}).get(node_key, {})
    display_name = catalog.get("display_names", {}).get(node_key, node_key)

    nodes_container = _ensure_nodes_dict(workflow)
    node_id = _allocate_node_id(nodes_container)

    inputs = _extract_default_inputs(node_info)
    required_params = _collect_required_params(node_info)
    required_links = _collect_required_links(node_info)
    for param in required_params:
        inputs.setdefault(param, "")
    new_node: Dict[str, Any] = {
        "id": int(node_id),
        "class_type": node_key,
        "inputs": inputs,
    }

    if display_name:
        new_node.setdefault("_meta", {})
        if isinstance(new_node["_meta"], dict):
            new_node["_meta"]["title"] = display_name

    nodes_container[node_id] = new_node

    name = get_user_data(context).get("workflow_name", "default")
    _persist_workflow(resources, user_id, workflow, name)
    await _flush_persistence(context)

    _enqueue_required_links(context, node_id, required_links)
    if required_params:
        get_user_data(context)["pending_required_params"] = [
            {"node_id": node_id, "parameter": param} for param in required_params[1:]
        ]
        await prompt_param_update(message_source, context, node_id, required_params[0])
        return

    if required_links:
        if await prompt_next_required_link(message_source, context):
            return

    await respond(
        message_source,
        f"➕ Нода <code>{escape(str(display_name))}</code> добавлена.",
        _workflow_markup_for_source(context, message_source, user_id),
        parse_mode=ParseMode.HTML,
    )
    await show_node_details(message_source, context, node_id)


def _short_label(text: str, limit: int = 24) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _ensure_nodes_dict(workflow: Dict[str, Any]) -> Dict[str, Any]:
    nodes = workflow.get("nodes")
    if isinstance(nodes, dict):
        return nodes

    mapping: Dict[str, Any] = {}
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            if node_id is None:
                continue
            mapping[str(node_id)] = node

    workflow["nodes"] = mapping
    return mapping


def _allocate_node_id(nodes: Dict[str, Any]) -> str:
    next_id = 1
    for key in nodes.keys():
        try:
            value = int(key)
        except (ValueError, TypeError):  # pragma: no cover - defensive path
            continue
        if value >= next_id:
            next_id = value + 1
    return str(next_id)


_INPUT_GROUP_KEYS = {"required", "optional", "hidden", "advanced", "ui", "basic"}
_PARAM_SPEC_HINT_KEYS = {
    "type",
    "default",
    "optional",
    "input_link",
    "forceInput",
    "choices",
    "enum",
    "min",
    "max",
    "step",
    "tooltip",
    "label",
}

_UI_ONLY_NODE_TYPES = {"MarkdownNote", "Note"}


def _iter_input_items(mapping: Any, *, group: Optional[str] = None) -> Iterable[tuple[str, Any, Optional[str]]]:
    if not isinstance(mapping, dict):
        return

    for name, spec in mapping.items():
        if isinstance(spec, dict) and (
            name in _INPUT_GROUP_KEYS
            or not (set(spec.keys()) & _PARAM_SPEC_HINT_KEYS)
        ):
            next_group = name if name in _INPUT_GROUP_KEYS else group
            yield from _iter_input_items(spec, group=next_group)
        else:
            yield str(name), spec, group


def _flatten_workflow_inputs(inputs: Any) -> tuple[Dict[str, Any], Optional[Any]]:
    if not isinstance(inputs, dict):
        return {}, None

    flat: Dict[str, Any] = {}
    required_fallback: Optional[Any] = None

    for key, value in inputs.items():
        if key in _INPUT_GROUP_KEYS:
            if isinstance(value, dict):
                for inner_key, inner_value in value.items():
                    flat[str(inner_key)] = inner_value
            elif key == "required" and required_fallback is None and not isinstance(value, dict):
                required_fallback = value
        else:
            flat[str(key)] = value

    return flat, required_fallback


def _consume_widget_value(pool: list[Any], spec: Any) -> Any:
    if not pool or spec is None:
        return None

    def _pop(predicate: Any) -> Any:
        for index, candidate in enumerate(pool):
            try:
                if predicate(candidate):
                    return pool.pop(index)
            except Exception:  # pragma: no cover - safeguard against unexpected types
                continue
        return None

    if isinstance(spec, list) and spec:
        first = spec[0]
        if isinstance(first, list):
            choices = {str(item) for item in first}

            def _is_choice(candidate: Any) -> bool:
                return isinstance(candidate, str) and candidate in choices

            return _pop(_is_choice)

        if isinstance(first, str):
            token = first.upper()
            if token in {"INT", "FLOAT"}:

                def _is_number(candidate: Any) -> bool:
                    return isinstance(candidate, (int, float))

                return _pop(_is_number)
            if token == "BOOLEAN":

                def _is_bool(candidate: Any) -> bool:
                    return isinstance(candidate, bool)

                return _pop(_is_bool)
            if token in {"STRING", "COMBO", "COMBOBOX", "DROPDOWN", "SELECT"}:

                def _is_str(candidate: Any) -> bool:
                    return isinstance(candidate, str)

                return _pop(_is_str)

    if isinstance(spec, dict):
        if "choices" in spec and isinstance(spec["choices"], (list, tuple)):
            choices = {str(item) for item in spec["choices"]}

            def _is_choice(candidate: Any) -> bool:
                return isinstance(candidate, str) and candidate in choices

            return _pop(_is_choice)
        if spec.get("type") in {"int", "float"}:

            def _is_number(candidate: Any) -> bool:
                return isinstance(candidate, (int, float))

            return _pop(_is_number)
        if spec.get("type") == "string":

            def _is_str(candidate: Any) -> bool:
                return isinstance(candidate, str)

            return _pop(_is_str)

    return None


def _extract_default_inputs(node_info: Dict[str, Any]) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {}
    raw_inputs = node_info.get("input") or node_info.get("inputs")
    for name, spec, _ in _iter_input_items(raw_inputs):
        default = _extract_default_from_spec(spec)
        if default is not None:
            defaults[name] = default
    return defaults


def _extract_default_from_spec(spec: Any) -> Any:
    if isinstance(spec, dict):
        return spec.get("default")

    if isinstance(spec, list) and spec:
        candidate = spec[1] if len(spec) > 1 else None
        if isinstance(candidate, dict):
            return candidate.get("default")
        if candidate not in (None, ""):
            return candidate

    return None


def _collect_required_params(node_info: Dict[str, Any]) -> list[str]:
    required: list[str] = []
    raw_inputs = node_info.get("input") or node_info.get("inputs")
    for name, spec, group in _iter_input_items(raw_inputs):
        if group == "hidden":
            continue
        if group not in (None, "required"):
            continue
        if _is_connection_spec(spec):
            continue
        if _is_optional_spec(spec):
            continue
        default = _extract_default_from_spec(spec)
        if default is None:
            required.append(name)
    return required


def _is_optional_spec(spec: Any) -> bool:
    if isinstance(spec, dict):
        return bool(spec.get("optional"))
    if isinstance(spec, list) and len(spec) > 1 and isinstance(spec[1], dict):
        return bool(spec[1].get("optional"))
    return False


def _is_connection_spec(spec: Any) -> bool:
    if isinstance(spec, str):
        token = spec.strip()
        return bool(token) and token.isupper()
    if isinstance(spec, dict) and spec.get("input_link"):
        return True
    if isinstance(spec, list):
        meta = spec[1] if len(spec) > 1 else None
        if isinstance(meta, dict) and meta.get("input_link"):
            return True
        if spec:
            first = spec[0]
            if isinstance(first, str):
                token = first.upper()
                scalar_types = {"INT", "FLOAT", "STRING", "BOOLEAN"}
                if token in scalar_types:
                    return False
                parameter_meta_keys = {"default", "choices", "options", "widget", "min", "max", "step", "round", "multiselect"}
                if isinstance(meta, dict) and any(key in meta for key in parameter_meta_keys):
                    return False
                if first.isupper():
                    return True
    return False


def _collect_required_links(node_info: Dict[str, Any]) -> list[str]:
    required: list[str] = []
    raw_inputs = node_info.get("input") or node_info.get("inputs")
    for name, spec, group in _iter_input_items(raw_inputs):
        if group == "hidden":
            continue
        if group not in (None, "required"):
            continue
        if not _is_connection_spec(spec):
            continue
        if _is_optional_spec(spec):
            continue
        required.append(name)
    return required


def _is_multi_connection_spec(spec: Any) -> bool:
    meta: Optional[Dict[str, Any]] = None
    if isinstance(spec, dict):
        meta = spec
    elif isinstance(spec, list) and len(spec) > 1 and isinstance(spec[1], dict):
        meta = spec[1]
    if not isinstance(meta, dict):
        return False
    return bool(
        meta.get("multiselect")
        or meta.get("multiple")
        or meta.get("allow_multiple")
        or meta.get("multi")
        or meta.get("forceInputList")
        or meta.get("accept_list")
    )


def _get_catalog_node_info(catalog: Dict[str, Any], node_type: str) -> Optional[Dict[str, Any]]:
    nodes = catalog.get("nodes")
    if isinstance(nodes, dict):
        info = nodes.get(node_type)
        if isinstance(info, dict):
            return info
    return None


def _gather_connection_inputs(node_info: Optional[Dict[str, Any]]) -> list[ConnectionInputInfo]:
    if not isinstance(node_info, dict):
        return []
    raw_inputs = node_info.get("input") or node_info.get("inputs")
    result: list[ConnectionInputInfo] = []
    for name, spec, group in _iter_input_items(raw_inputs):
        if group == "hidden":
            continue
        if not _is_connection_spec(spec):
            continue
        optional_flag = _is_optional_spec(spec) or group not in (None, "required")
        result.append(
            ConnectionInputInfo(
                name=str(name),
                spec=spec,
                optional=optional_flag,
                multi=_is_multi_connection_spec(spec),
            )
        )
    return result


def _gather_connection_outputs(catalog: Dict[str, Any], node: Dict[str, Any]) -> list[ConnectionOutputInfo]:
    class_type = node.get("class_type") or node.get("type")
    node_info = _get_catalog_node_info(catalog, class_type) if class_type else None
    outputs: list[ConnectionOutputInfo] = []

    if isinstance(node_info, dict):
        raw_outputs = (
            node_info.get("output")
            or node_info.get("outputs")
            or node_info.get("return")
            or node_info.get("returns")
        )
        if isinstance(raw_outputs, dict):
            for index, (name, _) in enumerate(raw_outputs.items()):
                outputs.append(ConnectionOutputInfo(index=index, label=str(name), name=str(name)))
        elif isinstance(raw_outputs, list):
            for index, item in enumerate(raw_outputs):
                if isinstance(item, dict):
                    label = item.get("name") or item.get("label") or item.get("type") or f"output{index}"
                else:
                    label = str(item)
                outputs.append(ConnectionOutputInfo(index=index, label=str(label), name=str(label)))

    if not outputs:
        outputs.append(ConnectionOutputInfo(index=0, label="output"))

    return outputs


def _describe_connection_value(value: Any) -> str:
    if not _is_connection_filled(value):
        return "нет"
    if isinstance(value, (list, tuple)):
        entry: Any
        if value and isinstance(value[0], (list, tuple)):
            entry = value[0]
        else:
            entry = value
        if isinstance(entry, (list, tuple)) and entry:
            source = entry[0] if len(entry) > 0 else "?"
            port = entry[2] if len(entry) > 2 and entry[2] not in (None, "") else entry[1] if len(entry) > 1 else "?"
            return f"#{source} → {port}"
    return str(value)


def _is_multi_connection_value(value: Any) -> bool:
    if not isinstance(value, list) or not value:
        return False
    return all(isinstance(item, (list, tuple)) for item in value)


def _build_connection_value(existing: Any, source_node_id: str, output: ConnectionOutputInfo, *, multi: bool) -> Any:
    payload: list[Any] = [str(source_node_id), output.index]
    if output.name:
        payload.append(output.name)

    if multi or _is_multi_connection_value(existing):
        return [payload]
    return payload


def _looks_like_connection_value(value: Any) -> bool:
    if isinstance(value, list):
        if not value:
            return False
        element = value[0]
        if isinstance(element, (list, tuple)):
            if not element:
                return False
            source = element[0]
            target = element[1] if len(element) > 1 else None
            return isinstance(source, (str, int)) and (target is None or isinstance(target, (int, str)))
        if len(value) < 2:
            return False
        second = value[1]
        return isinstance(element, (str, int)) and isinstance(second, (int, str))
    return False


def _format_node_label(node: Dict[str, Any], node_id: str) -> str:
    meta = node.get("_meta") if isinstance(node.get("_meta"), dict) else None
    title = meta.get("title") if isinstance(meta, dict) else None
    class_type = node.get("class_type") or node.get("type") or "Unknown"
    if title:
        return f"#{node_id} {title}"
    return f"#{node_id} {class_type}"


def _build_progress_labels(workflow: Dict[str, Any], prompt_payload: Dict[str, Any]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    nodes_index = {node_id: node for node_id, node in _iter_workflow_nodes(workflow)}
    for node_id in prompt_payload.keys():
        key = str(node_id)
        node = nodes_index.get(key)
        if node:
            mapping[key] = _format_node_label(node, key)
        else:
            mapping[key] = f"Нода #{key}"
    return mapping


def _format_progress_text(progress_pct: int, node_id: Optional[str | int], labels: Dict[str, str]) -> str:
    node_label: Optional[str] = None
    if node_id is not None:
        key = str(node_id)
        node_label = labels.get(key) or f"Нода #{key}"
    if node_label:
        return f"⏳ {node_label}\nПрогресс: {progress_pct}%"
    return f"⏳ Прогресс: {progress_pct}%"


def _estimate_expected_outputs(prompt_payload: Dict[str, Any]) -> int:
    count = 0
    for node in prompt_payload.values():
        if not isinstance(node, dict):
            continue
        class_type = node.get("class_type") or node.get("type")
        if isinstance(class_type, str) and class_type in SAVE_OUTPUT_NODE_TYPES:
            count += 1
    return count


def _preview_extension(mime: str | None) -> str:
    if not isinstance(mime, str):
        return "png"
    token = mime.lower()
    mapping = {
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/webp": "webp",
    }
    return mapping.get(token, "png")


def _preview_bytes_io(preview: PreviewPayload) -> BytesIO:
    buffer = BytesIO(preview.image)
    buffer.name = f"preview.{_preview_extension(preview.mime_type)}"
    buffer.seek(0)
    return buffer


async def _update_preview_message(
    context: ContextTypes.DEFAULT_TYPE,
    user_data: UserDataDict,
    chat_id: int,
    preview: PreviewPayload,
    caption: str,
) -> None:
    if not preview.image:
        return

    run_state = user_data.get("active_run")
    if not isinstance(run_state, dict):
        return

    caption = caption[:1024]
    digest = hashlib.sha1(preview.image).hexdigest()
    message_id = run_state.get("preview_message_id")
    bot = context.bot

    try:
        if message_id:
            if run_state.get("last_preview_digest") == digest:
                await bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=caption)
                run_state["last_preview_digest"] = digest
            else:
                media = InputMediaPhoto(_preview_bytes_io(preview), caption=caption)
                await bot.edit_message_media(chat_id=chat_id, message_id=message_id, media=media)
                run_state["last_preview_digest"] = digest
        else:
            message = await bot.send_photo(chat_id=chat_id, photo=_preview_bytes_io(preview), caption=caption)
            run_state["preview_message_id"] = message.message_id
            run_state["last_preview_digest"] = digest
    except Exception:  # pragma: no cover - preview updates are best-effort
        LOGGER.debug("Failed to update preview message", exc_info=True)


def _list_connection_candidates(workflow: Dict[str, Any], target_node_id: str) -> list[Dict[str, str]]:
    candidates: list[Dict[str, str]] = []
    for node_id, node_data in _iter_workflow_nodes(workflow):
        if str(node_id) == str(target_node_id):
            continue
        if not isinstance(node_data, dict):
            continue
        candidates.append({"node_id": str(node_id), "label": _format_node_label(node_data, str(node_id))})

    def _sort_key(item: Dict[str, str]) -> tuple[Any, str]:
        identifier = item.get("node_id", "")
        try:
            return (int(identifier), identifier)
        except ValueError:
            return (10**9, identifier)

    return sorted(candidates, key=_sort_key)


def _enqueue_required_links(context: ContextTypes.DEFAULT_TYPE, node_id: str, links: list[str]) -> None:
    if not links:
        return
    queue = get_user_data(context).get("pending_required_links")
    if not isinstance(queue, list):
        queue = []
    existing = {(item.get("node_id"), item.get("link")) for item in queue if isinstance(item, dict)}
    for link in links:
        key = (node_id, link)
        if key in existing:
            continue
        queue.append({"node_id": node_id, "link": link})
    if queue:
        get_user_data(context)["pending_required_links"] = queue


def _get_connection_state(context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, Any]]:
    state = get_user_data(context).get("connection_state")
    return state if isinstance(state, dict) else None


def _set_connection_state(context: ContextTypes.DEFAULT_TYPE, state: Dict[str, Any]) -> None:
    get_user_data(context)["connection_state"] = state


def _reset_connection_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    get_user_data(context).pop("connection_state", None)


def normalize_workflow_structure(workflow: Dict[str, Any], catalog: Optional[Dict[str, Any]] = None) -> list[str]:
    missing: list[str] = []

    nodes_raw = workflow.get("nodes")
    if isinstance(nodes_raw, dict):
        node_items = [(str(key), value) for key, value in nodes_raw.items() if isinstance(value, dict)]
    elif isinstance(nodes_raw, list):
        node_items = []
        for node in nodes_raw:
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            if node_id is None:
                continue
            node_items.append((str(node_id), node))
    else:
        workflow["nodes"] = {}
        return missing

    link_lookup = _build_link_lookup(workflow)
    normalized: Dict[str, Dict[str, Any]] = {}

    for node_id, node_data in node_items:
        class_type = node_data.get("class_type") or node_data.get("type")
        if class_type:
            node_data["class_type"] = class_type
        else:
            missing.append(str(node_id))

        node_info = _get_catalog_node_info(catalog, class_type) if catalog and class_type else None
        node_data["inputs"] = _convert_node_inputs(node_id, node_data, link_lookup, node_info)
        normalized[str(node_id)] = node_data

    workflow["nodes"] = normalized
    _apply_default_filename_prefix(workflow)
    return missing


def _build_link_lookup(workflow: Dict[str, Any]) -> Dict[int, tuple[str, int]]:
    mapping: Dict[int, tuple[str, int]] = {}
    links = workflow.get("links")

    if isinstance(links, list):
        for entry in links:
            if isinstance(entry, list) and len(entry) >= 3:
                try:
                    link_id = int(entry[0])
                    source_node = str(entry[1])
                    source_output = int(entry[2])
                except (TypeError, ValueError):
                    continue
                mapping[link_id] = (source_node, source_output)
            elif isinstance(entry, dict):
                candidate_id = entry.get("id")
                if candidate_id is None:
                    continue
                try:
                    link_id = int(candidate_id)
                except (TypeError, ValueError):
                    continue
                source_node = entry.get("origin_id") or entry.get("source") or entry.get("from_node")
                source_output = entry.get("origin_slot") or entry.get("source_slot") or entry.get("from_slot") or 0
                if source_node is None:
                    continue
                try:
                    mapping[link_id] = (str(source_node), int(source_output))
                except (TypeError, ValueError):
                    mapping[link_id] = (str(source_node), 0)

    return mapping


def _convert_node_inputs(
    node_id: str,
    node_data: Dict[str, Any],
    link_lookup: Dict[int, tuple[str, int]],
    node_info: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    inputs: Dict[str, Any] = {}

    existing = node_data.get("inputs")
    flat_inputs, required_fallback = _flatten_workflow_inputs(existing)
    for key, value in flat_inputs.items():
        inputs[str(key)] = value

    raw_inputs_meta = None
    if isinstance(node_info, dict):
        raw_inputs_meta = node_info.get("input") or node_info.get("inputs")

    spec_map: Dict[str, Any] = {}
    if isinstance(raw_inputs_meta, dict):
        spec_map = {name: spec for name, spec, _ in _iter_input_items(raw_inputs_meta)}

    if node_info:
        defaults = _extract_default_inputs(node_info)
        for key, value in defaults.items():
            target_key = str(key)
            if not _has_non_empty_value(inputs.get(target_key)) and _has_non_empty_value(value):
                inputs[target_key] = value

        widget_values = node_data.get("widgets_values")
        if isinstance(widget_values, list) and spec_map:
            widget_pool = list(widget_values)
            for name, spec, _ in _iter_input_items(raw_inputs_meta):
                if _is_connection_spec(spec):
                    continue
                key = str(name)
                if _has_non_empty_value(inputs.get(key)):
                    continue
                candidate = _consume_widget_value(widget_pool, spec_map.get(name))
                if _has_non_empty_value(candidate):
                    inputs[key] = candidate

        if _has_non_empty_value(required_fallback):
            for param in _collect_required_params(node_info):
                if not _has_non_empty_value(inputs.get(param)):
                    inputs[param] = required_fallback
                    required_fallback = None
                    break
    elif _has_non_empty_value(required_fallback):
        inputs.setdefault("required", required_fallback)
        required_fallback = None

    raw_inputs = node_data.get("inputs")
    if isinstance(raw_inputs, list):
        for entry in raw_inputs:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not name:
                continue
            key = str(name)

            link_ids: list[int] = []
            if "link" in entry and entry["link"] is not None:
                try:
                    link_ids.append(int(entry["link"]))
                except (TypeError, ValueError):
                    pass
            links_field = entry.get("links")
            if isinstance(links_field, list):
                for candidate in links_field:
                    try:
                        link_ids.append(int(candidate))
                    except (TypeError, ValueError):
                        continue

            if link_ids:
                for link_id in link_ids:
                    source = link_lookup.get(link_id)
                    if not source:
                        continue
                    connection = [str(source[0]), source[1]]
                    if node_info and _is_multi_connection_input(node_info, key):
                        current = inputs.setdefault(key, [])
                        if isinstance(current, list) and connection not in current:
                            current.append(connection)
                        elif not isinstance(current, list):
                            inputs[key] = [connection]
                    else:
                        inputs[key] = connection
            elif "value" in entry:
                inputs[key] = entry.get("value")

    return inputs


def _is_multi_connection_input(node_info: Dict[str, Any], input_name: str) -> bool:
    raw_inputs = node_info.get("input") or node_info.get("inputs")
    for name, spec, _ in _iter_input_items(raw_inputs):
        if name != input_name:
            continue
        return _is_multi_connection_spec(spec)
    return False


def build_prompt_payload(workflow: Dict[str, Any]) -> Dict[str, Any]:
    nodes_raw = workflow.get("nodes")
    if isinstance(nodes_raw, dict):
        node_items = nodes_raw.items()
    elif isinstance(nodes_raw, list):
        node_items = ((str(node.get("id")), node) for node in nodes_raw if isinstance(node, dict))
    else:
        return {}

    prompt: Dict[str, Dict[str, Any]] = {}
    for node_id, node_data in node_items:
        if not isinstance(node_data, dict):
            continue
        class_type = node_data.get("class_type") or node_data.get("type")
        if not class_type:
            raise ValueError(f"Нода #{node_id} не содержит class_type")

        lowered_class = str(class_type).lower()
        if class_type in _UI_ONLY_NODE_TYPES or lowered_class.endswith("note"):
            continue

        inputs: Dict[str, Any] = {}
        raw_inputs = node_data.get("inputs")
        if isinstance(raw_inputs, dict):
            for name, value in raw_inputs.items():
                converted = _coerce_prompt_value(value)
                if converted is None or converted == "":
                    continue
                inputs[str(name)] = converted

        if isinstance(class_type, str) and class_type in SAVE_OUTPUT_NODE_TYPES:
            inputs["filename_prefix"] = DEFAULT_FILENAME_PREFIX

        prompt_node: Dict[str, Any] = {"class_type": class_type, "inputs": inputs}

        if "widgets_values" in node_data:
            prompt_node["widgets_values"] = node_data["widgets_values"]

        prompt[str(node_id)] = prompt_node

    return prompt


def _coerce_prompt_value(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_coerce_prompt_value(item) for item in value]
    if isinstance(value, list):
        return [_coerce_prompt_value(item) for item in value]
    return value


def _is_seed_parameter(name: str) -> bool:
    lowered = name.lower()
    if lowered == "seed":
        return True
    if lowered.endswith("_seed"):
        return True
    if lowered.startswith("seed") and lowered[4:].isdigit():
        return True
    return False


def _generate_random_seed() -> int:
    return secrets.randbelow(MAX_COMFY_SEED_VALUE + 1)


def _maybe_randomize_seed(name: str, value: Any) -> tuple[Any, Optional[Any]]:
    if isinstance(value, list):
        updated: list[Any] = []
        changed = False
        for item in value:
            new_item, recorded = _maybe_randomize_seed(name, item)
            updated.append(new_item)
            if recorded is not None:
                changed = True
        return (updated, updated if changed else None)

    if not _is_seed_parameter(name):
        return value, None

    if isinstance(value, int) and value < 0:
        new_value = _generate_random_seed()
        return new_value, new_value

    if isinstance(value, str) and value.strip().lower() == "random":
        new_value = _generate_random_seed()
        return new_value, new_value

    return value, None


def _randomize_seed_inputs(prompt: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    overrides: Dict[str, Dict[str, Any]] = {}
    for node_id, node_data in prompt.items():
        inputs = node_data.get("inputs") if isinstance(node_data, dict) else None
        if not isinstance(inputs, dict):
            continue

        node_overrides: Dict[str, Any] = {}
        for key, raw_value in list(inputs.items()):
            new_value, recorded = _maybe_randomize_seed(key, raw_value)
            inputs[key] = new_value
            if recorded is not None:
                node_overrides[key] = recorded

        if node_overrides:
            overrides[str(node_id)] = node_overrides

    return overrides


def _format_seed_overrides(overrides: Dict[str, Dict[str, Any]], *, limit: int = 5) -> str:
    lines: list[str] = []
    total = 0
    for node_id, params in overrides.items():
        if not isinstance(params, dict):
            continue
        for param_name, value in params.items():
            total += 1
            if len(lines) < limit:
                if isinstance(value, list):
                    truncated = ", ".join(str(item) for item in value[:4])
                    if len(value) > 4:
                        truncated += ", …"
                    display = truncated
                else:
                    display = str(value)
                lines.append(f"• #{node_id} {param_name} → {display}")
    if total > limit:
        lines.append(f"• … и ещё {total - limit}")
    return "\n".join(lines)


def validate_workflow(workflow: Dict[str, Any], catalog: Dict[str, Any]) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    if not workflow.get("nodes"):
        errors.append("Workflow не содержит нод")
        return errors, warnings

    catalog_nodes_raw = catalog.get("nodes")
    catalog_nodes: Dict[str, Any] = catalog_nodes_raw if isinstance(catalog_nodes_raw, dict) else {}

    if not catalog_nodes:
        errors.append(
            "Каталог нод от ComfyUI пуст. Обновите подключение или попробуйте повторить попытку позже."
        )
        return errors, warnings

    for node_id, node_data in _iter_workflow_nodes(workflow):
        if not isinstance(node_data, dict):
            errors.append(f"Нода #{node_id}: некорректная структура")
            continue

        class_type = node_data.get("class_type") or node_data.get("type")
        if not class_type:
            errors.append(f"Нода #{node_id}: отсутствует class_type")
            continue

        node_info = catalog_nodes.get(class_type)
        if not isinstance(node_info, dict):
            lowered = str(class_type).lower()
            if class_type in _UI_ONLY_NODE_TYPES or lowered.endswith("note"):
                continue
            errors.append(
                f"Нода #{node_id}: тип '{class_type}' отсутствует в установленном ComfyUI."
            )
            continue

        inputs_raw = node_data.get("inputs")
        flat_inputs, required_fallback = _flatten_workflow_inputs(inputs_raw)
        raw_widget_values = node_data.get("widgets_values")
        widget_pool = list(raw_widget_values) if isinstance(raw_widget_values, list) else []
        spec_map = {name: spec for name, spec, _ in _iter_input_items(node_info.get("input") or {})}

        for param in _collect_required_params(node_info):
            value = flat_inputs.get(param)
            if (value is None or (isinstance(value, str) and value.strip() == "")) and required_fallback is not None:
                value = required_fallback
                required_fallback = None
            if value is None or (isinstance(value, str) and value.strip() == ""):
                candidate = _consume_widget_value(widget_pool, spec_map.get(param))
                if candidate is not None:
                    value = candidate
            if value is None or (isinstance(value, str) and value.strip() == ""):
                errors.append(f"Нода #{node_id}: заполните параметр '{param}'")

        for link in _collect_required_links(node_info):
            value = flat_inputs.get(link)
            if not _is_connection_filled(value):
                errors.append(f"Нода #{node_id}: подключите источник для '{link}'")

    return errors, warnings


def _iter_workflow_nodes(workflow: Dict[str, Any]) -> list[tuple[str, Any]]:
    nodes = workflow.get("nodes")
    result: list[tuple[str, Any]] = []
    if isinstance(nodes, dict):
        for key, value in nodes.items():
            result.append((str(key), value))
    elif isinstance(nodes, list):
        for item in nodes:
            if isinstance(item, dict):
                node_id = item.get("id")
                if node_id is not None:
                    result.append((str(node_id), item))
    return result


def _has_non_empty_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _is_connection_filled(value: Any) -> bool:
    if value in (None, ""):
        return False
    if isinstance(value, (list, tuple)):
        if not value:
            return False
        # Single connection represented as list/tuple
        if len(value) >= 2 and not isinstance(value[0], (list, tuple)):
            return value[0] not in (None, "")
        # Multiple connections
        for item in value:
            if isinstance(item, (list, tuple)) and len(item) >= 2 and item[0] not in (None, ""):
                return True
        return False
    return True


async def send_main_menu(update: MessageSource, context: ContextTypes.DEFAULT_TYPE, user_id: int, *, edit: bool = False) -> None:
    text = (
        "<b>🤖 ComfyUI Telegram Bot</b>\n\n"
        "Используйте кнопки ниже, чтобы управлять своими workflow."
    )
    _clear_dynamic_buttons(context)
    await _ensure_keyboard_mode(update, context, user_id, "menu")
    await respond(update, text, _menu_reply_keyboard(context, user_id), parse_mode=ParseMode.HTML)


def _menu_reply_keyboard(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> ReplyKeyboardMarkup:
    resources = require_resources(context)
    has_workflow = any(resources.storage.list_workflows(user_id))

    visible_actions: list[str] = []
    for action in MAIN_MENU_ACTIONS:
        if action == MENU_OPEN and not has_workflow:
            continue
        visible_actions.append(action)

    rows: list[list[str]] = []
    current_row: list[str] = []
    for action in visible_actions:
        label = MENU_DISPLAY_TEXT.get(action, action)
        current_row.append(label)
        if len(current_row) == 3:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)

    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _menu_action_from_text(text: str | None) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    return MENU_TEXT_TO_ACTION.get(cleaned)


async def _dispatch_menu_action(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
    *,
    via_callback: bool = False,
) -> bool:
    if action == MENU_BACK:
        user_id = get_user_id_from_source(message_source)
        await send_main_menu(message_source, context, user_id)
        return True

    if action == MENU_INSTRUCTION:
        user_id = get_user_id_from_source(message_source)
        _clear_dynamic_buttons(context)
        await _ensure_keyboard_mode(message_source, context, user_id, "menu")
        await respond(
            message_source,
            INSTRUCTION_TEXT,
            _menu_reply_keyboard(context, user_id),
            parse_mode=ParseMode.HTML,
        )
        return True

    if action == MENU_CREATE:
        await create_workflow(message_source, context)
        return True

    if action == MENU_OPEN:
        await show_workflow_overview(message_source, context)
        return True

    if action == MENU_WORKFLOWS:
        await show_workflow_library(message_source, context)
        return True

    if action == MENU_IMPORT:
        await begin_import(message_source, context)
        return True

    if action == MENU_STATUS:
        await show_status(message_source, context)
        return True

    if action == MENU_GALLERY:
        await show_gallery(message_source, context)
        return True

    if action == MENU_TEMPLATES:
        await show_template_categories(message_source, context)
        return True

    if action == MENU_NOTIFICATIONS:
        await show_notification_settings(message_source, context)
        return True

    if action == MENU_HISTORY:
        await show_history(message_source, context)
        return True

    if action == MENU_RESTART:
        await restart_comfyui(message_source, context)
        return True

    if action == QUEUE_STATUS:
        await show_queue(message_source, context)
        return True

    return False


def _workflow_action_from_text(text: str | None) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    return WORKFLOW_TEXT_TO_ACTION.get(cleaned)


def _queue_action_from_text(text: str | None) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    return QUEUE_TEXT_TO_ACTION.get(cleaned)


def _history_action_from_text(text: str | None) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    return HISTORY_TEXT_TO_ACTION.get(cleaned)


def _status_action_from_text(text: str | None) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    return STATUS_TEXT_TO_ACTION.get(cleaned)


def _parse_workflow_node_selection(text: str | None) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    if not cleaned.startswith("Нода #"):
        return None
    candidate = cleaned[len("Нода #") :].strip()
    if not candidate:
        return None
    return candidate


async def _dispatch_workflow_action(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> bool:
    if action == WORKFLOW_ADD_NODE:
        user_data = get_user_data(context)
        page = user_data.get("catalog_last_page", 0)
        await show_node_categories(message_source, context, page=page)
        return True

    if action == WORKFLOW_LAUNCH:
        await launch_workflow(message_source, context)
        return True

    if action == WORKFLOW_EXPORT:
        await export_current_workflow(message_source, context)
        return True

    if action == MENU_BACK:
        return await _dispatch_menu_action(message_source, context, MENU_BACK, via_callback=isinstance(message_source, CallbackQuery))

    return False


async def _dispatch_queue_action(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> bool:
    if action in {QUEUE_STATUS, QUEUE_REFRESH}:
        await show_queue(message_source, context)
        return True

    if action == QUEUE_INTERRUPT:
        await interrupt_queue(message_source, context)
        return True

    if action == QUEUE_CLEAR:
        await clear_queue(message_source, context)
        return True

    if action == MENU_BACK:
        return await _dispatch_menu_action(message_source, context, MENU_BACK, via_callback=isinstance(message_source, CallbackQuery))

    return False


async def _dispatch_history_action(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> bool:
    if action == MENU_HISTORY:
        await show_history(message_source, context)
        return True

    if action == MENU_BACK:
        return await _dispatch_menu_action(message_source, context, MENU_BACK, via_callback=isinstance(message_source, CallbackQuery))

    return False


async def _dispatch_status_action(
    message_source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    action: str,
) -> bool:
    if action == MENU_STATUS:
        await show_status(message_source, context)
        return True

    if action == MENU_BACK:
        return await _dispatch_menu_action(message_source, context, MENU_BACK, via_callback=isinstance(message_source, CallbackQuery))

    return False


def _workflow_reply_keyboard(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> ReplyKeyboardMarkup:
    user_data = get_user_data(context)
    workflow = user_data.get("workflow")
    rows: list[list[str]] = []

    action_row: list[str] = [
        WORKFLOW_DISPLAY_TEXT[WORKFLOW_LAUNCH],
        WORKFLOW_DISPLAY_TEXT[WORKFLOW_ADD_NODE],
        WORKFLOW_DISPLAY_TEXT[WORKFLOW_EXPORT],
    ]
    rows.append(action_row)
    rows.append([WORKFLOW_DISPLAY_TEXT[MENU_BACK]])

    if isinstance(workflow, dict):
        node_ids = get_node_ids(workflow)
        try:
            node_ids.sort(key=lambda value: (0, int(value)) if str(value).isdigit() else (1, str(value)))
        except Exception:
            node_ids.sort()

        current_row: list[str] = []
        for node_id in node_ids:
            current_row.append(f"Нода #{node_id}")
            if len(current_row) == 3:
                rows.append(current_row)
                current_row = []
        if current_row:
            rows.append(current_row)

    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _workflow_keyboard(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    *,
    edit: bool = False,
    prefer_inline: bool = False,
) -> ReplyMarkupType:
    return _workflow_reply_keyboard(context, user_id)


def _workflow_markup_for_source(
    context: ContextTypes.DEFAULT_TYPE,
    source: MessageSource,
    user_id: int,
    *,
    prefer_inline: bool = False,
) -> ReplyMarkupType:
    return _workflow_keyboard(context, user_id)


async def _ensure_keyboard_mode(
    source: MessageSource,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    mode: str,
    *,
    ensure_message: bool = False,
    force_send: bool = False,
) -> None:
    user_data = get_user_data(context)
    previous = user_data.get("keyboard_mode")
    user_data["keyboard_mode"] = mode

    if not ensure_message and not force_send:
        return
    if not force_send and previous == mode:
        return

    if mode == "menu":
        markup = _menu_reply_keyboard(context, user_id)
    elif mode == "workflow":
        markup = _workflow_reply_keyboard(context, user_id)
    elif mode == "status":
        markup = _status_reply_keyboard()
    elif mode == "history":
        markup = _history_reply_keyboard()
    elif mode == "queue":
        markup = _queue_reply_keyboard()
    elif mode == "progress_active":
        markup = _progress_reply_keyboard(context, active=True)
    elif mode == "progress_idle":
        markup = _progress_reply_keyboard(context, active=False)
    else:
        markup = ReplyKeyboardRemove()

    _clear_dynamic_buttons(context)
    await respond(source, KEYBOARD_UPDATED_TEXT, markup, parse_mode=ParseMode.HTML)


def _shorten(value: Any) -> str:
    text = str(value)
    return text if len(text) <= 16 else text[:13] + "…"


def _progress_reply_keyboard(context: ContextTypes.DEFAULT_TYPE, *, active: bool = True) -> ReplyKeyboardMarkup:
    mapping: dict[str, ButtonAction] = {}
    rows: list[list[str]]

    if active:
        stop_label = "⛔️ Остановить"
        rows = [[stop_label]]
        mapping[stop_label] = ("run_cancel",)
    else:
        launch_label = WORKFLOW_DISPLAY_TEXT[WORKFLOW_LAUNCH]
        rows = [[launch_label]]
        mapping[launch_label] = ("run_launch",)

    back_label = WORKFLOW_DISPLAY_TEXT[MENU_BACK]
    rows.append([back_label])
    mapping[back_label] = ("workflow_overview",)

    _set_dynamic_buttons(context, mapping)
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def format_queue_state(state: Dict[str, Any]) -> str:
    queue_block = state.get("queue") if isinstance(state.get("queue"), dict) else {}
    pending = _normalize_jobs(queue_block.get("pending") if isinstance(queue_block, dict) else None)
    waiting = _normalize_jobs(queue_block.get("queue") if isinstance(queue_block, dict) else None)
    finished = _normalize_jobs(queue_block.get("finished") if isinstance(queue_block, dict) else None)

    if not pending:
        pending = _normalize_jobs(state.get("pending"))
    if not waiting:
        waiting = _normalize_jobs(state.get("queue"))
    if not finished:
        finished = _normalize_jobs(state.get("finished"))

    lines = ["<b>🗂 Очередь ComfyUI</b>"]
    lines.append(f"• Выполняется: {len(pending)}")
    lines.extend(_summarize_jobs(pending))

    lines.append(f"• В очереди: {len(waiting)}")
    lines.extend(_summarize_jobs(waiting))

    if finished:
        lines.append(f"• Недавние завершённые: {min(len(finished), 5)}")
        lines.extend(_summarize_jobs(finished[:5]))

    if not pending and not waiting:
        lines.append("<i>Очередь пуста.</i>")

    return "\n".join(lines)


def _normalize_jobs(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if raw is None or raw is False:
        return []
    return [raw]


    lines: list[str] = []
    for job in jobs[:limit]:
        description = escape(_describe_job(job))
        lines.append(f"  └ <code>{description}</code>")
    remaining = len(jobs) - limit
    if remaining > 0:
        lines.append(f"  └ … и ещё {remaining}")
    return lines


def _describe_job(job: Any) -> str:
    if isinstance(job, dict):
        prompt_id = job.get("prompt_id") or job.get("id") or job.get("queue_id")
        node = job.get("node") or job.get("node_id") or job.get("class_type")
        status = job.get("status") or job.get("state")
        label_parts: list[str] = []
        if prompt_id:
            text = str(prompt_id)
            label_parts.append(text if len(text) <= 10 else text[:7] + "…")
        if node:
            label_parts.append(str(node))
        if status:
            label_parts.append(str(status))
        if label_parts:
            return " | ".join(label_parts)

        name = job.get("name") or job.get("type")
        if name:
            return str(name)

        if "workflow" in job and isinstance(job["workflow"], dict):
            return "workflow"

    return str(job)
def _coerce_message(result: Message | bool, fallback: Optional[Message]) -> Message:
    if isinstance(result, Message):
        return result
    if fallback is not None:
        return fallback
    raise RuntimeError("Не удалось получить сообщение после операции редактирования")


async def _remove_reply_keyboard(target: MessageSource) -> None:
    message: Optional[Message] = None
    if isinstance(target, CallbackQuery):
        if isinstance(target.message, Message):
            message = target.message
    elif isinstance(target, Update):
        if isinstance(target.effective_message, Message):
            message = target.effective_message
    elif isinstance(target, Message):
        message = target

    if not message:
        return

    try:
        removal_message = await message.reply_text(
            "Клавиатура обновлена",
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception:  # pragma: no cover - best effort cleanup
        LOGGER.debug("Failed to send keyboard removal message", exc_info=True)
        return

    try:
        await removal_message.delete()
    except Exception:  # pragma: no cover - message may already be gone
        LOGGER.debug("Failed to delete keyboard removal message", exc_info=True)


ReplyMarkupType = Union[InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove]


async def respond(
    target: MessageSource,
    text: str,
    reply_markup: Optional[ReplyMarkupType] = None,
    *,
    parse_mode: Optional[str] = None,
    edit: bool = False,
) -> Message:
    markup_for_edit: Optional[InlineKeyboardMarkup]
    if isinstance(reply_markup, InlineKeyboardMarkup) or reply_markup is None:
        markup_for_edit = reply_markup
    else:
        markup_for_edit = None

    if isinstance(target, CallbackQuery):
        if not isinstance(target.message, Message):
            raise RuntimeError("CallbackQuery does not carry an accessible message")
        target = target.message

    if isinstance(target, Update):
        message = target.effective_message
        if not isinstance(message, Message):
            raise RuntimeError("No effective message to respond to")

        if edit:
            try:
                result = await message.edit_text(text, parse_mode=parse_mode, reply_markup=markup_for_edit)
            except BadRequest as exc:
                if _is_message_not_modified_error(exc):
                    return message
                raise
            return _coerce_message(result, message)
        return await message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)

    if isinstance(target, Message):
        if edit:
            try:
                result = await target.edit_text(text, parse_mode=parse_mode, reply_markup=markup_for_edit)
            except BadRequest as exc:
                if _is_message_not_modified_error(exc):
                    return target
                raise
            return _coerce_message(result, target)
        return await target.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)

    raise RuntimeError("Unsupported target type")


async def edit_message(message: Message, text: str, *, reply_markup: Optional[InlineKeyboardMarkup] = None) -> Optional[Message]:
    try:
        result = await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        if isinstance(result, Message):
            return result
        return message
    except BadRequest as exc:
        if _is_message_not_modified_error(exc):
            return message
        LOGGER.debug("Failed to edit message", exc_info=True)
        return None
    except Exception:  # pragma: no cover - fallback when message already changed
        LOGGER.debug("Failed to edit message", exc_info=True)
        return None


def get_node(workflow: Dict[str, Any], node_id: str) -> Optional[Dict[str, Any]]:
    nodes = workflow.get("nodes")
    if isinstance(nodes, dict):
        return nodes.get(node_id) or nodes.get(int(node_id))
    if isinstance(nodes, list):
        for node in nodes:
            if str(node.get("id")) == str(node_id):
                return node
    return None


def get_node_ids(workflow: Dict[str, Any]) -> list[str]:
    nodes = workflow.get("nodes")
    if isinstance(nodes, dict):
        return [str(k) for k in nodes.keys()]
    if isinstance(nodes, list):
        return [str(node.get("id")) for node in nodes]
    return []


def require_resources(context: ContextTypes.DEFAULT_TYPE) -> BotResources:
    stored = context.application.bot_data.get("resources")
    if isinstance(stored, BotResources):
        return stored

    LOGGER.warning("Bot resources not pre-initialized; creating on-demand instance")
    resources = _build_resources()
    context.application.bot_data["resources"] = resources

    return resources


def _build_resources() -> BotResources:
    config = load_config()
    storage = WorkflowStorage(config.data_dir / "workflows")
    client = ComfyUIClient(
        config.comfyui_http_url,
        config.comfyui_ws_url,
        templates_dir=config.workflow_templates_dir,
    )
    return BotResources(config=config, storage=storage, client=client)


def get_user_id_from_source(source: Message | Update | CallbackQuery) -> int:
    if isinstance(source, Update):
        return get_user_id(source)
    if isinstance(source, CallbackQuery):
        if source.from_user is None:
            raise RuntimeError("Callback without user")
        return source.from_user.id
    if source.from_user is None:
        raise RuntimeError("Message without user")
    return source.from_user.id


def get_chat_id_from_source(source: MessageSource) -> int:
    if isinstance(source, CallbackQuery):
        if isinstance(source.message, Message):
            return source.message.chat_id
        raise RuntimeError("Callback does not carry a message")
    if isinstance(source, Message):
        return source.chat_id
    if isinstance(source, Update) and source.effective_chat is not None:
        return source.effective_chat.id
    raise RuntimeError("Cannot resolve chat id from source")


async def _flush_persistence(context: ContextTypes.DEFAULT_TYPE) -> None:
    application = getattr(context, "application", None)
    if application is None:
        return
    persistence = getattr(application, "persistence", None)
    if persistence is None:
        return

    removed_bot_entries: dict[str, Any] = {}
    original_bot_data = getattr(application, "bot_data", None)
    restorable_bot_data: Optional[Dict[str, Any]] = None
    if isinstance(original_bot_data, dict):
        restorable_bot_data = original_bot_data
        for key in ("resources",):
            if key in original_bot_data:
                removed_bot_entries[key] = original_bot_data.pop(key)

    async def _restore_bot_data() -> None:
        if restorable_bot_data is not None and removed_bot_entries:
            restorable_bot_data.update(removed_bot_entries)

    try:
        result = application.update_persistence()
    except Exception:  # pragma: no cover - persistence layer should not break bot flow
        LOGGER.warning("Failed to trigger persistence update", exc_info=True)
        await _restore_bot_data()
        return

    if inspect.isawaitable(result):
        try:
            await result
        except Exception:  # pragma: no cover - best effort flush
            LOGGER.warning("Failed to flush persistence", exc_info=True)
        finally:
            await _restore_bot_data()
    else:
        await _restore_bot_data()


def build_application(config: BotConfig, resources: BotResources) -> Application:
    persistence = PicklePersistence(filepath=str(config.persistence_path))

    async def _shutdown(_: Application) -> None:
        await resources.shutdown()

    application = (
        Application.builder()
        .token(config.bot_token)
        .persistence(persistence)
        .post_shutdown(_shutdown)
        .build()
    )

    application.bot_data["resources"] = resources

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_menu_callback))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    return application


def _configure_logging(config: BotConfig) -> None:
    log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    logging.basicConfig(level=log_level, handlers=[console_handler], format="%(asctime)s %(levelname)s %(name)s %(message)s")

    log_file = os.getenv("LOG_FILE")
    if not log_file:
        log_file = str(config.data_dir / "bot.log")

    try:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(file_path, mode="w", encoding="utf-8")
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)
        logging.getLogger(__name__).info("Файловый лог: %s", file_path)
    except Exception:  # pragma: no cover - логирование не должно ломать бот
        logging.getLogger(__name__).warning("Не удалось настроить файловый лог", exc_info=True)


def main() -> None:
    config = load_config()
    _configure_logging(config)
    storage = WorkflowStorage(config.data_dir / "workflows")
    client = ComfyUIClient(config.comfyui_http_url, config.comfyui_ws_url)
    resources = BotResources(config=config, storage=storage, client=client)

    application = build_application(config, resources)

    LOGGER.info("Starting bot")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

import asyncio
import base64
import binascii
import json
import logging
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urlparse, urlunparse

import aiohttp
from aiohttp import ClientError
import websockets
from websockets import WebSocketClientProtocol

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class PreviewPayload:
    image: bytes
    mime_type: str


@dataclass(slots=True, frozen=True)
class ProgressEvent:
    prompt_id: str
    node_id: Optional[str]
    value: float
    maximum: float
    preview: Optional[PreviewPayload] = None


@dataclass(slots=True, frozen=True)
class ExecutionResult:
    prompt_id: str
    data: Dict


class ComfyUIClient:
    """Async client for ComfyUI REST and WebSocket APIs."""

    _MODEL_EXTENSIONS: tuple[str, ...] = (".safetensors", ".ckpt", ".pth", ".gguf")

    def __init__(
        self,
        base_http_url: str,
        ws_url: str,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        templates_dir: Optional[Path] = None,
    ) -> None:
        self._configured_http_url = base_http_url.rstrip("/")
        self._configured_ws_url = ws_url
        self._base_http_url = self._configured_http_url
        self._ws_url = ws_url
        self._session = session
        self._own_session = session is None
        self._object_info_cache: Optional[Dict] = None
        self._template_cache: Optional[List[Dict[str, Any]]] = None
        self._model_cache: Dict[str, List[str]] = {}
        self._endpoint_ready = False
        self._endpoint_lock = asyncio.Lock()
        self._templates_dir = templates_dir

        parsed_http = urlparse(self._configured_http_url)
        self._auto_host = parsed_http.hostname or "127.0.0.1"
        self._auto_scheme = parsed_http.scheme or "http"
        self._auto_path = parsed_http.path.rstrip("/")
        self._auto_port = parsed_http.port

    def _ensure_state_defaults(self) -> None:
        if not hasattr(self, "_configured_http_url"):
            self._configured_http_url = getattr(self, "_base_http_url", "http://127.0.0.1:8000")
        if not hasattr(self, "_configured_ws_url"):
            self._configured_ws_url = getattr(self, "_ws_url", "ws://127.0.0.1:8000/ws")
        if not hasattr(self, "_endpoint_ready"):
            self._endpoint_ready = False
        if not hasattr(self, "_endpoint_lock"):
            self._endpoint_lock = asyncio.Lock()
        if not hasattr(self, "_template_cache"):
            self._template_cache = None
        if not hasattr(self, "_model_cache"):
            self._model_cache = {}
        if not hasattr(self, "_templates_dir"):
            self._templates_dir = None

        # Populate derived network hints when loading legacy instances from persistence.
        parsed_http = urlparse(getattr(self, "_configured_http_url"))
        if not hasattr(self, "_auto_host"):
            self._auto_host = parsed_http.hostname or "127.0.0.1"
        if not hasattr(self, "_auto_scheme"):
            self._auto_scheme = parsed_http.scheme or "http"
        if not hasattr(self, "_auto_path"):
            self._auto_path = parsed_http.path.rstrip("/")
        if not hasattr(self, "_auto_port"):
            self._auto_port = parsed_http.port

    @staticmethod
    def _coerce_model_names(value: Any) -> List[str]:
        names: List[str] = []
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    names.append(item)
                elif isinstance(item, dict):
                    name = item.get("name") or item.get("title") or item.get("id") or item.get("filename")
                    if isinstance(name, str):
                        names.append(name)
        elif isinstance(value, dict):
            # Some endpoints may return mapping {"model_name": {...}}
            for key in value.keys():
                if isinstance(key, str):
                    names.append(key)
        elif isinstance(value, str):
            names.append(value)
        return names

    @classmethod
    def _filter_model_names(cls, names: Iterable[str]) -> List[str]:
        filtered: List[str] = []
        seen: set[str] = set()
        for name in names:
            if not isinstance(name, str):
                continue
            trimmed = name.strip()
            if not trimmed:
                continue
            filename = trimmed.replace("\\", "/").split("/")[-1]
            lowered = filename.lower()
            if not any(lowered.endswith(ext) for ext in cls._MODEL_EXTENSIONS):
                continue
            if lowered in seen:
                continue
            seen.add(lowered)
            filtered.append(trimmed)
        filtered.sort(key=lambda value: value.lower())
        return filtered

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=60)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._own_session and self._session is not None:
            await self._session.close()

    async def get_object_info(self, refresh: bool = False) -> Dict:
        if not refresh and self._object_info_cache is not None:
            return self._object_info_cache

        async with self._request("GET", "/object_info") as resp:
            resp.raise_for_status()
            data = await resp.json()
            self._object_info_cache = data
            return data

    async def get_system_stats(self) -> Dict:
        async with self._request("GET", "/system_stats") as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_templates(self, refresh: bool = False) -> List[Dict[str, Any]]:
        if not refresh and self._template_cache is not None:
            return self._template_cache

        collected: Dict[str, Dict[str, Any]] = {}

        def _merge(items: Iterable[Dict[str, Any]], source: str) -> None:
            for item in items:
                if not isinstance(item, dict):
                    continue
                template_id = item.get("id") or item.get("name")
                if not template_id:
                    continue
                key = str(template_id)
                if key in collected:
                    continue
                item.setdefault("_source", source)
                collected[key] = item

        try:
            _merge(await self._fetch_templates_from_route("/templates"), "legacy")
        except (PermissionError, FileNotFoundError):
            LOGGER.debug("/templates endpoint unavailable for templates", exc_info=True)
        except RuntimeError:
            LOGGER.debug("/templates endpoint returned non-critical error", exc_info=True)

        try:
            _merge(await self._fetch_templates_from_api(), "api")
        except FileNotFoundError:
            LOGGER.debug("/api/workflow_templates endpoint missing", exc_info=True)
        except RuntimeError:
            LOGGER.debug("/api/workflow_templates endpoint returned non-critical error", exc_info=True)

        try:
            _merge(self._load_templates_from_disk(), "disk")
        except Exception:  # pragma: no cover - filesystem optional
            LOGGER.debug("Failed to load templates from disk", exc_info=True)

        templates = list(collected.values())
        templates.sort(key=lambda item: str(item.get("name", item.get("id", ""))).lower())
        self._template_cache = templates
        return templates

    async def get_template(self, template_ref: Any) -> Dict[str, Any]:
        if isinstance(template_ref, dict):
            route = template_ref.get("route") or template_ref.get("_route")
            if route == "api/workflow_templates":
                namespace = template_ref.get("namespace")
                name = template_ref.get("name")
                if not namespace or not name:
                    raise RuntimeError("Недостаточно данных для загрузки шаблона")
                return await self._fetch_template_from_api(namespace, name)
            if route == "disk":
                disk_path = template_ref.get("path") or template_ref.get("relative")
                if not disk_path:
                    raise RuntimeError("Не указан путь к шаблону на диске")
                return self._load_template_from_disk_file(str(disk_path))

            template_id = template_ref.get("id") or template_ref.get("path")
            if template_id:
                return await self.get_template(str(template_id))
            raise RuntimeError("Неизвестный формат ссылки на шаблон")

        template_id = str(template_ref)

        try:
            return await self._fetch_template_from_route("/templates", template_id)
        except (PermissionError, FileNotFoundError):
            if template_id.startswith("disk::"):
                return self._load_template_from_disk_file(template_id[len("disk::") :])
            if "/" not in template_id:
                raise
            namespace, name = template_id.split("/", 1)
            return await self._fetch_template_from_api(namespace, name)

    async def _fetch_templates_from_route(self, path: str) -> List[Dict[str, Any]]:
        async with self._request("GET", path) as resp:
            status = resp.status
            reason = resp.reason or ""
            text = await resp.text()

        if status in (401, 403):
            snippet = text.strip()
            extra = f": {snippet.splitlines()[0][:160]}" if snippet else ""
            raise PermissionError(
                f"ComfyUI вернул {status} Forbidden для {path}{extra}. Проверьте настройки доступа."
            )

        if status == 404:
            raise FileNotFoundError(f"ComfyUI не поддерживает endpoint {path}")

        if status >= 400:
            detail = text.strip() or reason or str(status)
            raise RuntimeError(f"HTTP {status} при запросе шаблонов ({path}): {detail}")

        try:
            data = json.loads(text or "[]")
        except json.JSONDecodeError as exc:  # pragma: no cover
            raise RuntimeError("ComfyUI вернул некорректный JSON при запросе шаблонов") from exc

        if isinstance(data, dict) and isinstance(data.get("templates"), list):
            return [item for item in data["templates"] if isinstance(item, dict)]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        raise RuntimeError("ComfyUI не вернул список шаблонов")

    async def _fetch_templates_from_api(self) -> List[Dict[str, Any]]:
        async with self._request("GET", "/api/workflow_templates") as resp:
            status = resp.status
            reason = resp.reason or ""
            text = await resp.text()

        if status == 404:
            raise FileNotFoundError("ComfyUI не поддерживает endpoint /api/workflow_templates")

        if status >= 400:
            detail = text.strip() or reason or str(status)
            raise RuntimeError(f"HTTP {status} при запросе шаблонов (api/workflow_templates): {detail}")

        try:
            payload = json.loads(text or "{}")
        except json.JSONDecodeError as exc:  # pragma: no cover
            raise RuntimeError("ComfyUI вернул некорректный JSON при запросе шаблонов (api/workflow_templates)") from exc

        templates: list[Dict[str, Any]] = []
        if isinstance(payload, dict):
            for namespace, items in payload.items():
                if not isinstance(namespace, str) or not isinstance(items, list):
                    continue
                for name in items:
                    if not isinstance(name, str):
                        continue
                    templates.append(
                        {
                            "id": f"{namespace}/{name}",
                            "source_id": f"{namespace}/{name}",
                            "name": name,
                            "category": namespace,
                            "group": namespace,
                            "_source_info": {
                                "route": "api/workflow_templates",
                                "namespace": namespace,
                                "name": name,
                            },
                        }
                    )

        if not templates:
            raise RuntimeError("ComfyUI не вернул шаблоны из /api/workflow_templates")

        return templates

    def _load_templates_from_disk(self) -> List[Dict[str, Any]]:
        if self._templates_dir is None:
            return []

        base = Path(self._templates_dir)
        if not base.exists():
            LOGGER.debug("Configured workflow templates dir %s does not exist", base)
            return []

        templates: list[Dict[str, Any]] = []
        for path in sorted(base.rglob("*.json")):
            if not path.is_file():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                LOGGER.debug("Failed to read workflow template %s", path, exc_info=True)
                continue

            relative = path.relative_to(base)
            category = relative.parts[0] if len(relative.parts) > 1 else "builtin"
            templates.append(
                {
                    "id": f"disk::{relative.as_posix()}",
                    "source_id": f"disk::{relative.as_posix()}",
                    "name": path.stem,
                    "category": category,
                    "group": category,
                    "workflow": payload if isinstance(payload, dict) else None,
                    "_source_info": {
                        "route": "disk",
                        "path": str(path),
                        "relative": relative.as_posix(),
                    },
                }
            )

        return templates

    async def _fetch_template_from_route(self, base_path: str, template_id: str) -> Dict[str, Any]:
        safe_template_id = template_id.strip("/")
        path = f"{base_path.rstrip('/')}/{safe_template_id}"
        async with self._request("GET", path) as resp:
            status = resp.status
            reason = resp.reason or ""
            text = await resp.text()

        if status in (401, 403):
            snippet = text.strip()
            extra = f": {snippet.splitlines()[0][:160]}" if snippet else ""
            raise PermissionError(
                f"ComfyUI вернул {status} Forbidden для {path}{extra}. Проверьте настройки доступа."
            )

        if status == 404:
            raise FileNotFoundError(f"ComfyUI не нашёл шаблон {safe_template_id}")

        if status >= 400:
            detail = text.strip() or reason or str(status)
            raise RuntimeError(f"HTTP {status} при загрузке шаблона ({path}): {detail}")

        try:
            data = json.loads(text or "{}")
        except json.JSONDecodeError as exc:  # pragma: no cover
            raise RuntimeError("ComfyUI вернул некорректный JSON при загрузке шаблона") from exc

        if not isinstance(data, dict):
            raise RuntimeError("ComfyUI не вернул данные шаблона")
        return data

    async def _fetch_template_from_api(self, namespace: str, name: str) -> Dict[str, Any]:
        namespace_encoded = quote(namespace, safe="")
        name_encoded = quote(name, safe="")
        try:
            return await self._fetch_template_from_route("/api/workflow_templates", f"{namespace_encoded}/{name_encoded}")
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"ComfyUI не нашёл шаблон {namespace}/{name}") from exc

    def _load_template_from_disk_file(self, location: str) -> Dict[str, Any]:
        if self._templates_dir is None:
            raise FileNotFoundError("Локальный каталог шаблонов не настроен")

        path = Path(location)
        if not path.is_absolute():
            path = (self._templates_dir / location).resolve()

        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise FileNotFoundError(f"Не удалось прочитать шаблон {path}") from exc

        try:
            data = json.loads(text or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Файл шаблона {path} содержит некорректный JSON") from exc

        if not isinstance(data, dict):
            raise RuntimeError(f"Файл шаблона {path} не содержит объект workflow")
        return data

    async def list_models(self, model_type: str, refresh: bool = False) -> List[str]:
        key = (model_type or "").lower()
        if not refresh and key in self._model_cache:
            return self._model_cache.get(key, [])

        params = {"type": model_type}
        async with self._request("GET", "/models", params=params) as resp:
            status = resp.status
            reason = resp.reason or ""
            text = await resp.text()

        if status == 404:
            LOGGER.debug("ComfyUI не поддерживает /models для типа %s", model_type)
            self._model_cache[key] = []
            return []

        if status >= 400:
            detail = text.strip() or reason or str(status)
            raise RuntimeError(f"HTTP {status} при запросе моделей ({model_type}): {detail}")

        try:
            data = json.loads(text or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"ComfyUI вернул некорректный JSON при запросе моделей ({model_type})") from exc

        names: List[str] = []
        if isinstance(data, dict):
            for candidate_key in ("models", "items", "model_list", "data", "names", model_type):
                candidate = data.get(candidate_key)
                extracted = self._coerce_model_names(candidate)
                if extracted:
                    names.extend(extracted)
                    break
        elif isinstance(data, list):
            names.extend(self._coerce_model_names(data))
        else:
            names.extend(self._coerce_model_names(data))

        normalized: List[str] = []
        seen: set[str] = set()
        for name in names:
            if not isinstance(name, str):
                continue
            trimmed = name.strip()
            if not trimmed:
                continue
            lowered = trimmed.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            normalized.append(trimmed)

        filtered = self._filter_model_names(normalized)

        if not filtered and model_type:
            alt_path = f"/models/{model_type}"
            async with self._request("GET", alt_path) as alt_resp:
                alt_status = alt_resp.status
                alt_reason = alt_resp.reason or ""
                alt_text = await alt_resp.text()

            if alt_status == 404:
                LOGGER.debug("ComfyUI не поддерживает %s", alt_path)
            elif alt_status >= 400:
                detail = alt_text.strip() or alt_reason or str(alt_status)
                raise RuntimeError(f"HTTP {alt_status} при запросе моделей ({alt_path}): {detail}")
            else:
                try:
                    alt_data = json.loads(alt_text or "{}")
                except json.JSONDecodeError:
                    alt_data = alt_text.splitlines()

                alt_names = []
                if isinstance(alt_data, dict):
                    alt_names = self._coerce_model_names(alt_data)
                elif isinstance(alt_data, list):
                    alt_names = self._coerce_model_names(alt_data)
                else:
                    alt_names = self._coerce_model_names(alt_data)

                filtered = self._filter_model_names(alt_names)

        self._model_cache[key] = filtered
        return filtered

    async def submit_workflow(self, workflow: Dict, *, client_id: Optional[str] = None) -> tuple[str, str]:
        client_id = client_id or str(uuid.uuid4())
        payload = {"prompt": workflow, "client_id": client_id}

        async with self._request("POST", "/prompt", json=payload) as resp:
            raw_text = await resp.text()
            if resp.status >= 400:
                details = _extract_error_message(raw_text)
                raise RuntimeError(f"HTTP {resp.status} при отправке workflow: {details}")

            try:
                body = json.loads(raw_text)
            except json.JSONDecodeError as exc:
                raise RuntimeError("ComfyUI вернул некорректный JSON при отправке workflow") from exc
            prompt_id = body.get("prompt_id")
            if not prompt_id:
                raise RuntimeError("ComfyUI did not return prompt_id")
            return prompt_id, client_id

    async def interrupt(self) -> Dict:
        async with self._request("POST", "/interrupt") as resp:
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if "json" in content_type:
                return await resp.json()
            text = await resp.text()
            if not text:
                return {}
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"raw": text}

    async def clear_queue(self) -> Dict:
        async with self._request("POST", "/queue", json={"queue": []}) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_queue_state(self) -> Dict:
        async with self._request("GET", "/queue") as resp:
            resp.raise_for_status()
            return await resp.json()

    async def track_progress(self, client_id: str, prompt_id: str) -> AsyncGenerator[ProgressEvent | ExecutionResult, None]:
        await self._ensure_connection()
        ws_url = f"{self._ws_url}?clientId={client_id}"
        LOGGER.debug("Track progress ws=%s", ws_url)

        async with websockets.connect(ws_url, open_timeout=10) as socket:
            async for item in self._track_loop(socket, prompt_id):
                yield item

    async def _track_loop(
        self,
        socket: WebSocketClientProtocol,
        prompt_id: str,
    ) -> AsyncGenerator[ProgressEvent | ExecutionResult, None]:
        last_node_id: Optional[str] = None
        last_value: float = 0.0
        last_maximum: float = 100.0
        seen_progress = False

        while True:
            try:
                message = await socket.recv()
            except websockets.ConnectionClosed:
                LOGGER.info("WebSocket closed by server")
                return

            if isinstance(message, (bytes, bytearray)):
                preview_payload = self._parse_binary_preview(bytes(message))
                if preview_payload is None:
                    LOGGER.debug("Skipping unknown binary websocket frame (len=%s)", len(message))
                    continue

                if not seen_progress:
                    LOGGER.debug("Preview frame received before any progress; deferring notification")
                    continue

                yield ProgressEvent(
                    prompt_id=prompt_id,
                    node_id=last_node_id,
                    value=last_value,
                    maximum=last_maximum,
                    preview=preview_payload,
                )
                continue

            try:
                data = json.loads(message)
            except (json.JSONDecodeError, UnicodeDecodeError):
                LOGGER.debug("Skipping non-JSON websocket frame", exc_info=True)
                continue

            LOGGER.debug("WS frame type=%s keys=%s", data.get("type"), list(data.keys()))

            if data.get("type") == "progress":
                normalized = self._normalize_progress_frame(data)
                if normalized is None:
                    LOGGER.debug("Progress frame ignored: %s", data)
                    continue
                node_id, value, maximum, preview_payload = normalized
                LOGGER.debug("Progress parsed: node=%s value=%s max=%s", node_id, value, maximum)
                last_node_id = node_id
                last_value = value
                last_maximum = maximum if maximum else 100.0
                seen_progress = True
                yield ProgressEvent(
                    prompt_id=data.get("prompt_id", prompt_id),
                    node_id=node_id,
                    value=value,
                    maximum=maximum,
                    preview=preview_payload,
                )
            elif data.get("type") == "executed":
                yield ExecutionResult(prompt_id=prompt_id, data=data)
                return
            elif data.get("type") == "execution_error":
                yield ExecutionResult(prompt_id=prompt_id, data=data)
                return

    async def get_history(self, prompt_id: str) -> Dict:
        async with self._request("GET", f"/history/{prompt_id}") as resp:
            resp.raise_for_status()
            return await resp.json()

    async def fetch_images(
        self,
        outputs: Dict,
        *,
        target_dir: Path,
    ) -> List[Path]:
        target_dir.mkdir(parents=True, exist_ok=True)
        files: List[Path] = []

        for node_outputs in outputs.values():
            for image in node_outputs.get("images", []):
                filename = image.get("filename")
                if not filename:
                    continue
                subfolder = image.get("subfolder", "")
                image_path = await self._download_image(filename, subfolder, target_dir)
                files.append(image_path)
        return files

    def locate_output_files(self, outputs: Dict, base_dir: Path) -> List[Path]:
        matches: List[Path] = []
        seen: set[Path] = set()

        for node_outputs in outputs.values():
            images = node_outputs.get("images", [])
            if not isinstance(images, list):
                continue
            for image in images:
                if not isinstance(image, dict):
                    continue
                filename = image.get("filename")
                if not filename:
                    continue
                subfolder = image.get("subfolder")
                candidates: List[Path] = []
                if isinstance(subfolder, str) and subfolder:
                    pure = PurePosixPath(subfolder)
                    safe = Path(*pure.parts)
                    candidates.append(base_dir / safe / filename)
                candidates.append(base_dir / filename)

                for candidate in candidates:
                    if candidate in seen:
                        continue
                    if candidate.exists():
                        seen.add(candidate)
                        matches.append(candidate)
                        break

        return matches

    async def fetch_images_from_output_dir(
        self,
        directory: Path,
        *,
        limit: int = 20,
    ) -> List[Tuple[Path, float]]:
        directory.mkdir(parents=True, exist_ok=True)
        files: List[Tuple[Path, float]] = []

        try:
            for entry in directory.glob("**/*"):
                if not entry.is_file():
                    continue
                if entry.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp", ".bmp"}:
                    continue
                mtime = entry.stat().st_mtime
                files.append((entry, mtime))
        except FileNotFoundError:
            return []

        files.sort(key=lambda item: item[1], reverse=True)
        return files[:limit]

    async def _download_image(self, filename: str, subfolder: str, target_dir: Path) -> Path:
        params = {"filename": filename, "subfolder": subfolder, "type": "output"}
        async with self._request("GET", "/view", params=params) as resp:
            resp.raise_for_status()
            content = await resp.read()

        file_path = target_dir / filename
        with file_path.open("wb") as fp:
            fp.write(content)
        return file_path

    @asynccontextmanager
    async def _request(self, method: str, path: str, **kwargs):
        response = await self._perform_request(method, path, **kwargs)
        try:
            yield response
        finally:
            response.release()

    async def _perform_request(self, method: str, path: str, **kwargs) -> aiohttp.ClientResponse:
        self._ensure_state_defaults()
        await self._ensure_connection()
        try:
            return await self._send_request(method, path, **kwargs)
        except (ClientError, asyncio.TimeoutError):  # pragma: no cover - network issues
            await self._ensure_connection(force=True)
            return await self._send_request(method, path, **kwargs)

    async def _send_request(self, method: str, path: str, **kwargs) -> aiohttp.ClientResponse:
        self._ensure_state_defaults()
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{self._base_http_url}{normalized_path}"
        return await self.session.request(method, url, **kwargs)

    async def _ensure_connection(self, *, force: bool = False) -> None:
        self._ensure_state_defaults()
        if not force and self._endpoint_ready:
            return
        async with self._endpoint_lock:
            if not force and self._endpoint_ready:
                return
            await self._detect_endpoint(force=force)

    async def _detect_endpoint(self, *, force: bool) -> None:
        candidates = self._candidate_http_urls(force=force)
        last_error: Optional[Exception] = None

        for base_url in candidates:
            try:
                timeout = aiohttp.ClientTimeout(total=5)
                response = await self.session.get(f"{base_url}/system_stats", timeout=timeout)
                try:
                    if response.status < 500:
                        self._base_http_url = base_url.rstrip("/")
                        self._ws_url = self._build_ws_url(self._base_http_url)
                        self._endpoint_ready = True
                        LOGGER.debug("Detected ComfyUI endpoint: %s", self._base_http_url)
                        return
                    last_error = RuntimeError(f"Unexpected status {response.status}")
                finally:
                    response.release()
            except Exception as exc:  # pragma: no cover - detection best effort
                last_error = exc

        self._endpoint_ready = False
        message = "Не удалось обнаружить ComfyUI на доступных портах"
        if last_error:
            LOGGER.debug(message, exc_info=last_error)
        raise RuntimeError(message) from last_error

    def _candidate_http_urls(self, *, force: bool) -> List[str]:
        reference = self._base_http_url if self._endpoint_ready and not force else self._configured_http_url
        parsed = urlparse(reference)
        host = parsed.hostname or self._auto_host
        scheme = parsed.scheme or self._auto_scheme
        path = (parsed.path or self._auto_path or "").rstrip("/")
        if path and not path.startswith("/"):
            path = f"/{path}"

        urls: List[str] = []

        def _compose(port: Optional[int]) -> str:
            if port:
                netloc = f"{host}:{port}"
            else:
                if parsed.netloc and parsed.netloc.split(":")[0] == host:
                    netloc = parsed.netloc
                elif parsed.port:
                    netloc = f"{host}:{parsed.port}"
                elif self._auto_port:
                    netloc = f"{host}:{self._auto_port}"
                else:
                    netloc = host
            base = urlunparse((scheme, netloc, path, "", "", ""))
            return base.rstrip("/")

        urls.append(_compose(parsed.port or self._auto_port))

        for port in range(8000, 8011):
            candidate = _compose(port)
            if candidate not in urls:
                urls.append(candidate)

        return urls

    def _build_ws_url(self, http_url: str) -> str:
        parsed_http = urlparse(http_url)
        template = urlparse(self._configured_ws_url)
        scheme = template.scheme or ("wss" if parsed_http.scheme == "https" else "ws")
        host = parsed_http.hostname or template.hostname or self._auto_host
        port = parsed_http.port or template.port
        netloc = f"{host}:{port}" if port else host
        path = template.path or "/ws"
        if not path.startswith("/"):
            path = f"/{path}"
        return urlunparse((scheme, netloc, path, "", "", ""))

    def _normalize_progress_frame(self, frame: Dict[str, Any]) -> Optional[Tuple[Optional[str], float, float, Optional[PreviewPayload]]]:
        payload = frame.get("data")
        if not isinstance(payload, dict):
            return None

        node_id: Optional[str] = None
        value: Optional[float] = None
        maximum: Optional[float] = None
        preview_payload: Optional[PreviewPayload] = None

        node_id = self._extract_node_id(payload) or node_id
        value = self._extract_number(payload, "value", "progress")
        maximum = self._extract_number(payload, "max", "maximum")
        preview_payload = self._extract_preview(payload.get("preview"))

        status = payload.get("status")
        if isinstance(status, dict):
            node_id = self._extract_node_id(status) or node_id
            value = value if value is not None else self._extract_number(status, "value", "progress")
            maximum = maximum if maximum is not None else self._extract_number(status, "max", "maximum")
            preview_payload = preview_payload or self._extract_preview(status.get("preview"))

            exec_info = status.get("exec_info") or status.get("execution") or status.get("exec")
            if isinstance(exec_info, dict):
                node_id = self._extract_node_id(exec_info) or node_id
                value = value if value is not None else self._extract_number(exec_info, "value", "progress")
                maximum = maximum if maximum is not None else self._extract_number(exec_info, "max", "maximum")
                preview_payload = preview_payload or self._extract_preview(exec_info.get("preview"))

        if value is None and maximum is None:
            return None

        value = float(value) if value is not None else 0.0
        maximum = float(maximum) if maximum not in (None, 0) else 100.0

        if maximum <= 0:
            maximum = 100.0
        if value > maximum:
            value = maximum

        node_id_str = str(node_id) if node_id is not None else None
        return (node_id_str, value, maximum, preview_payload)

    @staticmethod
    def _extract_number(source: Dict[str, Any], *keys: str) -> Optional[float]:
        if not isinstance(source, dict):
            return None
        for key in keys:
            if key not in source:
                continue
            raw = source.get(key)
            if raw is None:
                continue
            try:
                return float(raw)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _extract_node_id(source: Dict[str, Any]) -> Optional[str]:
        if not isinstance(source, dict):
            return None
        for key in ("node_id", "node", "current_node", "id"):
            if key in source and source[key] is not None:
                return str(source[key])
        return None

    def _extract_preview(self, preview: Any) -> Optional[PreviewPayload]:
        if preview is None:
            return None
        if isinstance(preview, PreviewPayload):
            return preview
        if isinstance(preview, dict):
            raw_image = preview.get("image") or preview.get("image_base64") or preview.get("img") or preview.get("data")
            if isinstance(raw_image, str):
                try:
                    decoded = base64.b64decode(raw_image)
                except (binascii.Error, ValueError):  # pragma: no cover - malformed preview data
                    decoded = None
                if decoded:
                    mime = preview.get("mime") or preview.get("mime_type") or preview.get("type") or "image/png"
                    return PreviewPayload(image=decoded, mime_type=str(mime))
        if isinstance(preview, (bytes, bytearray)):
            return PreviewPayload(image=bytes(preview), mime_type="image/png")
        return None

    def _parse_binary_preview(self, payload: bytes) -> Optional[PreviewPayload]:
        if len(payload) <= 8:
            return None

        header_type = int.from_bytes(payload[0:4], byteorder="big", signed=False)
        if header_type != 1:
            return None

        image_data = payload[8:]
        if not image_data:
            return None

        if image_data.startswith(b"\x89PNG"):
            mime = "image/png"
        elif image_data.startswith(b"\xff\xd8\xff"):
            mime = "image/jpeg"
        elif image_data.startswith(b"RIFF"):
            mime = "image/webp"
        else:
            mime = "image/jpeg"

        return PreviewPayload(image=image_data, mime_type=mime)

    async def restart(self, command: str) -> int:
        LOGGER.info("Executing restart command: %s", command)
        process = await asyncio.create_subprocess_shell(command)
        return await process.wait()


async def gather_outputs(history: Dict, prompt_id: str) -> Dict:
    """Extract outputs for the specific prompt from history response."""
    if not isinstance(history, dict):
        return {}

    prompt_data: Optional[Dict] = None

    nested_history = history.get("history")
    if isinstance(nested_history, dict):
        candidate = nested_history.get(prompt_id)
        if isinstance(candidate, dict):
            prompt_data = candidate

    if prompt_data is None:
        candidate = history.get(prompt_id)
        if isinstance(candidate, dict):
            prompt_data = candidate

    if prompt_data is None and isinstance(history, dict):
        for value in history.values():
            if isinstance(value, dict) and value.get("prompt_id") == prompt_id:
                prompt_data = value
                break

    if not prompt_data:
        return {}

    outputs = prompt_data.get("outputs")
    return outputs if isinstance(outputs, dict) else {}


def _extract_error_message(raw_text: str) -> str:
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        text = raw_text.strip()
        return text[:500] if len(text) > 500 else text

    for key in ("error", "message", "detail", "details"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return json.dumps(data, ensure_ascii=False)[:500]

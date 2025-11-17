#!/usr/bin/env python
"""Quick structural check for ComfyUI workflow JSON files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

REQUIRED_NODE_TYPES: Set[str] = {
    "CheckpointLoaderSimple",
    "CLIPTextEncode",
    "EmptyLatentImage",
    "KSampler",
    "VAEDecode",
    "SaveImage",
}


def _normalize_nodes(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    nodes = payload.get("nodes")
    result: List[Dict[str, Any]] = []
    if isinstance(nodes, list):
        for node in nodes:
            if isinstance(node, dict):
                result.append(node)
    elif isinstance(nodes, dict):
        for key, value in nodes.items():
            if isinstance(value, dict):
                value = dict(value)
                value.setdefault("id", key)
                result.append(value)
    return result


def _iter_links(payload: Dict[str, Any]) -> Iterable[Iterable[Any]]:
    links = payload.get("links")
    if isinstance(links, list):
        for item in links:
            if isinstance(item, list):
                yield item


def validate_workflow(path: Path) -> List[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"Не удалось прочитать JSON: {exc}"]

    if not isinstance(data, dict):
        return ["Корневой объект workflow не является JSON-объектом"]

    errors: List[str] = []
    nodes = _normalize_nodes(data)
    if not nodes:
        errors.append("Workflow не содержит нод")
        return errors

    types_found: Set[str] = set()
    node_ids: Set[str] = set()
    for node in nodes:
        node_id = str(node.get("id")) if node.get("id") is not None else None
        node_type = node.get("type") or node.get("class_type")
        if node_id is None:
            errors.append("Обнаружена нода без идентификатора")
        else:
            node_ids.add(node_id)
        if isinstance(node_type, str):
            types_found.add(node_type)

    missing_types = REQUIRED_NODE_TYPES - types_found
    if missing_types:
        missing = ", ".join(sorted(missing_types))
        errors.append(f"Отсутствуют обязательные ноды: {missing}")

    link_sources_missing: List[str] = []
    for link in _iter_links(data):
        if len(link) < 4:
            errors.append(f"Невалидная запись link: {link}")
            continue
        src_node = str(link[1])
        dst_node = str(link[3])
        if src_node not in node_ids:
            link_sources_missing.append(src_node)
        if dst_node not in node_ids:
            link_sources_missing.append(dst_node)
    if link_sources_missing:
        missing = ", ".join(sorted(set(link_sources_missing)))
        errors.append(f"Links ссылаются на отсутствующие ноды: {missing}")

    save_nodes = [node for node in nodes if (node.get("type") or node.get("class_type")) == "SaveImage"]
    if not save_nodes:
        errors.append("Не найдена нода SaveImage")
    else:
        for node in save_nodes:
            images_input = node.get("inputs")
            if isinstance(images_input, dict):
                value = images_input.get("images")
                if value in (None, ""):
                    errors.append("SaveImage имеет пустой вход 'images'")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Проверка структуры workflow JSON")
    default_path = Path(__file__).resolve().parents[1] / "data" / "workflows" / "default.json"
    parser.add_argument(
        "--workflow",
        type=Path,
        default=default_path,
        help="Путь к workflow для проверки (по умолчанию data/workflows/default.json)",
    )
    args = parser.parse_args()

    workflow_path = args.workflow.expanduser().resolve()
    if not workflow_path.exists():
        print(f"⚠️ Файл {workflow_path} не найден", file=sys.stderr)
        return 2

    errors = validate_workflow(workflow_path)
    if errors:
        print("❌ Найдены проблемы:")
        for issue in errors:
            print(f" - {issue}")
        return 1

    print(f"✅ Workflow {workflow_path} прошёл проверку")
    return 0


if __name__ == "__main__":
    sys.exit(main())

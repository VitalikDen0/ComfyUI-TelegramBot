from __future__ import annotations

from html import escape
from typing import Dict, Iterable, List, Tuple


def format_workflow_summary(workflow: Dict) -> str:
    """Generate HTML summary for workflow nodes."""
    nodes = workflow.get("nodes") or workflow.get("workflow") or {}

    if isinstance(nodes, dict):
        node_items = [(str(key), value) for key, value in nodes.items()]
    elif isinstance(nodes, list):
        node_items = [(str(node.get("id", idx)), node) for idx, node in enumerate(nodes)]
    else:
        node_items = []

    node_items.sort(key=lambda item: _node_sort_key(item[0]))
    node_lookup = {node_id: node for node_id, node in node_items}

    lines: List[str] = ["<b>Ваш workflow</b>"]
    if not nodes:
        lines.append("<i>Пока нет ни одной ноды. Добавьте новую через меню.</i>")
        return "\n".join(lines)

    for key, node in node_items:
        node_type = node.get("class_type") or node.get("type") or "Unknown"
        title = node.get("_meta", {}).get("title") if isinstance(node.get("_meta"), dict) else None
        label = escape(str(node_type))
        if title:
            label = f"{escape(str(title))} ({escape(str(node_type))})"

        lines.append(f"• <b>#{escape(str(key))}</b> — {label}")

        inputs = node.get("inputs", {})
        if isinstance(inputs, dict) and inputs:
            for name, value in inputs.items():
                display_value = _format_value(value)
                lines.append(f"  └ <code>{escape(str(name))}</code>: {display_value}")

    connections = _collect_connections(node_items)
    if connections:
        lines.append("")
        lines.append("<b>⚡️ Связи</b>")
        for src_id, dst_id, input_name, output_label in connections:
            src_label = escape(f"#{src_id}")
            dst_label = escape(f"#{dst_id}")
            input_markup = escape(input_name)
            output_markup = escape(output_label)
            lines.append(f"• {src_label} → {dst_label} <code>{input_markup}</code> ({output_markup})")

    return "\n".join(lines)


def _format_value(value) -> str:
    if isinstance(value, list):
        if not value:
            return "[]"
        if _looks_like_connection(value):
            parts = _normalize_connections(value)
            formatted = [f"→ #{_safe_str(src)}:{_describe_output(idx, name)}" for src, idx, name in parts]
            return "; ".join(formatted)
        if all(isinstance(item, (int, float, str)) for item in value):
            return ", ".join(escape(str(item)) for item in value[:4]) + ("…" if len(value) > 4 else "")
        return "[complex]"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        ref_node = value.get("node")
        ref_field = value.get("field")
        if ref_node is not None and ref_field is not None:
            return f"→ #{ref_node}:{ref_field}"
        return "{…}"
    if value is None:
        return "—"
    text = escape(str(value))
    return text if len(text) <= 64 else text[:61] + "…"


def _node_sort_key(node_id: str) -> Tuple[int, str]:
    try:
        return (0, str(int(node_id)))
    except ValueError:
        return (1, node_id)


def _looks_like_connection(value) -> bool:
    if isinstance(value, list) and value:
        sample = value[0]
        if isinstance(sample, (list, tuple)):
            return len(sample) >= 2
        return len(value) >= 2 and isinstance(value[0], (str, int)) and isinstance(value[1], (int, str))
    if isinstance(value, tuple) and len(value) >= 2:
        return isinstance(value[0], (str, int))
    return False


def _normalize_connections(raw) -> List[Tuple[str, int, str | None]]:
    parts: List[Tuple[str, int, str | None]] = []
    if isinstance(raw, (list, tuple)):
        candidates = raw
        # Single connection stored directly
        if candidates and not isinstance(candidates[0], (list, tuple)):
            candidates = [candidates]
        for item in candidates:
            if not isinstance(item, (list, tuple)) or not item:
                continue
            source = str(item[0])
            raw_index = item[1] if len(item) > 1 else 0
            try:
                output_index = int(raw_index)
            except (TypeError, ValueError):
                output_index = 0
            output_name = str(item[2]) if len(item) > 2 else None
            parts.append((source, output_index, output_name))
    return parts


def _collect_connections(node_items: Iterable[Tuple[str, Dict]]) -> List[Tuple[str, str, str, str]]:
    connections: List[Tuple[str, str, str, str]] = []
    for node_id, node in node_items:
        if not isinstance(node, dict):
            continue
        inputs = node.get("inputs") if isinstance(node.get("inputs"), dict) else None
        if not inputs:
            continue
        for name, value in inputs.items():
            if not _looks_like_connection(value):
                continue
            for source, index, output_name in _normalize_connections(value):
                connections.append((source, node_id, str(name), _describe_output(index, output_name)))
    connections.sort(key=lambda item: (_node_sort_key(item[0]), _node_sort_key(item[1]), item[2]))
    return connections


def _describe_output(index: int, name: str | None) -> str:
    if name and name.strip():
        return name
    return f"out#{index}"


def _safe_str(value) -> str:
    return escape(str(value))

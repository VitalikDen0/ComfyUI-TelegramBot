import type { ComfyWorkflow } from "./types";
import { showAlert } from "./telegram";

const API_BASE = import.meta.env.VITE_API_BASE?.toString().replace(/\/$/, "") || "";

export async function fetchWorkflow(sessionId: string): Promise<ComfyWorkflow> {
  const url = `${API_BASE}/api/workflow/${encodeURIComponent(sessionId)}`;
  const res = await fetch(url, { method: "GET" });
  if (!res.ok) {
    const text = await res.text();
    const message = text || res.statusText || "Failed to fetch workflow";
    showAlert(message);
    throw new Error(message);
  }
  const data = await res.json();
  if (data && typeof data === "object" && "workflow" in data && data.workflow) {
    return data.workflow as ComfyWorkflow;
  }
  return data as ComfyWorkflow;
}

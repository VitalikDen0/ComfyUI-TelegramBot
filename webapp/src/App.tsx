import { useEffect, useMemo, useState } from "react";
import ReactFlow, { Background, Controls, Edge, Node, OnNodesChange, applyNodeChanges } from "reactflow";
import "reactflow/dist/style.css";
import { initTelegramUi, showAlert } from "./telegram";
import type { ComfyLink, ComfyWorkflow, FetchState } from "./types";
import { fetchWorkflow } from "./workflow-api";

const EMPTY_STATE: FetchState<ComfyWorkflow> = { loading: true };

function App() {
  const [state, setState] = useState<FetchState<ComfyWorkflow>>(EMPTY_STATE);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [nodesState, setNodesState] = useState<Node[]>([]);
  useEffect(() => {
    initTelegramUi();

    const params = new URLSearchParams(window.location.search);
    const sessionId = params.get("sid") || params.get("session") || "";
    if (!sessionId) {
      setState({ loading: false, error: "Нет session id" });
      showAlert("Ошибка: отсутствует session id. Откройте Mini App из бота, чтобы получить ссылку.");
      return;
    }

    fetchWorkflow(sessionId)
      .then((workflow) => setState({ loading: false, data: workflow }))
      .catch((err) => {
        console.error(err);
        setState({ loading: false, error: "Не удалось загрузить workflow" });
        showAlert("Ошибка загрузки workflow");
      });
  }, []);

  const { nodes, edges, nodeMap } = useMemo(() => {
    if (!state.data) return { nodes: [] as Node[], edges: [] as Edge[], nodeMap: new Map<string, any>() };
    return transformWorkflow(state.data);
  }, [state.data]);

  useEffect(() => {
    setNodesState(nodes);
  }, [nodes]);

  const onNodesChange: OnNodesChange = (changes) => {
    setNodesState((nds) => applyNodeChanges(changes, nds));
  };

  const selectedNode = useMemo(() => {
    if (!selectedId) return null;
    return nodeMap.get(selectedId) || null;
  }, [selectedId, nodeMap]);

  if (state.loading) {
    return (
      <div className="flex h-screen items-center justify-center bg-slate-950 text-slate-100 text-lg">
        Загрузка…
      </div>
    );
  }

  if (state.error) {
    return (
      <div className="flex h-screen items-center justify-center bg-slate-950 text-slate-100 px-6 text-center">
        <div className="rounded-lg border border-red-500/50 bg-red-900/20 px-4 py-3 shadow-lg">
          <div className="text-lg font-semibold">Ошибка</div>
          <div className="text-sm text-red-200 mt-1">{state.error}</div>
          <div className="text-xs text-slate-300 mt-2">Откройте Mini App из бота, чтобы получить ссылку с sid.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen w-screen bg-slate-950 text-slate-100 overflow-hidden">
      <ReactFlow
        nodes={nodesState}
        edges={edges}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.25}
        maxZoom={2.5}
        onNodesChange={onNodesChange}
        onNodeClick={(_, node) => setSelectedId(node.id)}
      >
        <Background color="#1e293b" gap={16} size={1} />
        <Controls showInteractive={false} position="bottom-right" />
      </ReactFlow>

      <SidePanel node={selectedNode} onClose={() => setSelectedId(null)} />
    </div>
  );
}

function transformWorkflow(workflow: ComfyWorkflow): { nodes: Node[]; edges: Edge[]; nodeMap: Map<string, any> } {
  const nodesRaw = Array.isArray(workflow.nodes)
    ? workflow.nodes
    : Object.entries(workflow.nodes || {}).map(([id, payload]) => ({ id, ...(payload as any) }));

  const nodeMap = new Map<string, any>();

  const nodes: Node[] = nodesRaw
    .filter((node) => typeof node === "object" && node !== null)
    .map((node: any) => {
      const id = String(node.id ?? node.key ?? node._id ?? Math.random().toString(36).slice(2));
      nodeMap.set(id, node);
      return {
        id,
        type: "default",
        position: inferPosition(node, id),
        data: {
          label: node._meta?.title || node.class_type || node.type || `Нода ${id}`,
          subtitle: node.class_type || node.type,
        },
        style: {
          background: "#0f172a",
          color: "#e2e8f0",
          border: "1px solid #1e293b",
          padding: 8,
          borderRadius: 10,
          boxShadow: "0 4px 18px rgba(0,0,0,0.35)",
          minWidth: 140,
        },
        draggable: false,
      } satisfies Node;
    });

  const edges: Edge[] = (workflow.links || [])
    .map((link, idx) => normalizeLink(link, idx))
    .filter((edge): edge is Edge => Boolean(edge));

  return { nodes, edges, nodeMap };
}

function inferPosition(node: any, id: string) {
  const pos = node.pos || node.position;
  if (Array.isArray(pos) && pos.length >= 2) {
    return { x: Number(pos[0]) || 0, y: Number(pos[1]) || 0 };
  }
  if (pos && typeof pos === "object" && "x" in pos && "y" in pos) {
    return { x: Number(pos.x) || 0, y: Number(pos.y) || 0 };
  }
  // fallback grid
  const hash = Math.abs(hashCode(id));
  const col = hash % 6;
  const row = Math.floor(hash / 6) % 6;
  return { x: col * 260, y: row * 180 };
}

function normalizeLink(link: ComfyLink, idx: number): Edge | null {
  if (!Array.isArray(link)) return null;
  const [, fromNode, fromPort, toNode, toPort] = link;
  if (fromNode == null || toNode == null) return null;
  return {
    id: `e-${idx}-${fromNode}-${toNode}`,
    source: String(fromNode),
    target: String(toNode),
    sourceHandle: String(fromPort ?? "0"),
    targetHandle: String(toPort ?? "0"),
    animated: true,
    style: { stroke: "#38bdf8", strokeWidth: 1.8 },
  } satisfies Edge;
}

function hashCode(str: string) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = Math.imul(31, h) + str.charCodeAt(i) | 0;
  }
  return h;
}

export default App;

type SidePanelProps = {
  node: any;
  onClose: () => void;
};

function SidePanel({ node, onClose }: SidePanelProps) {
  const inputs = node?.inputs ? Object.keys(node.inputs) : [];
  const outputs = node?.outputs ? Object.keys(node.outputs) : [];

  return (
    <div className={`side-panel ${node ? "visible" : ""}`}>
      {node ? (
        <>
          <div className="side-panel__header">
            <div>
              <div className="side-panel__title">{node._meta?.title || node.class_type || node.type || "Нода"}</div>
              <div className="side-panel__subtitle">{node.class_type || node.type || ""}</div>
            </div>
            <button className="side-panel__close" onClick={onClose}>
              ✕
            </button>
          </div>

          <div className="side-panel__section">
            <div className="side-panel__label">Inputs</div>
            {inputs.length === 0 && <div className="side-panel__muted">Нет входов</div>}
            {inputs.length > 0 && (
              <ul className="side-panel__list">
                {inputs.map((key) => (
                  <li key={key}>{key}</li>
                ))}
              </ul>
            )}
          </div>

          <div className="side-panel__section">
            <div className="side-panel__label">Outputs</div>
            {outputs.length === 0 && <div className="side-panel__muted">Нет выходов</div>}
            {outputs.length > 0 && (
              <ul className="side-panel__list">
                {outputs.map((key) => (
                  <li key={key}>{key}</li>
                ))}
              </ul>
            )}
          </div>

          <div className="side-panel__section">
            <div className="side-panel__label">ID</div>
            <div className="side-panel__value">{node.id}</div>
          </div>
        </>
      ) : (
        <div className="side-panel__muted">Выберите ноду, чтобы увидеть её входы/выходы.</div>
      )}
    </div>
  );
}

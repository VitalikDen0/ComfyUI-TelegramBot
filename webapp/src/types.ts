export type ComfyLink = [number, string | number, number, string | number, number?, string?];

export interface ComfyWorkflow {
  nodes: Record<string, any> | any[];
  links?: ComfyLink[];
}

export interface SessionWorkflowResponse {
  workflow: ComfyWorkflow | Record<string, any>;
}

export interface FetchState<T> {
  data?: T;
  error?: string;
  loading: boolean;
}

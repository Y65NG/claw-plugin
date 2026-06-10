export type SessionStatus =
  | "idle"
  | "running"
  | "completed"
  | "failed"
  | "interrupted"
  | "archived";

export type SessionSummary = {
  id: string;
  title: string;
  status: SessionStatus;
  hostKind: string;
  runnerCommand: string;
  createdAt: string;
  updatedAt: string;
  lastEventSeq: number;
};

export type SessionMessage = {
  id: string;
  sessionId: string;
  role: string;
  content: string;
  createdAt: string;
  seq?: number;
  messageSeq?: number;
  message_seq?: number;
  payload?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  data?: Record<string, unknown>;
  __openclaw?: Record<string, unknown>;
};

export type TimelineEvent = {
  id: string;
  sessionId: string;
  seq: number;
  kind: string;
  payload: Record<string, unknown>;
  createdAt: string;
};

export type SessionDetail = {
  session: SessionSummary;
  messages: SessionMessage[];
};

export type ControlAction = "stop" | "retry" | "rename" | "archive";

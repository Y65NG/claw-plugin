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

export type PluginStatusSnapshot = {
  hostKind: string;
  stateDir?: string;
  configPath?: string;
  serviceVersion?: string;
  pluginVersion?: string;
  port?: number;
  pid?: number;
  runnerCommand?: string;
  activeSessionCount: number;
  runningSessionCount: number;
  healthy: boolean;
  modelPrimary?: string;
  enabledSkills?: string[];
  hub53ai?: Hub53AIStatusSnapshot;
};

export type RunnerConfig = {
  command?: string;
  args?: string[];
  env?: Record<string, string>;
};

export type GatewayConfigView = {
  baseUrl?: string;
  botId?: string;
  secret?: string;
  requestTimeoutMs?: number;
  streamReconnectMs?: number;
};

export type Hub53AIStatusSnapshot = {
  enabled: boolean;
  configured: boolean;
  connectionStatus: "disabled" | "connecting" | "connected" | "disconnected" | "error";
  botId?: string;
  wsUrl?: string;
  lastHeartbeatAt?: string;
  lastConnectedAt?: string;
  lastError?: string;
  receivedMessageCount: number;
  sentMessageCount: number;
  pendingOutboundCount: number;
};

export type BootstrapPayload = {
  token: string;
  status: PluginStatusSnapshot;
  config: {
    runner?: RunnerConfig;
    gateway?: GatewayConfigView;
    config: Record<string, unknown>;
  };
};

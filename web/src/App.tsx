import { startTransition, useDeferredValue, useEffect, useEffectEvent, useMemo, useRef, useState } from "react";

import {
  createSession,
  fetchBootstrap,
  fetchSessionDetail,
  fetchSessionEvents,
  fetchSessions,
  sendMessage
} from "./api";
import "./App.css";
import type {
  BootstrapPayload,
  PluginStatusSnapshot,
  SessionMessage,
  SessionSummary,
  TimelineEvent
} from "./types";

type ConfigView = BootstrapPayload["config"] | null;

type ConversationItem =
  | {
      key: string;
      kind: "message";
      createdAt: string;
      message: SessionMessage;
      order: number;
    }
  | {
      key: string;
      kind: "activity";
      createdAt: string;
      activity: ActivityCard;
      order: number;
    };

type ActivityCard = {
  title: string;
  summary: string;
  details: string[];
  tone: "neutral" | "success" | "warning";
};

function resetScrollToTop(element: HTMLDivElement | null) {
  if (!element) {
    return;
  }

  if (typeof element.scrollTo === "function") {
    element.scrollTo({ top: 0 });
    return;
  }

  element.scrollTop = 0;
}

export function App() {
  const [token, setToken] = useState("");
  const [status, setStatus] = useState<PluginStatusSnapshot | null>(null);
  const [config, setConfig] = useState<ConfigView>(null);
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string>("");
  const [messages, setMessages] = useState<SessionMessage[]>([]);
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  const [expandedActivities, setExpandedActivities] = useState<Record<string, boolean>>({});
  const [draft, setDraft] = useState("");
  const [newSessionTitle, setNewSessionTitle] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const conversationBodyRef = useRef<HTMLDivElement | null>(null);
  const refreshStateRef = useRef({
    inFlight: false,
    queued: false
  });

  const deferredSessions = useDeferredValue(sessions);
  const sortedSessions = useMemo(() => sortSessions(deferredSessions), [deferredSessions]);
  const activeSession = useMemo(
    () => sortedSessions.find((session) => session.id === activeSessionId) ?? null,
    [activeSessionId, sortedSessions]
  );
  const conversationItems = useMemo(
    () => buildConversationItems(messages, events),
    [messages, events]
  );

  const refreshSessions = useEffectEvent(async () => {
    if (refreshStateRef.current.inFlight) {
      refreshStateRef.current.queued = true;
      return;
    }

    refreshStateRef.current.inFlight = true;
    try {
      do {
        refreshStateRef.current.queued = false;
        const nextSessions = await fetchSessions();
        startTransition(() => {
          setSessions(nextSessions);
          setActiveSessionId((current) => {
            if (current && nextSessions.some((session) => session.id === current)) {
              return current;
            }
            return mostRecentlyUpdatedSession(nextSessions)?.id ?? "";
          });
        });
      } while (refreshStateRef.current.queued);
    } finally {
      refreshStateRef.current.inFlight = false;
    }
  });

  const refreshActiveSessionDetail = useEffectEvent(async (sessionId: string) => {
    const [detail, timeline] = await Promise.all([fetchSessionDetail(sessionId), fetchSessionEvents(sessionId)]);
    startTransition(() => {
      setMessages(detail.messages);
      setEvents(timeline);
      setSessions((current) => syncSessionFromDetail(current, detail.session));
    });
  });

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [bootstrap, nextSessions] = await Promise.all([fetchBootstrap(), fetchSessions()]);
        if (cancelled) {
          return;
        }
        setToken(bootstrap.token);
        setStatus(bootstrap.status);
        setConfig(bootstrap.config);
        startTransition(() => {
          setSessions(nextSessions);
          setActiveSessionId((current) => current || mostRecentlyUpdatedSession(nextSessions)?.id || "");
        });
      } catch (loadError) {
        if (!cancelled) {
          setError((loadError as Error).message);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    if (typeof fetch === "function") {
      void load();
    } else {
      setLoading(false);
    }

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!activeSessionId) {
      setMessages([]);
      setEvents([]);
      return;
    }

    resetScrollToTop(conversationBodyRef.current);

    let cancelled = false;
    async function loadDetail() {
      try {
        const [detail, timeline] = await Promise.all([fetchSessionDetail(activeSessionId), fetchSessionEvents(activeSessionId)]);
        if (cancelled) {
          return;
        }
        setMessages(detail.messages);
        setEvents(timeline);
      } catch (detailError) {
        if (!cancelled) {
          setError((detailError as Error).message);
        }
      }
    }

    void loadDetail();

    const sessionSocket = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/sessions/${activeSessionId}`);
    sessionSocket.onmessage = (event) => {
      const payload = safeParseJson<TimelineEvent>(event.data);
      if (!payload?.sessionId || typeof payload.seq !== "number" || typeof payload.kind !== "string") {
        return;
      }
      setEvents((current) => (current.some((item) => item.seq === payload.seq) ? current : [...current, payload]));
      setSessions((current) => syncSessionFromEvent(current, activeSessionId, payload));
      if (payload.kind === "assistant.message") {
        const content = String((payload.payload as { content?: string }).content ?? "");
        if (content) {
          setMessages((current) =>
            current.some((message) => message.id === `assistant-${payload.seq}`)
              ? current
              : [
                  ...current,
                  {
                    id: `assistant-${payload.seq}`,
                    sessionId: activeSessionId,
                    role: "assistant",
                    content,
                    createdAt: payload.createdAt
                  }
                ]
          );
        }
      }
      if (payload.kind === "run.completed" || payload.kind === "run.failed" || payload.kind === "run.interrupted") {
        void refreshActiveSessionDetail(activeSessionId).catch((detailError) => {
          setError((detailError as Error).message);
        });
      }
    };
    sessionSocket.onerror = () => {
      setError("Session event stream disconnected.");
    };

    return () => {
      cancelled = true;
      sessionSocket.close();
    };
  }, [activeSessionId]);

  useEffect(() => {
    const statusSocket = new WebSocket(
      `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/status`
    );
    statusSocket.onmessage = (event) => {
      const snapshot = safeParseJson<PluginStatusSnapshot>(event.data);
      if (!snapshot) {
        return;
      }
      setStatus(snapshot);
      void refreshSessions().catch((refreshError) => {
        setError((refreshError as Error).message);
      });
    };
    statusSocket.onerror = () => {
      setError("Status stream disconnected.");
    };
    return () => {
      statusSocket.close();
    };
  }, []);

  async function handleCreateSession() {
    try {
      const title = newSessionTitle.trim() || `Session ${sessions.length + 1}`;
      const session = await createSession(token, title);
      setNewSessionTitle("");
      startTransition(() => {
        setSessions((current) => sortSessions([session, ...current.filter((entry) => entry.id !== session.id)]));
        setActiveSessionId(session.id);
      });
    } catch (createError) {
      setError((createError as Error).message);
    }
  }

  async function handleSendMessage(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!activeSessionId || !draft.trim()) {
      return;
    }

    const content = draft.trim();
    setDraft("");
    setMessages((current) => [
      ...current,
      {
        id: `user-${Date.now()}`,
        sessionId: activeSessionId,
        role: "user",
        content,
        createdAt: new Date().toISOString()
      }
    ]);

    try {
      await sendMessage(token, activeSessionId, content);
      setSessions((current) =>
        current.map((session) =>
          session.id === activeSessionId ? { ...session, status: "running" } : session
        )
      );
    } catch (sendError) {
      setError((sendError as Error).message);
    }
  }

  function toggleActivity(key: string, button?: HTMLButtonElement | null) {
    button?.blur();
    setExpandedActivities((current) => ({
      ...current,
      [key]: !current[key]
    }));
  }

  return (
    <div className="app-shell">
      <nav className="panel sessions-panel" aria-label="Sessions">
        <div className="panel-header">
          <div>
            <h2>Sessions</h2>
            <p className="panel-subtitle">Sessions follow the default timeline order.</p>
          </div>
          <button className="secondary-button" type="button" onClick={handleCreateSession}>
            New
          </button>
        </div>
        <div className="session-list">
          <input
            aria-label="New session title"
            placeholder="Session title"
            value={newSessionTitle}
            onChange={(event) => setNewSessionTitle(event.target.value)}
            className="secondary-button"
          />
          {sortedSessions.map((session) => (
            <button
              key={session.id}
              className={`session-item session-${session.status}${session.id === activeSessionId ? " is-active" : ""}`}
              type="button"
              onClick={() => setActiveSessionId(session.id)}
            >
              <span className="session-title">{session.title}</span>
              <span className="session-meta">
                <span>{session.runnerCommand || session.hostKind}</span>
                <span className={`status-pill status-${session.status}`}>{session.status}</span>
              </span>
              <span className="session-updated">Updated {formatUpdatedAt(session.updatedAt)}</span>
            </button>
          ))}
          {!deferredSessions.length && !loading ? (
            <div className="empty-state">No sessions yet. Create one to start driving Claw.</div>
          ) : null}
        </div>
      </nav>

      <main className="panel conversation-panel" aria-label="Conversation">
        <div className="panel-header">
          <div>
            <h2>{activeSession?.title ?? "Conversation"}</h2>
            <div className="session-meta">
              <span>{activeSession?.runnerCommand ?? "No runner selected"}</span>
              <span className={`status-pill status-${activeSession?.status ?? "idle"}`}>
                {activeSession?.status ?? "idle"}
              </span>
            </div>
          </div>
        </div>

        {error ? <div className="error-banner">{error}</div> : null}
        {loading ? <div className="loading-state">Loading control plane…</div> : null}

        <div ref={conversationBodyRef} className="conversation-body">
          {!conversationItems.length && !loading ? (
            <div className="empty-state">
              Send a message to start a run. Claw 的关键活动 (activity) 会直接嵌入到对话中，并默认折叠显示。
            </div>
          ) : null}
          {conversationItems.map((item) =>
            item.kind === "message" ? (
              <article key={item.key} className={`message ${item.message.role}`}>
                <div>{item.message.content}</div>
                <small>{new Date(item.message.createdAt).toLocaleString()}</small>
              </article>
            ) : (
              <article key={item.key} className={`activity-card tone-${item.activity.tone}`}>
                <div className="activity-header">
                  <div className="activity-summary">
                    <strong>{item.activity.title}</strong>
                    {item.activity.summary !== "Claw invoked this tool during the run." ? (
                      <span>{item.activity.summary}</span>
                    ) : null}
                  </div>
                  <div className="activity-meta">
                    <time>{formatUpdatedAt(item.createdAt)}</time>
                    <button
                      type="button"
                      className="activity-details-toggle"
                      onMouseDown={(event) => event.preventDefault()}
                      onClick={(event) => toggleActivity(item.key, event.currentTarget)}
                      aria-expanded={expandedActivities[item.key] === true}
                    >
                      {expandedActivities[item.key] ? "Hide details" : "Show details"}
                    </button>
                  </div>
                </div>
                {expandedActivities[item.key] ? (
                  <div className="activity-details">
                    <p>{item.activity.summary}</p>
                    {item.activity.details.map((detail) => (
                      <p key={detail}>{detail}</p>
                    ))}
                  </div>
                ) : null}
              </article>
            )
          )}
        </div>

        <div className="composer">
          <form onSubmit={handleSendMessage}>
            <textarea
              aria-label="Message input"
              placeholder="Ask Claw to do something..."
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
            />
            <div className="toolbar-row">
              <span className="session-meta">
                <span>
                  {activeSession?.status === "running"
                    ? "This session is actively running in QClaw."
                    : "UI disconnects do not stop the underlying run."}
                </span>
              </span>
              <button className="primary-button" type="submit" disabled={!activeSessionId}>
                Send
              </button>
            </div>
          </form>
        </div>
      </main>

      <aside className="panel sidebar-panel" aria-label="Status and claw details">
        <div className="panel-header">
          <h3>Status & claw details</h3>
        </div>
        <div className="sidebar-scroll">
          <section className="stat-grid">
            <div className="stat-card">
              <span className="stat-label">Host</span>
              <span className="stat-value">{status?.hostKind ?? "unknown"}</span>
            </div>
            <div className="stat-card">
              <span className="stat-label">Health</span>
              <span className="stat-value">{status?.healthy ? "Healthy" : "Unknown"}</span>
            </div>
            <div className="stat-card">
              <span className="stat-label">Active</span>
              <span className="stat-value">{status?.activeSessionCount ?? 0}</span>
            </div>
            <div className="stat-card">
              <span className="stat-label">Running</span>
              <span className="stat-value">{status?.runningSessionCount ?? 0}</span>
            </div>
            <div className="stat-card stat-card-wide">
              <span className="stat-label">Model</span>
              <span className="stat-value stat-value-compact">{status?.modelPrimary ?? "Unknown"}</span>
            </div>
            <div className="stat-card stat-card-wide">
              <span className="stat-label">Enabled skills</span>
              <span className="stat-value stat-value-compact">
                {status?.enabledSkills?.length ? status.enabledSkills.join(", ") : "None detected"}
              </span>
            </div>
          </section>

          <section className="config-card">
            <h3>Plugin config</h3>
            <details>
              <summary>Show redacted config</summary>
              <pre>{JSON.stringify(config, null, 2)}</pre>
            </details>
          </section>
        </div>
      </aside>
    </div>
  );
}

function sortSessions(sessions: SessionSummary[]): SessionSummary[] {
  return [...sessions].sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
}

function mostRecentlyUpdatedSession(sessions: SessionSummary[]): SessionSummary | null {
  return [...sessions].sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))[0] ?? null;
}

function syncSessionFromEvent(
  sessions: SessionSummary[],
  sessionId: string,
  event: TimelineEvent
): SessionSummary[] {
  return sortSessions(
    sessions.map((session) =>
      session.id === sessionId
        ? {
            ...session,
            status: nextStatusForEvent(event.kind, session.status),
            updatedAt: event.createdAt,
            lastEventSeq: Math.max(session.lastEventSeq, event.seq)
          }
        : session
    )
  );
}

function syncSessionFromDetail(
  sessions: SessionSummary[],
  detailSession: SessionSummary
): SessionSummary[] {
  const remaining = sessions.filter((session) => session.id !== detailSession.id);
  return sortSessions([detailSession, ...remaining]);
}

function nextStatusForEvent(kind: string, current: SessionSummary["status"]): SessionSummary["status"] {
  switch (kind) {
    case "run.started":
    case "assistant.delta":
    case "assistant.message":
    case "tool.call":
    case "tool.result":
      return "running";
    case "run.completed":
      return "completed";
    case "run.failed":
      return "failed";
    case "run.interrupted":
      return "interrupted";
    default:
      return current;
  }
}

function buildConversationItems(messages: SessionMessage[], events: TimelineEvent[]): ConversationItem[] {
  const messageItems: ConversationItem[] = messages.map((message, index) => ({
    key: `message-${message.id}`,
    kind: "message",
    createdAt: message.createdAt,
    message,
    order: index
  }));

  const activityItems: ConversationItem[] = events
    .map((event, index) => {
      const activity = summarizeActivity(event);
      if (!activity) {
        return null;
      }

      return {
        key: `activity-${event.sessionId}-${event.seq}`,
        kind: "activity" as const,
        createdAt: event.createdAt,
        activity,
        order: messages.length + index
      };
    })
    .filter((item): item is ConversationItem => item !== null);

  return [...messageItems, ...activityItems].sort(compareConversationItems);
}

function summarizeActivity(event: TimelineEvent): ActivityCard | null {
  if (event.kind === "tool.call") {
    return summarizeToolCall(event);
  }

  if (event.kind === "run.completed") {
    const runtimeMs = readRuntimeMs(event.payload);
    return {
      title: "Completed this run",
      summary: runtimeMs ? `Finished in ${formatRuntime(runtimeMs)}.` : "Claw finished this task.",
      details: runtimeMs ? [`Runtime: ${formatRuntime(runtimeMs)}`] : ["The current run has reached a completed state."],
      tone: "success"
    };
  }

  if (event.kind === "run.failed") {
    const error = readNestedString(event.payload, ["error"]) ?? "The run reported an error.";
    return {
      title: "Run failed",
      summary: truncateSummary(error),
      details: [error],
      tone: "warning"
    };
  }

  if (event.kind === "run.interrupted") {
    return {
      title: "Run interrupted",
      summary: "The run stopped before reaching a completed state.",
      details: ["Claw reported that this run was interrupted."],
      tone: "warning"
    };
  }

  return null;
}

function summarizeToolCall(event: TimelineEvent): ActivityCard | null {
  const toolName = readNestedString(event.payload, ["data", "name"]) ?? readNestedString(event.payload, ["name"]);
  if (!toolName) {
    return null;
  }

  const path = readNestedString(event.payload, ["data", "args", "path"]);
  const skillName = extractSkillName(path);
  if (skillName) {
    return {
      title: `Inspected skill ${skillName}`,
      summary: "Claw opened a local skill guide before continuing.",
      details: path ? [`Path: ${path}`] : ["A skill instruction file was inspected."],
      tone: "neutral"
    };
  }

  const args = readNestedObject(event.payload, ["data", "args"]);
  const argumentSummary = summarizeArguments(args);
  return {
    title: `Used tool ${toolName}`,
    summary: argumentSummary ? `Arguments: ${argumentSummary}` : "Claw invoked this tool during the run.",
    details: argumentSummary ? [argumentSummary] : ["No user-facing arguments were captured for this tool call."],
    tone: "neutral"
  };
}

function compareConversationItems(left: ConversationItem, right: ConversationItem): number {
  const timeDelta = new Date(left.createdAt).getTime() - new Date(right.createdAt).getTime();
  if (timeDelta !== 0) {
    return timeDelta;
  }
  return left.order - right.order;
}

function readNestedString(value: unknown, path: string[]): string | undefined {
  let current = value;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return typeof current === "string" && current.trim().length > 0 ? current.trim() : undefined;
}

function readNestedObject(value: unknown, path: string[]): Record<string, unknown> | undefined {
  let current = value;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current && typeof current === "object" ? (current as Record<string, unknown>) : undefined;
}

function extractSkillName(path?: string): string | null {
  if (!path) {
    return null;
  }

  const match = path.match(/\/skills\/([^/]+)\/SKILL\.md$/i);
  return match?.[1] ?? null;
}

function summarizeArguments(args?: Record<string, unknown>): string {
  if (!args) {
    return "";
  }

  return Object.entries(args)
    .slice(0, 3)
    .map(([key, value]) => `${key}=${stringifyArgumentValue(value)}`)
    .join(", ");
}

function stringifyArgumentValue(value: unknown): string {
  if (typeof value === "string") {
    return value.length > 48 ? `${value.slice(0, 48)}…` : value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return Array.isArray(value) ? `[${value.length} items]` : "{…}";
}

function readRuntimeMs(payload: Record<string, unknown>): number | null {
  const direct = payload.runtimeMs;
  if (typeof direct === "number" && Number.isFinite(direct) && direct >= 0) {
    return direct;
  }

  const sessionRuntime = payload.session;
  if (sessionRuntime && typeof sessionRuntime === "object") {
    const nested = (sessionRuntime as Record<string, unknown>).runtimeMs;
    if (typeof nested === "number" && Number.isFinite(nested) && nested >= 0) {
      return nested;
    }
  }

  return null;
}

function formatRuntime(runtimeMs: number): string {
  if (runtimeMs >= 1000) {
    return `${(runtimeMs / 1000).toFixed(runtimeMs >= 10_000 ? 0 : 1)}s`;
  }
  return `${runtimeMs}ms`;
}

function truncateSummary(value: string, limit = 120): string {
  const normalized = value.trim();
  return normalized.length > limit ? `${normalized.slice(0, limit)}…` : normalized;
}

function formatUpdatedAt(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(date);
}

function safeParseJson<T>(value: string): T | null {
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}

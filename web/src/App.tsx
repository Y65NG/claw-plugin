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
      eventItem: EventListItem;
      order: number;
    };

type ActivityCard = {
  kind?: string;
  title: string;
  summary: string;
  details: string[];
  tone: "neutral" | "success" | "warning";
  tool?: ToolDisplay;
};

type EventListItem = {
  key: string;
  seq: number;
  kind: string;
  title: string;
  summary: string;
  detail: string;
  createdAt: string;
  tone: "neutral" | "success" | "warning";
  tool?: ToolDisplay;
};

type ToolDisplay = {
  name: string;
  displayName: string;
  meta?: string;
  input?: unknown;
  output?: unknown;
  isError: boolean;
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
  const eventListItems = useMemo(() => buildEventListItems(events), [events]);

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
      if (payload.kind === "assistant.delta") {
        const content = String((payload.payload as { content?: string }).content ?? "");
        if (content) {
          const mode = readDeltaMode(payload.payload);
          setMessages((current) =>
            upsertStreamingAssistantMessage(current, activeSessionId, content, payload.createdAt, mode)
          );
        }
      }
      if (payload.kind === "assistant.message") {
        const content = String((payload.payload as { content?: string }).content ?? "");
        if (content) {
          setMessages((current) =>
            current.some((message) => message.id === `assistant-${payload.seq}`)
              ? current
              : [
                  ...current.filter((message) => message.id !== streamingAssistantMessageId(activeSessionId)),
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
                  <details
                    key={item.key}
                    className={`conversation-event-row event-row tone-${item.eventItem.tone} ${eventKindClass(item.eventItem.kind)}`}
                  >
                <summary>
                  <span className="event-main">
                    <span className="event-title">{item.eventItem.title}</span>
                    <span className="event-summary">{item.eventItem.summary}</span>
                  </span>
                  <span className="event-meta">
                    <span>#{item.eventItem.seq}</span>
                    <time>{formatUpdatedAt(item.eventItem.createdAt)}</time>
                  </span>
                </summary>
                <EventDetail item={item.eventItem} />
              </details>
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

      <aside className="panel events-panel" aria-label="Conversation event list">
        <div className="panel-header">
          <div>
            <h3>Event list</h3>
            <p className="panel-subtitle">Current conversation activity stream</p>
          </div>
          <span className="event-count">{events.length}</span>
        </div>
        <div className="events-scroll">
          <section className="event-list-card">
            <div className="event-list">
              {eventListItems.length ? (
                eventListItems.map((item) => (
                  <details key={item.key} className={`event-row tone-${item.tone} ${eventKindClass(item.kind)}`}>
                    <summary>
                      <span className="event-main">
                        <span className="event-title">{item.title}</span>
                        <span className="event-summary">{item.summary}</span>
                      </span>
                      <span className="event-meta">
                        <span>#{item.seq}</span>
                        <time>{formatUpdatedAt(item.createdAt)}</time>
                      </span>
                    </summary>
                    <EventDetail item={item} />
                  </details>
                ))
              ) : (
                <div className="event-empty">No events captured for this session yet.</div>
              )}
            </div>
          </section>
        </div>
      </aside>

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
            <div className="stat-card stat-card-wide">
              <span className="stat-label">53AIHub</span>
              <span className="stat-value stat-value-compact">
                {status?.hub53ai?.enabled
                  ? `${status.hub53ai.connectionStatus}${status.hub53ai.botId ? ` · ${status.hub53ai.botId}` : ""}`
                  : "Disabled"}
              </span>
            </div>
            <div className="stat-card stat-card-wide">
              <span className="stat-label">53AIHub traffic</span>
              <span className="stat-value stat-value-compact">
                {status?.hub53ai
                  ? `In ${status.hub53ai.receivedMessageCount} · Out ${status.hub53ai.sentMessageCount} · Pending ${status.hub53ai.pendingOutboundCount}`
                  : "No bridge status"}
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

function EventDetail({ item }: { item: EventListItem }) {
  if (item.tool) {
    return (
      <div className="tool-detail-card">
        <div className="tool-detail-heading">
          <span className="tool-detail-name">{item.tool.displayName}</span>
          {item.tool.isError ? <span className="tool-detail-error">error</span> : null}
        </div>
        {item.tool.meta ? <div className="tool-detail-meta">with {item.tool.meta}</div> : null}
        {item.tool.input !== undefined ? (
          <section className="tool-detail-section">
            <h4>TOOL INPUT</h4>
            <pre className="tool-detail-code">{formatToolValue(item.tool.input)}</pre>
          </section>
        ) : null}
        {item.tool.output !== undefined ? (
          <section className="tool-detail-section">
            <h4>TOOL OUTPUT</h4>
            <pre className="tool-detail-code">{formatToolValue(item.tool.output)}</pre>
          </section>
        ) : null}
        {item.tool.input === undefined && item.tool.output === undefined ? (
          <pre className="tool-detail-code">{item.detail}</pre>
        ) : null}
      </div>
    );
  }

  return <pre>{item.detail}</pre>;
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

function streamingAssistantMessageId(sessionId: string): string {
  return `assistant-stream-${sessionId}`;
}

function upsertStreamingAssistantMessage(
  messages: SessionMessage[],
  sessionId: string,
  content: string,
  createdAt: string,
  mode: "append" | "replace" | "auto" = "auto"
): SessionMessage[] {
  const id = streamingAssistantMessageId(sessionId);
  const existing = messages.find((message) => message.id === id);
  if (!content.trim()) {
    return messages;
  }

  if (existing) {
    const nextContent = mergeStreamingContent(existing.content, content, mode);
    if (existing.content === nextContent) {
      return messages;
    }
    return messages.map((message) =>
      message.id === id ? { ...message, content: nextContent, createdAt } : message
    );
  }

  return [
    ...messages,
    {
      id,
      sessionId,
      role: "assistant",
      content,
      createdAt
    }
  ];
}

function readDeltaMode(payload: Record<string, unknown>): "append" | "replace" | "auto" {
  if (payload.mode === "append" || payload.mode === "replace") {
    return payload.mode;
  }
  if (payload.replace === true) {
    return "replace";
  }
  return "auto";
}

function mergeStreamingContent(
  existing: string,
  incoming: string,
  mode: "append" | "replace" | "auto"
): string {
  if (mode === "replace") {
    return incoming;
  }

  if (mode === "append") {
    if (incoming === existing || existing.endsWith(incoming)) {
      return existing;
    }
    if (incoming.startsWith(existing)) {
      return incoming;
    }
    return `${existing}${incoming}`;
  }

  if (incoming === existing || existing.startsWith(incoming)) {
    return existing;
  }
  if (incoming.startsWith(existing)) {
    return incoming;
  }
  return `${existing}${incoming}`;
}

function nextStatusForEvent(kind: string, current: SessionSummary["status"]): SessionSummary["status"] {
  switch (kind) {
    case "run.started":
    case "assistant.thinking":
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
  const visibleMessages = dedupeConversationMessages(messages);
  const messageItems: ConversationItem[] = visibleMessages.map((message, index) => ({
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
        eventItem: summarizeEventListItem(event),
        order: visibleMessages.length + index
      };
    })
    .filter((item): item is ConversationItem => item !== null);

  return [...messageItems, ...activityItems].sort(compareConversationItems);
}

function dedupeConversationMessages(messages: SessionMessage[]): SessionMessage[] {
  const result: SessionMessage[] = [];
  let assistantSinceLastUser: SessionMessage[] = [];
  const exactAssistantContent = new Set<string>();

  for (const message of messages) {
    if (message.role !== "assistant") {
      result.push(message);
      if (message.role === "user") {
        assistantSinceLastUser = [];
        exactAssistantContent.clear();
      }
      continue;
    }

    const normalized = normalizeContentForComparison(message.content);
    if (!normalized) {
      continue;
    }

    if (exactAssistantContent.has(normalized)) {
      continue;
    }

    const containsPreviousAnswer = assistantSinceLastUser.some((previous) => {
      const previousContent = normalizeContentForComparison(previous.content);
      return previousContent.length >= 10 && normalized.includes(previousContent) && normalized !== previousContent;
    });
    if (containsPreviousAnswer) {
      continue;
    }

    result.push(message);
    assistantSinceLastUser.push(message);
    exactAssistantContent.add(normalized);
  }

  return result;
}

function summarizeActivity(event: TimelineEvent): ActivityCard | null {
  if (event.kind === "assistant.thinking") {
    const content = normalizeReasoningForDisplay(
      readNestedString(event.payload, ["content"]) ??
        "Claw emitted a thinking update. Raw reasoning is not displayed."
    );
    const textLength = readNestedNumber(event.payload, ["textLength"]);
    const rawThinkingVisible = readNestedBoolean(event.payload, ["rawThinkingVisible"]);
    if (rawThinkingVisible) {
      return {
        title: "Model reasoning",
        summary: truncateSummary(content),
        details: [
          content,
          ...(textLength ? [`Reasoning length: ${textLength} chars`] : [])
        ],
        tone: "neutral"
      };
    }
    return {
      title: "Thinking update",
      summary: truncateSummary(content),
      details: [
        "Private chain-of-thought is intentionally hidden; this card only marks that Claw produced a thinking stream.",
        ...(textLength ? [`Hidden thinking length: ${textLength} chars`] : [])
      ],
      tone: "neutral"
    };
  }

  if (event.kind === "tool.result" || (event.kind === "tool.call" && isToolResultLike(event))) {
    return summarizeToolResult(event);
  }

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

function buildEventListItems(events: TimelineEvent[]): EventListItem[] {
  return events.slice(-120).map((event) => summarizeEventListItem(event));
}

function summarizeEventListItem(event: TimelineEvent): EventListItem {
  const activity = summarizeActivity(event);
  if (activity) {
    return {
      key: `event-${event.sessionId}-${event.seq}`,
      seq: event.seq,
      kind: activity.kind ?? event.kind,
      title: activity.title,
      summary: activity.summary,
      detail: activity.details.join("\n") || formatEventPayload(event.payload),
      createdAt: event.createdAt,
      tone: activity.tone,
      tool: activity.tool
    };
  }

  if (event.kind === "assistant.delta") {
    const content = readNestedString(event.payload, ["content"]) ?? "";
    return {
      key: `event-${event.sessionId}-${event.seq}`,
      seq: event.seq,
      kind: event.kind,
      title: "Streaming output",
      summary: content ? truncateSummary(content, 96) : "Assistant streamed an empty delta.",
      detail: content || formatEventPayload(event.payload),
      createdAt: event.createdAt,
      tone: "neutral"
    };
  }

  if (event.kind === "assistant.thinking") {
    const content = normalizeReasoningForDisplay(
      readNestedString(event.payload, ["content"]) ??
        "Claw emitted a thinking update. Raw reasoning is not displayed."
    );
    const textLength = readNestedNumber(event.payload, ["textLength"]);
    const rawThinkingVisible = readNestedBoolean(event.payload, ["rawThinkingVisible"]);
    return {
      key: `event-${event.sessionId}-${event.seq}`,
      seq: event.seq,
      kind: event.kind,
      title: rawThinkingVisible ? "Model reasoning" : "Thinking update",
      summary: rawThinkingVisible
        ? truncateSummary(content, 96)
        : textLength
          ? `${content} (${textLength} hidden chars)`
          : content,
      detail: rawThinkingVisible ? content : formatEventPayload(event.payload),
      createdAt: event.createdAt,
      tone: "neutral"
    };
  }

  if (event.kind === "assistant.message") {
    const content = readNestedString(event.payload, ["content"]) ?? "";
    return {
      key: `event-${event.sessionId}-${event.seq}`,
      seq: event.seq,
      kind: event.kind,
      title: "Assistant message",
      summary: content ? truncateSummary(content, 96) : "Assistant emitted a final message event.",
      detail: content || formatEventPayload(event.payload),
      createdAt: event.createdAt,
      tone: "success"
    };
  }

  if (event.kind === "tool.result") {
    const tool = buildToolDisplay(event);
    return {
      key: `event-${event.sessionId}-${event.seq}`,
      seq: event.seq,
      kind: event.kind,
      title: "Tool output",
      summary: tool?.displayName ?? "Tool result",
      detail: tool ? formatToolDetail(tool, true).join("\n") : formatEventPayload(event.payload),
      createdAt: event.createdAt,
      tone: tool?.isError ? "warning" : "success",
      tool
    };
  }

  if (event.kind === "status.update") {
    const status =
      readNestedString(event.payload, ["status"]) ??
      readNestedString(event.payload, ["state"]) ??
      readNestedString(event.payload, ["message"]);
    return {
      key: `event-${event.sessionId}-${event.seq}`,
      seq: event.seq,
      kind: event.kind,
      title: "Status update",
      summary: status ? truncateSummary(status, 96) : "Claw reported a status update.",
      detail: formatEventPayload(event.payload),
      createdAt: event.createdAt,
      tone: "neutral"
    };
  }

  if (event.kind === "stderr.line") {
    const line = readNestedString(event.payload, ["line"]) ?? readNestedString(event.payload, ["message"]);
    return {
      key: `event-${event.sessionId}-${event.seq}`,
      seq: event.seq,
      kind: event.kind,
      title: "stderr",
      summary: line ? truncateSummary(line, 96) : "Claw wrote to stderr.",
      detail: line || formatEventPayload(event.payload),
      createdAt: event.createdAt,
      tone: "warning"
    };
  }

  if (event.kind === "run.started") {
    return {
      key: `event-${event.sessionId}-${event.seq}`,
      seq: event.seq,
      kind: event.kind,
      title: "Run started",
      summary: "Claw started processing this request.",
      detail: formatEventPayload(event.payload),
      createdAt: event.createdAt,
      tone: "neutral"
    };
  }

  const content =
    readNestedString(event.payload, ["content"]) ??
    readNestedString(event.payload, ["message"]) ??
    readNestedString(event.payload, ["text"]);
  return {
    key: `event-${event.sessionId}-${event.seq}`,
    seq: event.seq,
    kind: event.kind,
    title: event.kind,
    summary: content ? truncateSummary(content, 96) : "Raw timeline event.",
    detail: content || formatEventPayload(event.payload),
    createdAt: event.createdAt,
    tone: "neutral"
  };
}

function summarizeToolCall(event: TimelineEvent): ActivityCard | null {
  const tool = buildToolDisplay(event);
  if (!tool) {
    return null;
  }

  const toolName = tool.name;
  const path =
    readNestedString(event.payload, ["data", "args", "path"]) ??
    readStringFromUnknown(tool.input, ["path"]);
  const skillName = extractSkillName(path);
  if (skillName) {
    return {
      title: `Inspected skill ${skillName}`,
      summary: "Claw opened a local skill guide before continuing.",
      details: path ? [`Path: ${path}`] : ["A skill instruction file was inspected."],
      tone: "neutral"
    };
  }

  return {
    kind: "tool.call",
    title: tool.meta ?? formatToolCallTitle(toolName, asRecord(tool.input)) ?? `Used ${tool.displayName}`,
    summary: tool.displayName,
    details: formatToolDetail(tool, event.kind === "tool.result"),
    tone: tool.isError ? "warning" : "neutral",
    tool
  };
}

function summarizeToolResult(event: TimelineEvent): ActivityCard | null {
  const tool = buildToolDisplay(event);
  if (!tool) {
    return null;
  }

  return {
    kind: "tool.result",
    title: "Tool output",
    summary: tool.displayName,
    details: formatToolDetail(tool, true),
    tone: tool.isError ? "warning" : "success",
    tool
  };
}

function compareConversationItems(left: ConversationItem, right: ConversationItem): number {
  const timeDelta = new Date(left.createdAt).getTime() - new Date(right.createdAt).getTime();
  if (timeDelta !== 0) {
    return timeDelta;
  }
  return left.order - right.order;
}

function eventKindClass(kind: string): string {
  return `event-kind-${kind.replace(/[^a-z0-9]+/gi, "-").replace(/^-|-$/g, "").toLowerCase()}`;
}

function formatToolCallTitle(toolName: string, args?: Record<string, unknown>): string | null {
  if (!args) {
    return null;
  }

  if (toolName === "web_search") {
    const query = readStringFromRecord(args, ["query", "q", "search", "text"]);
    const top = readNumberFromRecord(args, ["top", "topK", "limit", "count"]);
    return query ? `for "${query}"${top ? ` (top ${top})` : ""}` : null;
  }

  if (toolName === "web_fetch") {
    const url = readStringFromRecord(args, ["url", "href"]);
    const maxChars = readNumberFromRecord(args, ["maxChars", "max_chars", "limit"]);
    return url ? `from ${url}${maxChars ? ` (max ${maxChars} chars)` : ""}` : null;
  }

  return null;
}

function buildToolDisplay(event: TimelineEvent): ToolDisplay | undefined {
  const toolName =
    readNestedString(event.payload, ["data", "name"]) ??
    readNestedString(event.payload, ["name"]) ??
    readNestedString(event.payload, ["tool"]) ??
    readNestedString(event.payload, ["toolName"]);
  if (!toolName) {
    return undefined;
  }

  const meta =
    readNestedString(event.payload, ["data", "meta"]) ??
    readNestedString(event.payload, ["meta"]) ??
    readNestedString(event.payload, ["title"]);
  const rawInput =
    readNestedValue(event.payload, ["data", "args"]) ??
    readNestedValue(event.payload, ["data", "arguments"]) ??
    readNestedValue(event.payload, ["data", "input"]) ??
    readNestedValue(event.payload, ["args"]) ??
    readNestedValue(event.payload, ["arguments"]) ??
    readNestedValue(event.payload, ["input"]);
  const rawOutput =
    readNestedValue(event.payload, ["data", "result", "details"]) ??
    readNestedValue(event.payload, ["data", "result", "output"]) ??
    readNestedValue(event.payload, ["data", "result", "content"]) ??
    readNestedValue(event.payload, ["data", "result"]) ??
    readNestedValue(event.payload, ["result"]) ??
    readNestedValue(event.payload, ["output"]) ??
    readNestedValue(event.payload, ["content"]);
  const output = normalizeToolValue(rawOutput);
  const inferredInput =
    normalizeToolValue(rawInput) ??
    inferToolInputFromMeta(toolName, meta) ??
    inferToolInputFromOutput(toolName, output);
  const isError =
    readNestedBoolean(event.payload, ["data", "isError"]) ||
    readNestedString(event.payload, ["data", "result", "details", "status"]) === "error" ||
    readNestedString(event.payload, ["result", "status"]) === "error";

  return {
    name: toolName,
    displayName: formatToolName(toolName),
    meta,
    input: inferredInput,
    output,
    isError
  };
}

function isToolResultLike(event: TimelineEvent): boolean {
  const phase =
    readNestedString(event.payload, ["phase"]) ??
    readNestedString(event.payload, ["data", "phase"]);
  return phase ? ["result", "done", "output"].includes(phase) : false;
}

function inferToolInputFromMeta(toolName: string, meta?: string): unknown {
  if (!meta) {
    return undefined;
  }

  const searchMatch = meta.match(/^for\s+"(.+)"(?:\s+\(top\s+(\d+)\))?/i);
  if (searchMatch) {
    return {
      query: searchMatch[1],
      ...(searchMatch[2] ? { count: Number(searchMatch[2]) } : {})
    };
  }

  const fetchMatch = meta.match(/^from\s+(\S+)(?:\s+\(max\s+(\d+)\s+chars\))?/i);
  if (fetchMatch) {
    return {
      url: fetchMatch[1],
      ...(fetchMatch[2] ? { maxChars: Number(fetchMatch[2]) } : {})
    };
  }

  if (toolName === "exec" || toolName === "shell") {
    return { command: meta };
  }

  return undefined;
}

function inferToolInputFromOutput(toolName: string, output: unknown): unknown {
  const record = asRecord(output);
  if (!record) {
    return undefined;
  }

  if (toolName === "web_search") {
    const query = readStringFromRecord(record, ["query"]);
    const count = readNumberFromRecord(record, ["count", "limit", "top"]);
    if (query) {
      return {
        query,
        ...(count ? { count } : {})
      };
    }
  }

  if (toolName === "web_fetch") {
    const url = readStringFromRecord(record, ["url", "href"]);
    const maxChars = readNumberFromRecord(record, ["maxChars", "max_chars", "limit"]);
    if (url) {
      return {
        url,
        ...(maxChars ? { maxChars } : {})
      };
    }
  }

  return undefined;
}

function formatToolDetail(tool: ToolDisplay, includeOutput: boolean): string[] {
  const lines = [tool.displayName];
  if (tool.meta) {
    lines.push(`with ${tool.meta}`);
  }
  if (tool.input !== undefined) {
    lines.push("TOOL INPUT", formatToolValue(tool.input));
  }
  if (includeOutput && tool.output !== undefined) {
    lines.push("TOOL OUTPUT", formatToolValue(tool.output));
  }
  return lines;
}

function formatToolName(toolName: string): string {
  return toolName
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function normalizeToolValue(value: unknown): unknown {
  if (value === undefined || value === null) {
    return undefined;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return undefined;
    }
    return parseJsonishValue(trimmed) ?? trimmed;
  }

  if (Array.isArray(value)) {
    const textParts = value
      .flatMap((entry) => {
        if (!entry || typeof entry !== "object") {
          return [];
        }
        const record = entry as Record<string, unknown>;
        const text = record.text ?? record.content ?? record.output;
        return typeof text === "string" && text.trim() ? [text] : [];
      });
    if (textParts.length > 0) {
      const joined = textParts.join("\n");
      return parseJsonishValue(joined) ?? joined;
    }
    return value;
  }

  return value;
}

function parseJsonishValue(value: string): unknown {
  if (!/^[\[{]/.test(value)) {
    return undefined;
  }
  try {
    return JSON.parse(value);
  } catch {
    return undefined;
  }
}

function formatToolValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : undefined;
}

function readStringFromRecord(record: Record<string, unknown>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function readStringFromUnknown(value: unknown, path: string[]): string | undefined {
  const current = readUnknownPath(value, path);
  return typeof current === "string" && current.trim() ? current.trim() : undefined;
}

function readUnknownPath(value: unknown, path: string[]): unknown {
  let current = value;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}

function readNumberFromRecord(record: Record<string, unknown>, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim() && Number.isFinite(Number(value))) {
      return Number(value);
    }
  }
  return undefined;
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

function normalizeContentForComparison(content: string): string {
  return content.replace(/\s+/g, " ").trim();
}

function normalizeReasoningForDisplay(content: string): string {
  return collapseRepeatedLeadingSegment(content);
}

function collapseRepeatedLeadingSegment(value: string): string {
  const leadingWhitespace = value.match(/^\s*/)?.[0] ?? "";
  const text = value.trimStart();
  const maxSegmentLength = Math.min(32, Math.floor(text.length / 2));

  for (let length = maxSegmentLength; length >= 2; length -= 1) {
    const segment = text.slice(0, length);
    if (!segment.trim() || !text.startsWith(`${segment}${segment}`)) {
      continue;
    }

    const remainder = text.slice(length * 2);
    const next = remainder[0] ?? "";
    const segmentLooksNatural =
      /\s/.test(segment) ||
      /^[A-Z][a-z]+$/.test(segment) ||
      /[\u4e00-\u9fff]/.test(segment);
    const hasBoundary = !next || /\s|[,.!?;:，。！？；：]/.test(next) || /[\u4e00-\u9fff]/.test(next);

    if (segmentLooksNatural && hasBoundary) {
      return `${leadingWhitespace}${segment}${remainder}`;
    }
  }

  return value;
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

function readNestedValue(value: unknown, path: string[]): unknown {
  let current = value;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}

function readNestedNumber(value: unknown, path: string[]): number | undefined {
  let current = value;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return typeof current === "number" && Number.isFinite(current) ? current : undefined;
}

function readNestedBoolean(value: unknown, path: string[]): boolean {
  let current = value;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return false;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current === true;
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

function formatEventPayload(payload: Record<string, unknown>): string {
  try {
    return JSON.stringify(payload, null, 2);
  } catch {
    return String(payload);
  }
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

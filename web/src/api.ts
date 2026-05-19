import type {
  BootstrapPayload,
  SessionDetail,
  SessionSummary,
  TimelineEvent
} from "./types";

export async function fetchBootstrap(): Promise<BootstrapPayload> {
  return fetchJson<BootstrapPayload>("/api/bootstrap");
}

export async function fetchSessions(): Promise<SessionSummary[]> {
  const payload = await fetchJson<{ sessions: SessionSummary[] }>("/api/sessions");
  return payload.sessions ?? [];
}

export async function fetchSessionDetail(sessionId: string): Promise<SessionDetail> {
  return fetchJson<SessionDetail>(`/api/sessions/${sessionId}`);
}

export async function fetchSessionEvents(sessionId: string, afterSeq = 0): Promise<TimelineEvent[]> {
  const payload = await fetchJson<{ events: TimelineEvent[] }>(
    `/api/sessions/${sessionId}/events?afterSeq=${afterSeq}`
  );
  return payload.events ?? [];
}

export async function createSession(
  token: string,
  title: string,
  initialPrompt = ""
): Promise<SessionSummary> {
  return fetchJson<SessionSummary>("/api/sessions", {
    method: "POST",
    headers: buildAuthHeaders(token),
    body: JSON.stringify({ title, initialPrompt })
  });
}

export async function sendMessage(token: string, sessionId: string, content: string): Promise<void> {
  await fetchJson(`/api/sessions/${sessionId}/messages`, {
    method: "POST",
    headers: buildAuthHeaders(token),
    body: JSON.stringify({ content })
  });
}

function buildAuthHeaders(token: string): HeadersInit {
  return token
    ? {
        "Content-Type": "application/json",
        "X-Plugin-Token": token
      }
    : {
        "Content-Type": "application/json"
      };
}

async function fetchJson<T>(input: RequestInfo | URL, init?: RequestInit): Promise<T> {
  const response = await fetch(input, init);
  if (!response.ok) {
    let message = `Request failed: ${response.status}`;
    try {
      const payload = (await response.json()) as { error?: string };
      if (payload.error) {
        message = payload.error;
      }
    } catch {
      // ignore JSON parse failures on error paths
    }
    throw new Error(message);
  }

  return (await response.json()) as T;
}

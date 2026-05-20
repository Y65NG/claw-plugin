import { afterEach, beforeEach, vi } from "vitest";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { App } from "../src/App";

class MockWebSocket {
  static instances: MockWebSocket[] = [];
  onmessage: ((event: MessageEvent<string>) => void) | null = null;
  onopen: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  url: string;
  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
  }
  close() {
    this.onclose?.();
  }
}

describe("App", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn(() => new Promise(() => {})));
    vi.stubGlobal("WebSocket", MockWebSocket);
    MockWebSocket.instances = [];
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders the main work areas", () => {
    render(<App />);

    expect(screen.getByRole("navigation", { name: /sessions/i })).toBeInTheDocument();
    expect(screen.getByRole("main", { name: /conversation/i })).toBeInTheDocument();
    expect(screen.getByRole("complementary", { name: /conversation event list/i })).toBeInTheDocument();
    expect(screen.getByRole("complementary", { name: /status and claw details/i })).toBeInTheDocument();
  });

  it("allows creating a session even when bootstrap token is empty", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            token: "",
            status: {
              hostKind: "openclaw",
              activeSessionCount: 0,
              runningSessionCount: 0,
              healthy: true
            },
            config: {
              runner: {},
              config: {}
            }
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        )
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ sessions: [] }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        })
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            id: "session-1",
            title: "Session 1",
            status: "idle",
            hostKind: "openclaw",
            runnerCommand: "openclaw",
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            lastEventSeq: 1
          }),
          { status: 201, headers: { "Content-Type": "application/json" } }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            session: {
              id: "session-1",
              title: "Session 1",
              status: "idle",
              hostKind: "openclaw",
              runnerCommand: "openclaw",
              createdAt: new Date().toISOString(),
              updatedAt: new Date().toISOString(),
              lastEventSeq: 1
            },
            messages: []
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        )
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ events: [] }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        })
      );

    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(<App />);

    await waitFor(() => {
      expect(screen.getByText(/no sessions yet/i)).toBeInTheDocument();
    });

    await user.click(screen.getByRole("button", { name: /^new$/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/sessions",
        expect.objectContaining({
          method: "POST"
        })
      );
    });
  });

  it("keeps sessions in default recency order instead of pinning running sessions to the top", async () => {
    const now = new Date().toISOString();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(
        jsonResponse({
          token: "local-token",
          status: {
            hostKind: "qclaw",
            activeSessionCount: 2,
            runningSessionCount: 1,
            healthy: true
          },
          config: {
            gateway: {},
            config: {}
          }
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          sessions: [
            {
              id: "session-completed",
              title: "Completed session",
              status: "completed",
              hostKind: "qclaw",
              runnerCommand: "gateway",
              createdAt: now,
              updatedAt: now,
              lastEventSeq: 1
            },
            {
              id: "session-running",
              title: "Running session",
              status: "running",
              hostKind: "qclaw",
              runnerCommand: "gateway",
              createdAt: now,
              updatedAt: now,
              lastEventSeq: 2
            }
          ]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          session: {
            id: "session-running",
            title: "Running session",
            status: "running",
            hostKind: "qclaw",
            runnerCommand: "gateway",
            createdAt: now,
            updatedAt: now,
            lastEventSeq: 2
          },
          messages: []
        })
      )
      .mockResolvedValueOnce(jsonResponse({ events: [] }));

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(() => {
      const sessionButtons = screen.getAllByRole("button");
      expect(sessionButtons.some((button) => button.textContent?.includes("Running session"))).toBe(true);
      expect(sessionButtons.some((button) => button.textContent?.includes("Completed session"))).toBe(true);
    });

    const sessionButtons = screen.getAllByRole("button");
    const runningIndex = sessionButtons.findIndex((button) => button.textContent?.includes("Running session"));
    const completedIndex = sessionButtons.findIndex((button) => button.textContent?.includes("Completed session"));
    expect(runningIndex).toBeGreaterThan(-1);
    expect(completedIndex).toBeGreaterThan(-1);
    expect(completedIndex).toBeLessThan(runningIndex);
  });

  it("opens the most recently updated session by default instead of auto-entering an older running session", async () => {
    const oldTime = "2026-05-18T08:43:31.730Z";
    const newTime = "2026-05-19T05:48:04.009Z";
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(
        jsonResponse({
          token: "local-token",
          status: {
            hostKind: "qclaw",
            activeSessionCount: 2,
            runningSessionCount: 1,
            healthy: true
          },
          config: {
            gateway: {},
            config: {}
          }
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          sessions: [
            {
              id: "session-running-old",
              title: "Old running session",
              status: "running",
              hostKind: "qclaw",
              runnerCommand: "gateway",
              createdAt: oldTime,
              updatedAt: oldTime,
              lastEventSeq: 99
            },
            {
              id: "session-new",
              title: "Fresh session",
              status: "idle",
              hostKind: "qclaw",
              runnerCommand: "gateway",
              createdAt: newTime,
              updatedAt: newTime,
              lastEventSeq: 0
            }
          ]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          session: {
            id: "session-new",
            title: "Fresh session",
            status: "idle",
            hostKind: "qclaw",
            runnerCommand: "gateway",
            createdAt: newTime,
            updatedAt: newTime,
            lastEventSeq: 0
          },
          messages: []
        })
      )
      .mockResolvedValueOnce(jsonResponse({ events: [] }));

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: "Fresh session" })).toBeInTheDocument();
    });
  });

  it("ignores malformed websocket payloads without crashing", async () => {
    const now = new Date().toISOString();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(
        jsonResponse({
          token: "local-token",
          status: {
            hostKind: "qclaw",
            activeSessionCount: 1,
            runningSessionCount: 0,
            healthy: true
          },
          config: {
            gateway: {},
            config: {}
          }
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          sessions: [
            {
              id: "session-1",
              title: "Stable session",
              status: "completed",
              hostKind: "qclaw",
              runnerCommand: "gateway",
              createdAt: now,
              updatedAt: now,
              lastEventSeq: 1
            }
          ]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          session: {
            id: "session-1",
            title: "Stable session",
            status: "completed",
            hostKind: "qclaw",
            runnerCommand: "gateway",
            createdAt: now,
            updatedAt: now,
            lastEventSeq: 1
          },
          messages: []
        })
      )
      .mockResolvedValueOnce(jsonResponse({ events: [] }));

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: "Stable session" })).toBeInTheDocument();
    });

    const sessionSocket = MockWebSocket.instances.find((socket) => socket.url.includes("/ws/sessions/session-1"));
    sessionSocket?.onmessage?.({ data: "{bad json" } as MessageEvent<string>);

    expect(screen.getByRole("heading", { name: "Stable session" })).toBeInTheDocument();
  });

  it("appends websocket assistant delta chunks into one visible streaming reply", async () => {
    const now = new Date().toISOString();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(
        jsonResponse({
          token: "local-token",
          status: {
            hostKind: "qclaw",
            activeSessionCount: 1,
            runningSessionCount: 1,
            healthy: true
          },
          config: {
            gateway: {},
            config: {}
          }
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          sessions: [
            {
              id: "session-1",
              title: "Streaming session",
              status: "running",
              hostKind: "qclaw",
              runnerCommand: "gateway",
              createdAt: now,
              updatedAt: now,
              lastEventSeq: 0
            }
          ]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          session: {
            id: "session-1",
            title: "Streaming session",
            status: "running",
            hostKind: "qclaw",
            runnerCommand: "gateway",
            createdAt: now,
            updatedAt: now,
            lastEventSeq: 0
          },
          messages: []
        })
      )
      .mockResolvedValueOnce(jsonResponse({ events: [] }));

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(() => {
      expect(screen.getByRole("heading", { name: "Streaming session" })).toBeInTheDocument();
    });

    const sessionSocket = MockWebSocket.instances.find((socket) => socket.url.includes("/ws/sessions/session-1"));
    expect(sessionSocket).toBeDefined();

    act(() => {
      sessionSocket?.onmessage?.({
        data: JSON.stringify({
          id: "evt-1",
          sessionId: "session-1",
          seq: 1,
          kind: "assistant.delta",
          payload: { content: "Hel", mode: "append" },
          createdAt: now
        })
      } as MessageEvent<string>);
      sessionSocket?.onmessage?.({
        data: JSON.stringify({
          id: "evt-2",
          sessionId: "session-1",
          seq: 2,
          kind: "assistant.delta",
          payload: { content: "lo", mode: "append" },
          createdAt: now
        })
      } as MessageEvent<string>);
    });

    await waitFor(() => {
      const conversation = within(screen.getByRole("main", { name: /conversation/i }));
      expect(conversation.getByText("Hello")).toBeInTheDocument();
      expect(conversation.queryByText("lo")).not.toBeInTheDocument();
    });
  });

  it("hides assistant messages that repeat an already displayed final answer with a reasoning prefix", async () => {
    const now = new Date().toISOString();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(
        jsonResponse({
          token: "local-token",
          status: {
            hostKind: "openclaw",
            activeSessionCount: 1,
            runningSessionCount: 0,
            healthy: true
          },
          config: {
            gateway: {},
            config: {}
          }
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          sessions: [
            {
              id: "session-1",
              title: "Dedup session",
              status: "completed",
              hostKind: "openclaw",
              runnerCommand: "gateway",
              createdAt: now,
              updatedAt: now,
              lastEventSeq: 1
            }
          ]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          session: {
            id: "session-1",
            title: "Dedup session",
            status: "completed",
            hostKind: "openclaw",
            runnerCommand: "gateway",
            createdAt: now,
            updatedAt: now,
            lastEventSeq: 1
          },
          messages: [
            {
              id: "user-1",
              sessionId: "session-1",
              role: "user",
              content: "Summarize books",
              createdAt: now
            },
            {
              id: "assistant-1",
              sessionId: "session-1",
              role: "assistant",
              content: "Final visible answer.",
              createdAt: now
            },
            {
              id: "assistant-2",
              sessionId: "session-1",
              role: "assistant",
              content: "Reasoning prefix that should not render. Final visible answer.",
              createdAt: now
            }
          ]
        })
      )
      .mockResolvedValueOnce(jsonResponse({ events: [] }));

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(() => {
      const conversation = within(screen.getByRole("main", { name: /conversation/i }));
      expect(conversation.getByText("Final visible answer.")).toBeInTheDocument();
      expect(conversation.queryByText("Reasoning prefix that should not render. Final visible answer.")).not.toBeInTheDocument();
    });
  });

  it("renders collapsed activity cards in the conversation and shows model plus enabled skills in status", async () => {
    const now = new Date().toISOString();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(
        jsonResponse({
          token: "local-token",
          status: {
            hostKind: "qclaw",
            activeSessionCount: 1,
            runningSessionCount: 0,
            healthy: true,
            modelPrimary: "qclaw/modelroute",
            enabledSkills: ["browser", "online-search"]
          },
          config: {
            gateway: {},
            config: {}
          }
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          sessions: [
            {
              id: "session-1",
              title: "Stable session",
              status: "completed",
              hostKind: "qclaw",
              runnerCommand: "gateway",
              createdAt: now,
              updatedAt: now,
              lastEventSeq: 16
            }
          ]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          session: {
            id: "session-1",
            title: "Stable session",
            status: "completed",
            hostKind: "qclaw",
            runnerCommand: "gateway",
            createdAt: now,
            updatedAt: now,
            lastEventSeq: 16
          },
          messages: [
            {
              id: "user-1",
              sessionId: "session-1",
              role: "user",
              content: "Check the weather skill",
              createdAt: now
            },
            {
              id: "assistant-2",
              sessionId: "session-1",
              role: "assistant",
              content: "I inspected the skill and used the weather tool.",
              createdAt: now
            }
          ]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          events: [
            {
              id: "evt-1",
              sessionId: "session-1",
              seq: 10,
              kind: "run.started",
              payload: {
                runId: "run-1"
              },
              createdAt: now
            },
            {
              id: "evt-2",
              sessionId: "session-1",
              seq: 11,
              kind: "tool.call",
              payload: {
                data: {
                  phase: "start",
                  name: "read",
                  args: {
                    path: "~/Library/Application Support/QClaw/openclaw/node_modules/openclaw/skills/weather/SKILL.md"
                  }
                }
              },
              createdAt: now
            },
            {
              id: "evt-3",
              sessionId: "session-1",
              seq: 12,
              kind: "tool.call",
              payload: {
                data: {
                  phase: "start",
                  name: "weather",
                  args: {
                    city: "Shanghai"
                  }
                }
              },
              createdAt: now
            },
            {
              id: "evt-4",
              sessionId: "session-1",
              seq: 13,
              kind: "assistant.thinking",
              payload: {
                content: "I need to inspect the weather skill, then call the weather tool.",
                privateContentOmitted: false,
                rawThinkingVisible: true,
                textLength: 42
              },
              createdAt: now
            }
          ]
        })
      );

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(() => {
      const conversation = within(screen.getByRole("main", { name: /conversation/i }));
      const conversationElement = screen.getByRole("main", { name: /conversation/i });
      const eventPanel = within(screen.getByRole("complementary", { name: /conversation event list/i }));
      const sidebar = within(screen.getByRole("complementary", { name: /status and claw details/i }));

      expect(screen.getByText("Model")).toBeInTheDocument();
      expect(screen.getByText("qclaw/modelroute")).toBeInTheDocument();
      expect(screen.getByText("Enabled skills")).toBeInTheDocument();
      expect(screen.getByText("browser, online-search")).toBeInTheDocument();
      expect(conversation.getByText("Inspected skill weather")).toBeInTheDocument();
      expect(conversation.getByText("Used tool weather")).toBeInTheDocument();
      expect(conversation.getByText("Model reasoning")).toBeInTheDocument();
      expect(conversation.getByText("I need to inspect the weather skill, then call the weather tool.")).toBeInTheDocument();
      expect(conversationElement.querySelectorAll(".conversation-event-row.event-row")).toHaveLength(3);
      expect(conversationElement.querySelector(".event-kind-assistant-thinking")).toBeTruthy();
      expect(conversationElement.querySelectorAll(".event-kind-tool-call")).toHaveLength(2);
      expect(conversation.queryByText("Show details")).not.toBeInTheDocument();
      expect(eventPanel.getByText("Event list")).toBeInTheDocument();
      expect(eventPanel.getByText("Run started")).toBeInTheDocument();
      expect(eventPanel.getByText("Inspected skill weather")).toBeInTheDocument();
      expect(eventPanel.getByText("Used tool weather")).toBeInTheDocument();
      expect(eventPanel.getByText("Model reasoning")).toBeInTheDocument();
      expect(sidebar.queryByText("Event list")).not.toBeInTheDocument();
    });
  });

});

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" }
  });
}

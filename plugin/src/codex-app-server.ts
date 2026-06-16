import { EventEmitter } from "node:events";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";

export type CodexAppServerNotification = {
  method: string;
  params?: Record<string, any>;
};

export type CodexTurnRunnerEvent = CodexAppServerNotification;

export type CodexRunTurnInput = {
  prompt: string;
  cwd: string;
  threadId?: string;
  conversationId: string;
  onThreadStarted?(event: { threadId: string; cwd: string }): void | Promise<void>;
  onTurnStarted?(event: { threadId: string; turnId: string }): void | Promise<void>;
  onEvent(event: CodexTurnRunnerEvent): void | Promise<void>;
};

export type CodexRunTurnResult = {
  threadId: string;
  turnId: string;
  status: "completed" | "interrupted" | "failed" | "inProgress" | string;
  finalText: string;
};

export type CodexTurnRunner = {
  runTurn(input: CodexRunTurnInput): Promise<CodexRunTurnResult>;
  interruptTurn?(threadId: string, turnId: string): Promise<void>;
  close?(): Promise<void>;
};

export type CodexAppServerClientOptions = {
  binPath: string;
  logger?: {
    warn?(message: string): void;
    error?(message: string): void;
  };
};

type PendingRequest = {
  resolve(value: unknown): void;
  reject(error: Error): void;
};

export class CodexAppServerClient extends EventEmitter {
  private nextId = 1;
  private pending = new Map<number, PendingRequest>();
  private closed = false;

  private constructor(
    private readonly proc: ChildProcessWithoutNullStreams,
    private readonly logger?: CodexAppServerClientOptions["logger"]
  ) {
    super();
    const lines = createInterface({ input: proc.stdout });
    lines.on("line", (line) => this.handleLine(line));
    proc.stderr.on("data", (chunk) => {
      const text = String(chunk).trim();
      if (text) {
        this.logger?.warn?.(`[codex-app-server] ${text}`);
      }
    });
    proc.on("error", (error) => this.rejectAll(error));
    proc.on("exit", (code, signal) => {
      this.closed = true;
      this.rejectAll(new Error(`Codex app-server exited: code=${code ?? "null"} signal=${signal ?? "null"}`));
      this.emit("close");
    });
  }

  static async start(options: CodexAppServerClientOptions): Promise<CodexAppServerClient> {
    const proc = spawn(options.binPath, ["app-server", "--listen", "stdio://"], {
      stdio: ["pipe", "pipe", "pipe"]
    });
    const client = new CodexAppServerClient(proc, options.logger);
    await client.request("initialize", {
      clientInfo: {
        name: "53aihub_codex_channel",
        title: "53AIHub Codex Channel",
        version: "0.1.0"
      },
      capabilities: {
        experimentalApi: true,
        requestAttestation: false
      }
    });
    client.notify("initialized", {});
    return client;
  }

  request<T = any>(method: string, params: Record<string, unknown> | undefined): Promise<T> {
    if (this.closed) {
      return Promise.reject(new Error("Codex app-server is closed"));
    }
    const id = this.nextId++;
    const payload = params === undefined ? { method, id } : { method, id, params };
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, {
        resolve: (value) => resolve(value as T),
        reject
      });
      this.proc.stdin.write(`${JSON.stringify(payload)}\n`, (error) => {
        if (!error) {
          return;
        }
        this.pending.delete(id);
        reject(error);
      });
    });
  }

  notify(method: string, params: Record<string, unknown>): void {
    if (this.closed) {
      return;
    }
    this.proc.stdin.write(`${JSON.stringify({ method, params })}\n`);
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.proc.kill();
    this.rejectAll(new Error("Codex app-server closed"));
  }

  private handleLine(line: string) {
    let message: Record<string, any>;
    try {
      message = JSON.parse(line) as Record<string, any>;
    } catch {
      this.logger?.warn?.(`[codex-app-server] ignored non-JSON line: ${line.slice(0, 120)}`);
      return;
    }

    if (typeof message.id === "number" && !message.method) {
      const pending = this.pending.get(message.id);
      if (!pending) {
        return;
      }
      this.pending.delete(message.id);
      if (message.error) {
        pending.reject(new Error(String(message.error.message || message.error.code || "Codex app-server error")));
      } else {
        pending.resolve(message.result);
      }
      return;
    }

    if (typeof message.id === "number" && typeof message.method === "string") {
      this.proc.stdin.write(
        `${JSON.stringify({
          id: message.id,
          error: {
            code: -32601,
            message: `53AIHub Codex channel does not handle server request: ${message.method}`
          }
        })}\n`
      );
      return;
    }

    if (typeof message.method === "string") {
      this.emit("notification", {
        method: message.method,
        params: message.params && typeof message.params === "object" ? message.params : {}
      } satisfies CodexAppServerNotification);
    }
  }

  private rejectAll(error: Error) {
    const pending = [...this.pending.values()];
    this.pending.clear();
    for (const request of pending) {
      request.reject(error);
    }
  }
}

export function createCodexAppServerTurnRunner(input: CodexAppServerClientOptions): CodexTurnRunner {
  let clientPromise: Promise<CodexAppServerClient> | undefined;

  async function getClient() {
    if (!clientPromise) {
      clientPromise = CodexAppServerClient.start(input);
    }
    return clientPromise;
  }

  return {
    async runTurn(turnInput) {
      const client = await getClient();
      let threadId = turnInput.threadId;
      if (threadId) {
        await client.request("thread/resume", {
          threadId,
          cwd: turnInput.cwd,
          approvalPolicy: "never",
          sandbox: "workspace-write"
        });
      } else {
        const started = await client.request<any>("thread/start", {
          cwd: turnInput.cwd,
          approvalPolicy: "never",
          sandbox: "workspace-write",
          serviceName: "53AIHub",
          threadSource: "user"
        });
        threadId = String(started?.thread?.id || "");
        if (!threadId) {
          throw new Error("Codex app-server did not return a thread id");
        }
        await turnInput.onThreadStarted?.({ threadId, cwd: String(started?.cwd || turnInput.cwd) });
      }

      let turnId = "";
      let completed = false;
      let finalStatus = "inProgress";
      let finalText = "";
      let resolveCompleted!: () => void;
      let rejectCompleted!: (error: Error) => void;
      const completedPromise = new Promise<void>((resolve, reject) => {
        resolveCompleted = resolve;
        rejectCompleted = reject;
      });

      const handleNotification = (notification: CodexAppServerNotification) => {
        const params = notification.params || {};
        if (params.threadId && params.threadId !== threadId) {
          return;
        }
        const notificationTurnId = readNotificationTurnId(notification);
        if (turnId && notificationTurnId && notificationTurnId !== turnId) {
          return;
        }
        if (!turnId && notificationTurnId) {
          turnId = notificationTurnId;
        }
        void turnInput.onEvent(notification);
        if (notification.method === "turn/completed") {
          completed = true;
          const turn = params.turn && typeof params.turn === "object" ? params.turn : {};
          finalStatus = typeof turn.status === "string" ? turn.status : "completed";
          finalText = extractAssistantTextFromTurn(turn);
          resolveCompleted();
        }
        if (notification.method === "error" && params.willRetry === false) {
          rejectCompleted(new Error(readCodexErrorMessage(params.error) || "Codex turn failed"));
        }
      };

      client.on("notification", handleNotification);
      try {
        const started = await client.request<any>("turn/start", {
          threadId,
          cwd: turnInput.cwd,
          approvalPolicy: "never",
          input: [
            {
              type: "text",
              text: turnInput.prompt,
              text_elements: []
            }
          ]
        });
        turnId = String(started?.turn?.id || turnId);
        if (!turnId) {
          throw new Error("Codex app-server did not return a turn id");
        }
        await turnInput.onTurnStarted?.({ threadId, turnId });
        if (!completed) {
          await completedPromise;
        }
        return {
          threadId,
          turnId,
          status: finalStatus,
          finalText
        };
      } finally {
        client.off("notification", handleNotification);
      }
    },
    async interruptTurn(threadId, turnId) {
      const client = await getClient();
      await client.request("turn/interrupt", { threadId, turnId });
    },
    async close() {
      if (!clientPromise) {
        return;
      }
      const client = await clientPromise;
      await client.close();
    }
  };
}

function readNotificationTurnId(notification: CodexAppServerNotification): string {
  const params = notification.params || {};
  if (typeof params.turnId === "string") {
    return params.turnId;
  }
  const turn = params.turn && typeof params.turn === "object" ? params.turn as Record<string, unknown> : {};
  return typeof turn.id === "string" ? turn.id : "";
}

function extractAssistantTextFromTurn(turn: Record<string, any>): string {
  const items = Array.isArray(turn.items) ? turn.items : [];
  return items
    .map((item) => {
      if (!item || typeof item !== "object" || item.type !== "agentMessage") {
        return "";
      }
      return typeof item.text === "string" ? item.text : "";
    })
    .filter(Boolean)
    .join("\n\n");
}

function readCodexErrorMessage(error: unknown): string {
  if (!error || typeof error !== "object") {
    return "";
  }
  const record = error as Record<string, unknown>;
  return typeof record.message === "string" ? record.message : "";
}

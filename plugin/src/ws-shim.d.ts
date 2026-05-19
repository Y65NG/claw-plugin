declare module "ws" {
  import { EventEmitter } from "node:events";
  import type { Duplex } from "node:stream";

  class WebSocket extends EventEmitter {
    static readonly OPEN: number;
    readonly readyState: number;
    constructor(address: string, options?: { headers?: Record<string, string> });
    send(data: string): void;
    close(): void;
    once(event: "close", listener: (code: number, reason: Buffer) => void): this;
    on(event: "open", listener: () => void): this;
    on(event: "message", listener: (data: Buffer) => void): this;
    on(event: "error", listener: (error: Error) => void): this;
    on(event: "close", listener: (code: number, reason: Buffer) => void): this;
  }

  class WebSocketServer extends EventEmitter {
    constructor(options?: { noServer?: boolean });
    handleUpgrade(
      request: unknown,
      socket: Duplex,
      head: Buffer,
      callback: (client: WebSocket) => void
    ): void;
  }

  export { WebSocketServer };
  export default WebSocket;
}

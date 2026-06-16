import { afterEach, describe, expect, it } from "vitest";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createCodexAppServerTurnRunner } from "../src/codex-app-server";

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("Codex App Server turn runner", () => {
  it("starts Codex threads with a schema-compatible user thread source", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-app-server-"));
    cleanupPaths.push(tempRoot);
    const fakeCodex = join(tempRoot, "codex");
    const capturePath = join(tempRoot, "thread-start.json");
    await writeFile(
      fakeCodex,
      [
        "#!/usr/bin/env node",
        "const fs = require('node:fs');",
        "const readline = require('node:readline');",
        "const capturePath = process.env.CODEX_CAPTURE_THREAD_START;",
        "const rl = readline.createInterface({ input: process.stdin });",
        "function send(message) { process.stdout.write(JSON.stringify(message) + '\\n'); }",
        "rl.on('line', (line) => {",
        "  const message = JSON.parse(line);",
        "  if (message.method === 'initialize') { send({ id: message.id, result: {} }); return; }",
        "  if (message.method === 'initialized') { return; }",
        "  if (message.method === 'thread/start') {",
        "    fs.writeFileSync(capturePath, JSON.stringify(message.params, null, 2));",
        "    send({ id: message.id, result: { thread: { id: 'thread-1' }, cwd: message.params.cwd } });",
        "    return;",
        "  }",
        "  if (message.method === 'turn/start') {",
        "    send({ id: message.id, result: { turn: { id: 'turn-1' } } });",
        "    setImmediate(() => send({ method: 'turn/completed', params: { threadId: message.params.threadId, turn: { id: 'turn-1', status: 'completed', items: [{ type: 'agentMessage', text: 'done' }] } } }));",
        "    return;",
        "  }",
        "});",
        "setInterval(() => {}, 1000);",
        ""
      ].join("\n")
    );
    await chmod(fakeCodex, 0o755);

    const previousCapturePath = process.env.CODEX_CAPTURE_THREAD_START;
    process.env.CODEX_CAPTURE_THREAD_START = capturePath;
    const runner = createCodexAppServerTurnRunner({ binPath: fakeCodex });
    try {
      const result = await runner.runTurn({
        prompt: "hello",
        cwd: tempRoot,
        conversationId: "conversation-a",
        onEvent() {}
      });
      expect(result.finalText).toBe("done");
      const threadStartParams = JSON.parse(await readFile(capturePath, "utf8")) as Record<string, unknown>;
      expect(threadStartParams.threadSource).toBe("user");
      expect(threadStartParams.serviceName).toBe("53AIHub");
      expect(threadStartParams.cwd).toBe(tempRoot);
    } finally {
      if (previousCapturePath === undefined) {
        delete process.env.CODEX_CAPTURE_THREAD_START;
      } else {
        process.env.CODEX_CAPTURE_THREAD_START = previousCapturePath;
      }
      await runner.close?.();
    }
  });
});

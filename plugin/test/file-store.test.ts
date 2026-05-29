import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { FileSessionStore } from "../src/file-store";

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("FileSessionStore", () => {
  it("preserves an existing 53AIHub title when gateway sync reports the control center display name", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-session-store-"));
    cleanupPaths.push(stateDir);

    const store = new FileSessionStore(join(stateDir, "state.json"), 20);
    await store.init();

    await store.upsertSession({
      id: "agent:main:dashboard:hub-session",
      title: "53AI Hub-Y65NG：从网上找5本书并总结",
      status: "completed",
      hostKind: "openclaw",
      runnerCommand: "openclaw-gateway",
      createdAt: "2026-05-29T02:00:00.000Z",
      updatedAt: "2026-05-29T02:00:01.000Z",
      lastEventSeq: 8
    });

    await store.upsertSession({
      id: "agent:main:dashboard:hub-session",
      title: "Claw Control Center",
      status: "running",
      hostKind: "openclaw",
      runnerCommand: "openclaw-gateway",
      createdAt: "2026-05-29T02:00:00.000Z",
      updatedAt: "2026-05-29T02:10:00.000Z",
      lastEventSeq: 13
    });

    expect(store.getSession("agent:main:dashboard:hub-session")?.session).toMatchObject({
      title: "53AI Hub-Y65NG：从网上找5本书并总结",
      status: "running",
      updatedAt: "2026-05-29T02:10:00.000Z",
      lastEventSeq: 13
    });
  });

  it("removes sessions that are absent from a successful full gateway sync", async () => {
    const stateDir = await mkdtemp(join(tmpdir(), "claw-session-store-prune-"));
    cleanupPaths.push(stateDir);

    const store = new FileSessionStore(join(stateDir, "state.json"), 20);
    await store.init();

    await store.upsertSession({
      id: "agent:main:dashboard:deleted-hub",
      title: "53AI Hub-Y65NG：已删除",
      status: "completed",
      hostKind: "openclaw",
      runnerCommand: "openclaw-gateway",
      createdAt: "2026-05-29T02:00:00.000Z",
      updatedAt: "2026-05-29T02:00:01.000Z",
      lastEventSeq: 8
    });

    await store.replaceSessions([]);

    expect(store.listSessions()).toEqual([]);
  });
});

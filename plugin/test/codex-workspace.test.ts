import { afterEach, describe, expect, it } from "vitest";
import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import {
  CODEX_WORKSPACE_MAPPING_FILE,
  ensureCodexConversationWorkspace,
  readCodexWorkspaceMappings,
  sanitizeCodexWorkspacePart,
  updateCodexWorkspaceThread
} from "../src/codex-workspace";

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("Codex workspace manager", () => {
  it("creates hidden 53AI workspace roots and stable per-conversation subfolders", async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), "codex-workspace-"));
    cleanupPaths.push(tempRoot);
    const workspaceRoot = join(tempRoot, ".53ai", "codex-workspaces");

    const first = await ensureCodexConversationWorkspace({
      workspaceRoot,
      conversationId: "conversation/alpha 1",
      userId: "42",
      userName: "Ada / Lovelace"
    });
    const same = await ensureCodexConversationWorkspace({
      workspaceRoot,
      conversationId: "conversation/alpha 1",
      userId: "42",
      userName: "Ada / Lovelace"
    });
    const second = await ensureCodexConversationWorkspace({
      workspaceRoot,
      conversationId: "conversation/beta 2",
      userId: "42",
      userName: "Ada / Lovelace"
    });

    expect(first.workspaceRoot).toBe(workspaceRoot);
    expect(basename(first.workspaceRoot)).toBe("codex-workspaces");
    expect(basename(join(first.workspaceRoot, ".."))).toBe(".53ai");
    expect(first.workspaceDir).toBe(same.workspaceDir);
    expect(first.workspaceDir).not.toBe(second.workspaceDir);
    expect(first.workspaceName).toMatch(/^53aihub-Ada-Lovelace-conversation-alpha-1-[a-f0-9]{10}$/);
    expect(second.workspaceName).toMatch(/^53aihub-Ada-Lovelace-conversation-beta-2-[a-f0-9]{10}$/);
    await access(first.workspaceDir);
    await access(second.workspaceDir);

    const updated = await updateCodexWorkspaceThread(workspaceRoot, first.conversationId, "thread-abc");
    expect(updated?.threadId).toBe("thread-abc");
    const mapping = await readCodexWorkspaceMappings(workspaceRoot);
    expect(mapping.conversations[first.conversationId]?.threadId).toBe("thread-abc");
    expect(await readFile(join(workspaceRoot, CODEX_WORKSPACE_MAPPING_FILE), "utf8")).toContain("thread-abc");
  });

  it("falls back to user id and strips unsafe filename characters", () => {
    expect(sanitizeCodexWorkspacePart("  /../../ name with spaces! ", "fallback")).toBe("name-with-spaces");
    expect(sanitizeCodexWorkspacePart("", "fallback")).toBe("fallback");
  });
});

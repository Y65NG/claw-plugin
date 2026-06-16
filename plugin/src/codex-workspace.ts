import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

export type CodexWorkspaceMapping = {
  conversationId: string;
  userId: string;
  userName?: string;
  workspaceName: string;
  workspaceDir: string;
  workspaceRoot: string;
  threadId?: string;
  createdAt: string;
  updatedAt: string;
};

export type CodexWorkspaceMappingFile = {
  version: 1;
  conversations: Record<string, CodexWorkspaceMapping>;
};

export type EnsureCodexWorkspaceInput = {
  conversationId: string;
  userId: string;
  userName?: string;
  workspaceRoot?: string;
};

export const DEFAULT_CODEX_WORKSPACE_ROOT = join(homedir(), ".53ai", "codex-workspaces");
export const CODEX_WORKSPACE_MAPPING_FILE = ".53aihub-codex-workspaces.json";

export async function ensureCodexConversationWorkspace(
  input: EnsureCodexWorkspaceInput
): Promise<CodexWorkspaceMapping> {
  const workspaceRoot = resolve(input.workspaceRoot || DEFAULT_CODEX_WORKSPACE_ROOT);
  const conversationId = input.conversationId.trim();
  if (!conversationId) {
    throw new Error("conversationId is required");
  }

  await mkdir(workspaceRoot, { recursive: true });
  const mappingFile = await readCodexWorkspaceMappings(workspaceRoot);
  const existing = mappingFile.conversations[conversationId];
  if (existing) {
    const normalized: CodexWorkspaceMapping = {
      ...existing,
      workspaceRoot,
      workspaceDir: resolve(existing.workspaceDir),
      updatedAt: new Date().toISOString()
    };
    await mkdir(normalized.workspaceDir, { recursive: true });
    mappingFile.conversations[conversationId] = normalized;
    await writeCodexWorkspaceMappings(workspaceRoot, mappingFile);
    return normalized;
  }

  const now = new Date().toISOString();
  const owner = sanitizeCodexWorkspacePart(input.userName || `user-${input.userId || "unknown"}`, "user");
  const shortConversationId = buildConversationIdShort(conversationId);
  const workspaceName = `53aihub-${owner}-${shortConversationId}`;
  const workspaceDir = resolve(join(workspaceRoot, workspaceName));
  if (!workspaceDir.startsWith(`${workspaceRoot}/`) && workspaceDir !== workspaceRoot) {
    throw new Error("resolved workspace directory escaped the Codex workspace root");
  }

  const mapping: CodexWorkspaceMapping = {
    conversationId,
    userId: input.userId || "unknown",
    ...(input.userName?.trim() ? { userName: input.userName.trim() } : {}),
    workspaceName,
    workspaceDir,
    workspaceRoot,
    createdAt: now,
    updatedAt: now
  };
  await mkdir(workspaceDir, { recursive: true });
  mappingFile.conversations[conversationId] = mapping;
  await writeCodexWorkspaceMappings(workspaceRoot, mappingFile);
  return mapping;
}

export async function updateCodexWorkspaceThread(
  workspaceRoot: string,
  conversationId: string,
  threadId: string
): Promise<CodexWorkspaceMapping | undefined> {
  const root = resolve(workspaceRoot || DEFAULT_CODEX_WORKSPACE_ROOT);
  const mappingFile = await readCodexWorkspaceMappings(root);
  const mapping = mappingFile.conversations[conversationId];
  if (!mapping) {
    return undefined;
  }
  const updated = {
    ...mapping,
    threadId,
    updatedAt: new Date().toISOString()
  };
  mappingFile.conversations[conversationId] = updated;
  await writeCodexWorkspaceMappings(root, mappingFile);
  return updated;
}

export async function readCodexWorkspaceMappings(workspaceRoot: string): Promise<CodexWorkspaceMappingFile> {
  const mappingPath = getCodexWorkspaceMappingPath(workspaceRoot);
  if (!existsSync(mappingPath)) {
    return { version: 1, conversations: {} };
  }
  try {
    const parsed = JSON.parse(await readFile(mappingPath, "utf8")) as Partial<CodexWorkspaceMappingFile>;
    return {
      version: 1,
      conversations:
        parsed.conversations && typeof parsed.conversations === "object" && !Array.isArray(parsed.conversations)
          ? parsed.conversations
          : {}
    };
  } catch {
    return { version: 1, conversations: {} };
  }
}

export function getCodexWorkspaceMappingPath(workspaceRoot: string): string {
  return join(resolve(workspaceRoot || DEFAULT_CODEX_WORKSPACE_ROOT), CODEX_WORKSPACE_MAPPING_FILE);
}

export function sanitizeCodexWorkspacePart(value: string, fallback: string): string {
  const normalized = value
    .normalize("NFKC")
    .replace(/[\\/\0:\s]+/g, "-")
    .replace(/[^\p{L}\p{N}._-]+/gu, "-")
    .replace(/-+/g, "-")
    .replace(/^[._-]+|[._-]+$/g, "")
    .slice(0, 48);
  return normalized || fallback;
}

export function buildConversationIdShort(conversationId: string): string {
  const sanitized = sanitizeCodexWorkspacePart(conversationId, "conversation").slice(0, 28);
  const hash = createHash("sha1").update(conversationId).digest("hex").slice(0, 10);
  return `${sanitized}-${hash}`;
}

async function writeCodexWorkspaceMappings(
  workspaceRoot: string,
  mappingFile: CodexWorkspaceMappingFile
): Promise<void> {
  await mkdir(resolve(workspaceRoot || DEFAULT_CODEX_WORKSPACE_ROOT), { recursive: true });
  await writeFile(getCodexWorkspaceMappingPath(workspaceRoot), `${JSON.stringify(mappingFile, null, 2)}\n`, {
    mode: 0o600
  });
}

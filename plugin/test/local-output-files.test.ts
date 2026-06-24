import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import {
  collectConversationManifestLocalOutputFiles,
  collectManifestLocalOutputFiles,
  collectRecentReferencedLocalOutputFiles,
  collectReferencedLocalOutputFiles,
  collectCreatedLocalOutputFiles,
  extractReferencedLocalOutputPaths,
  resolveLocalOutputManifestPath,
  resolveLocalOutputWorkspaceDirs,
  snapshotLocalOutputFiles
} from "../src/local-output-files";

describe("local output file detection", () => {
  it("detects newly created workspace files and filters hidden, directories, excluded, and oversized files", async () => {
    const workspace = join(await makeTempDir(), "workspace");
    await mkdir(workspace, { recursive: true });
    await writeFile(join(workspace, "existing.txt"), "before");

    const before = await snapshotLocalOutputFiles({
      config: {
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspace],
        createdFilesMaxFileBytes: 16,
        createdFilesExclude: ["**/ignored/**"]
      }
    });

    await writeFile(join(workspace, "created.txt"), "hello");
    await writeFile(join(workspace, ".hidden.txt"), "secret");
    await mkdir(join(workspace, "folder"), { recursive: true });
    await writeFile(join(workspace, "large.txt"), "this file is too large");
    await mkdir(join(workspace, "ignored"), { recursive: true });
    await writeFile(join(workspace, "ignored", "skip.txt"), "skip");

    const files = await collectCreatedLocalOutputFiles(before, {
      config: {
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspace],
        createdFilesMaxFileBytes: 16,
        createdFilesExclude: ["**/ignored/**"]
      }
    });

    expect(files).toHaveLength(1);
    expect(files[0]).toMatchObject({
      file_name: "created.txt",
      mime_type: "text/plain",
      size: 5,
      base64: Buffer.from("hello").toString("base64")
    });
    expect(files[0].id).toMatch(/^local-/);
  });

  it("detects workspace files modified during the run", async () => {
    const workspace = join(await makeTempDir(), "workspace");
    await mkdir(workspace, { recursive: true });
    const reportPath = join(workspace, "report.txt");
    await writeFile(reportPath, "before");

    const before = await snapshotLocalOutputFiles({
      config: {
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspace]
      }
    });

    await wait(5);
    await writeFile(reportPath, "after");

    const files = await collectCreatedLocalOutputFiles(before, {
      config: {
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspace]
      }
    });

    expect(files).toHaveLength(1);
    expect(files[0]).toMatchObject({
      file_name: "report.txt",
      size: 5,
      base64: Buffer.from("after").toString("base64")
    });
  });

  it("does not return referenced workspace files that were not created or modified during the run", async () => {
    const workspace = join(await makeTempDir(), "workspace");
    await mkdir(workspace, { recursive: true });
    const reportPath = join(workspace, "report.txt");
    await writeFile(reportPath, "before");

    const before = await snapshotLocalOutputFiles({
      config: {
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspace]
      }
    });
    const paths = extractReferencedLocalOutputPaths({ content: `已保存到 ${reportPath}` }, before);
    const files = await collectReferencedLocalOutputFiles(paths, before, {
      config: {
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspace]
      }
    });

    expect(paths).toEqual([reportPath]);
    expect(files).toHaveLength(0);
  });

  it("returns referenced workspace files when they were modified during the run", async () => {
    const workspace = join(await makeTempDir(), "workspace");
    await mkdir(workspace, { recursive: true });
    const reportPath = join(workspace, "report.txt");
    await writeFile(reportPath, "before");

    const before = await snapshotLocalOutputFiles({
      config: {
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspace]
      }
    });

    await wait(5);
    await writeFile(reportPath, "after");

    const paths = extractReferencedLocalOutputPaths({ content: `已保存到 ${reportPath}` }, before);
    const files = await collectReferencedLocalOutputFiles(paths, before, {
      config: {
        detectCreatedFiles: true,
        fileWorkspaceDirs: [workspace]
      }
    });

    expect(paths).toEqual([reportPath]);
    expect(files).toHaveLength(1);
    expect(files[0]).toMatchObject({
      file_name: "report.txt",
      size: 5,
      base64: Buffer.from("after").toString("base64")
    });
  });

  it("returns recently referenced workspace files without requiring a snapshot", async () => {
    const workspace = join(await makeTempDir(), "workspace");
    await mkdir(workspace, { recursive: true });
    const reportPath = join(workspace, "fresh.txt");
    const sinceMs = Date.now();

    await wait(5);
    await writeFile(reportPath, "fresh output");

    const paths = extractReferencedLocalOutputPaths(
      { content: `已保存到 ${reportPath}` },
      undefined,
      {
        config: {
          detectCreatedFiles: true,
          fileWorkspaceDirs: [workspace]
        }
      }
    );
    const files = await collectRecentReferencedLocalOutputFiles(
      paths,
      {
        config: {
          detectCreatedFiles: true,
          fileWorkspaceDirs: [workspace]
        }
      },
      sinceMs
    );

    expect(paths).toEqual([reportPath]);
    expect(files).toHaveLength(1);
    expect(files[0]).toMatchObject({
      file_name: "fresh.txt",
      size: 12,
      base64: Buffer.from("fresh output").toString("base64")
    });
  });

  it("infers QClaw and OpenClaw workspace directories from config paths", async () => {
    const root = await makeTempDir();
    const qclawConfig = join(root, ".qclaw", "openclaw.json");
    const openclawConfig = join(root, ".openclaw", "openclaw.json");

    expect(resolveLocalOutputWorkspaceDirs({ configPath: qclawConfig })).toContain(join(root, ".qclaw", "workspace"));
    expect(resolveLocalOutputWorkspaceDirs({ configPath: openclawConfig })).toContain(join(root, ".openclaw", "workspace"));
  });

  it("reads only matching records from a conversation-level output manifest", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    await mkdir(workspace, { recursive: true });
    const firstPath = join(workspace, "first.md");
    const secondPath = join(workspace, "second.md");
    await writeFile(firstPath, "# first\n");
    await writeFile(secondPath, "# second\n");

    const conversationId = "session-1";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(
      manifestPath,
      [
        JSON.stringify(buildManifestRecord(conversationId, "turn-1", "req-1", firstPath, "first.md", "# first\n")),
        JSON.stringify(buildManifestRecord(conversationId, "turn-2", "req-2", secondPath, "second.md", "# second\n"))
      ].join("\n")
    );

    const files = await collectManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      turnId: "turn-2",
      activeRequestId: "req-2",
      config: { fileWorkspaceDirs: [workspace] }
    });

    expect(files).toHaveLength(1);
    expect(files[0]).toMatchObject({
      file_name: "second.md",
      mime_type: "text/markdown",
      size: 9,
      base64: Buffer.from("# second\n").toString("base64"),
      source_kind: "tool.write"
    });
  });

  it("reads all valid records from a conversation-level output manifest for backfill", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    await mkdir(workspace, { recursive: true });
    const firstPath = join(workspace, "first.md");
    const secondPath = join(workspace, "second.md");
    await writeFile(firstPath, "# first\n");
    await writeFile(secondPath, "# second\n");

    const conversationId = "session-backfill";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(
      manifestPath,
      [
        JSON.stringify(buildManifestRecord(conversationId, "turn-1", "req-1", firstPath, "first.md", "# first\n")),
        JSON.stringify(buildManifestRecord(conversationId, "turn-2", "req-2", secondPath, "second.md", "# second\n")),
        JSON.stringify(buildManifestRecord("other-session", "turn-3", "req-3", secondPath, "other.md", "# second\n"))
      ].join("\n")
    );

    const files = await collectConversationManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      config: { fileWorkspaceDirs: [workspace] }
    });

    expect(files.map((file) => file.file_name)).toEqual(["first.md", "second.md"]);
    expect(files[0]).toMatchObject({
      conversation_id: conversationId,
      turn_id: "turn-1",
      active_request_id: "req-1",
      part_id: "turn-1:output",
      sha256: createHash("sha256").update("# first\n").digest("hex")
    });
  });

  it("accepts sha256-verified manifest records when size metadata is stale", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    await mkdir(workspace, { recursive: true });
    const outputPath = join(workspace, "summary.md");
    await writeFile(outputPath, "# summary\n\nupdated content\n");

    const conversationId = "session-stale-size";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    await mkdir(dirname(manifestPath), { recursive: true });
    const record = buildManifestRecord(
      conversationId,
      "turn-summary",
      "req-summary",
      outputPath,
      "summary.md",
      "# summary\n\nupdated content\n"
    );
    record.size = 7;
    const warn = vi.fn();
    await writeFile(manifestPath, JSON.stringify(record));

    const files = await collectConversationManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      config: { fileWorkspaceDirs: [workspace] },
      logger: { warn }
    });

    expect(files).toHaveLength(1);
    expect(files[0]).toMatchObject({
      file_name: "summary.md",
      size: Buffer.byteLength("# summary\n\nupdated content\n"),
      sha256: createHash("sha256").update("# summary\n\nupdated content\n").digest("hex")
    });
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("accepting sha256-verified content"));
  });

  it("keeps concurrent manifest records separated by active request id", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    await mkdir(workspace, { recursive: true });
    const alphaPath = join(workspace, "alpha.txt");
    const betaPath = join(workspace, "beta.txt");
    await writeFile(alphaPath, "alpha");
    await writeFile(betaPath, "beta");

    const conversationId = "session-concurrent";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(
      manifestPath,
      [
        JSON.stringify(buildManifestRecord(conversationId, "turn-alpha", "req-alpha", alphaPath, "alpha.txt", "alpha")),
        JSON.stringify(buildManifestRecord(conversationId, "turn-beta", "req-beta", betaPath, "beta.txt", "beta"))
      ].join("\n")
    );

    const files = await collectManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      turnId: "turn-beta",
      activeRequestId: "req-beta",
      config: { fileWorkspaceDirs: [workspace] }
    });

    expect(files.map((file) => file.file_name)).toEqual(["beta.txt"]);
  });

  it("accepts numeric manifest request ids by normalizing them to strings", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    await mkdir(workspace, { recursive: true });
    const outputPath = join(workspace, "10chars.txt");
    await writeFile(outputPath, "0123456789");

    const conversationId = "agent:main:dashboard:test";
    const activeRequestId = "1782197186887";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    const record = buildManifestRecord(
      conversationId,
      `${conversationId}:turn:${activeRequestId}`,
      activeRequestId,
      outputPath,
      "10chars.txt",
      "0123456789"
    ) as Record<string, unknown>;
    record.active_request_id = Number(activeRequestId);
    record.part_id = 1;
    record.size = "10";
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(manifestPath, JSON.stringify(record));

    const files = await collectManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      turnId: `${conversationId}:turn:${activeRequestId}`,
      activeRequestId,
      config: { fileWorkspaceDirs: [workspace] }
    });

    expect(files).toHaveLength(1);
    expect(files[0]).toMatchObject({
      file_name: "10chars.txt",
      size: 10,
      base64: Buffer.from("0123456789").toString("base64")
    });
  });

  it("skips malformed manifest lines and keeps valid records usable", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    await mkdir(workspace, { recursive: true });
    const reportPath = join(workspace, "report.txt");
    await writeFile(reportPath, "report");

    const conversationId = "session-bad-line";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(
      manifestPath,
      [
        "{not valid json",
        JSON.stringify(buildManifestRecord(conversationId, "turn-1", "req-1", reportPath, "report.txt", "report"))
      ].join("\n")
    );
    const logger = { warn: vi.fn() };

    const files = await collectManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      turnId: "turn-1",
      activeRequestId: "req-1",
      config: { fileWorkspaceDirs: [workspace] },
      logger
    });

    expect(files.map((file) => file.file_name)).toEqual(["report.txt"]);
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining("skipping malformed local output manifest line 1"));
  });

  it("rejects manifest records outside allowed workspaces", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    const outside = join(stateDir, "outside");
    await mkdir(workspace, { recursive: true });
    await mkdir(outside, { recursive: true });
    const outsidePath = join(outside, "secret.txt");
    await writeFile(outsidePath, "secret");

    const conversationId = "session-outside";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(
      manifestPath,
      JSON.stringify(buildManifestRecord(conversationId, "turn-1", "req-1", outsidePath, "secret.txt", "secret"))
    );

    const files = await collectManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      turnId: "turn-1",
      activeRequestId: "req-1",
      config: { fileWorkspaceDirs: [workspace] }
    });

    expect(files).toHaveLength(0);
  });

  it("rejects manifest records whose sha256 does not match the file", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    await mkdir(workspace, { recursive: true });
    const reportPath = join(workspace, "report.txt");
    await writeFile(reportPath, "current");

    const conversationId = "session-sha";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    const record = buildManifestRecord(conversationId, "turn-1", "req-1", reportPath, "report.txt", "current");
    record.sha256 = "0".repeat(64);
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(manifestPath, JSON.stringify(record));

    const files = await collectManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      turnId: "turn-1",
      activeRequestId: "req-1",
      config: { fileWorkspaceDirs: [workspace] }
    });

    expect(files).toHaveLength(0);
  });

  it("deduplicates repeated manifest records for the same request, logical path, and sha256", async () => {
    const stateDir = await makeTempDir();
    const workspace = join(stateDir, "workspace");
    await mkdir(workspace, { recursive: true });
    const reportPath = join(workspace, "report.txt");
    await writeFile(reportPath, "dupe");

    const conversationId = "session-dupe";
    const manifestPath = resolveLocalOutputManifestPath({ stateDir, conversationId })!;
    const record = buildManifestRecord(conversationId, "turn-1", "req-1", reportPath, "report.txt", "dupe");
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(manifestPath, [JSON.stringify(record), JSON.stringify(record)].join("\n"));

    const files = await collectManifestLocalOutputFiles({
      manifestPath,
      conversationId,
      turnId: "turn-1",
      activeRequestId: "req-1",
      config: { fileWorkspaceDirs: [workspace] }
    });

    expect(files.map((file) => file.file_name)).toEqual(["report.txt"]);
  });
});

async function makeTempDir(): Promise<string> {
  const { mkdtemp } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  return mkdtemp(join(tmpdir(), "claw-local-output-"));
}

async function wait(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function buildManifestRecord(
  conversationId: string,
  turnId: string,
  activeRequestId: string,
  path: string,
  logicalPath: string,
  content: string
) {
  return {
    conversation_id: conversationId,
    turn_id: turnId,
    active_request_id: activeRequestId,
    part_id: `${turnId}:output`,
    path,
    logical_path: logicalPath,
    mime_type: logicalPath.endsWith(".md") ? "text/markdown" : "text/plain",
    size: Buffer.byteLength(content),
    sha256: createHash("sha256").update(content).digest("hex"),
    created_at: "2026-06-23T00:00:00.000Z",
    source_kind: "tool.write"
  };
}

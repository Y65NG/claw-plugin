import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  collectRecentReferencedLocalOutputFiles,
  collectReferencedLocalOutputFiles,
  collectCreatedLocalOutputFiles,
  extractReferencedLocalOutputPaths,
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
});

async function makeTempDir(): Promise<string> {
  const { mkdtemp } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  return mkdtemp(join(tmpdir(), "claw-local-output-"));
}

async function wait(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

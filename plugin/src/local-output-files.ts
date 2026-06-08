import { createHash } from "node:crypto";
import { lstat, readdir, readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, relative, resolve, sep } from "node:path";

export type LocalOutputFileConfig = {
  detectCreatedFiles?: boolean;
  fileWorkspaceDirs?: string[];
  createdFilesMaxFileBytes?: number;
  createdFilesMaxCount?: number;
  createdFilesExclude?: string[];
};

export type LocalOutputFileSnapshot = {
  workspaceDirs: string[];
  files: Map<string, LocalOutputFileEntry>;
};

export type LocalOutputFileEntry = {
  path: string;
  workspaceDir: string;
  relativePath: string;
  size: number;
  mtimeMs: number;
};

export type LocalOutputFile = {
  id: string;
  file_name: string;
  mime_type?: string;
  size?: number;
  base64: string;
};

type ExcludeRules = {
  patterns: RegExp[];
  segments: Set<string>;
};

type LocalOutputFileRuntime = {
  config?: LocalOutputFileConfig;
  configPath?: string;
  stateDir?: string;
  logger?: {
    warn?(message: string): void;
  };
};

const DEFAULT_MAX_FILE_BYTES = 10 * 1024 * 1024;
const DEFAULT_MAX_FILE_COUNT = 20;
const DEFAULT_EXCLUDE_PATTERNS = [
  "**/.git/**",
  "**/.cache/**",
  "**/.DS_Store",
  "**/node_modules/**",
  "**/dist/**",
  "**/build/**",
  "**/__pycache__/**"
];
const DEFAULT_EXCLUDE_SEGMENTS = new Set([".git", ".cache", "node_modules", "dist", "build", "__pycache__"]);

export function resolveLocalOutputWorkspaceDirs(input: LocalOutputFileRuntime): string[] {
  const configured = input.config?.fileWorkspaceDirs?.map(expandHome).map((entry) => resolve(entry)) ?? [];
  const inferred = inferWorkspaceDirs(input);
  return dedupeStrings([...configured, ...inferred]).filter(Boolean);
}

export async function snapshotLocalOutputFiles(input: LocalOutputFileRuntime): Promise<LocalOutputFileSnapshot | undefined> {
  if (input.config?.detectCreatedFiles === false) {
    return undefined;
  }
  const workspaceDirs = resolveLocalOutputWorkspaceDirs(input);
  if (workspaceDirs.length === 0) {
    return undefined;
  }
  const files = new Map<string, LocalOutputFileEntry>();
  const excludeRules = buildExcludeRules(input.config);
  for (const workspaceDir of workspaceDirs) {
    await collectWorkspaceFiles(workspaceDir, workspaceDir, excludeRules, files, input.logger);
  }
  return { workspaceDirs, files };
}

export async function collectCreatedLocalOutputFiles(
  before: LocalOutputFileSnapshot | undefined,
  input: LocalOutputFileRuntime
): Promise<LocalOutputFile[]> {
  if (!before || input.config?.detectCreatedFiles === false) {
    return [];
  }
  const after = new Map<string, LocalOutputFileEntry>();
  const excludeRules = buildExcludeRules(input.config);
  for (const workspaceDir of before.workspaceDirs) {
    await collectWorkspaceFiles(workspaceDir, workspaceDir, excludeRules, after, input.logger);
  }

  const maxFileBytes = input.config?.createdFilesMaxFileBytes ?? DEFAULT_MAX_FILE_BYTES;
  const maxFileCount = input.config?.createdFilesMaxCount ?? DEFAULT_MAX_FILE_COUNT;
  const createdOrModified = [...after.values()]
    .filter((entry) => isCreatedOrModified(entry, before.files.get(entry.path)))
    .filter((entry) => entry.size > 0 && entry.size <= maxFileBytes)
    .sort((left, right) => right.mtimeMs - left.mtimeMs)
    .slice(0, Math.max(0, maxFileCount));

  return entriesToOutputFiles(createdOrModified, input);
}

export function extractReferencedLocalOutputPaths(
  value: unknown,
  snapshot: LocalOutputFileSnapshot | undefined,
  input?: LocalOutputFileRuntime
): string[] {
  const workspaceDirs = snapshot?.workspaceDirs ?? (input ? resolveLocalOutputWorkspaceDirs(input) : []);
  if (workspaceDirs.length === 0) {
    return [];
  }
  const text = collectTextFragments(value).join("\n");
  if (!text.trim()) {
    return [];
  }

  const paths: string[] = [];
  for (const workspaceDir of workspaceDirs) {
    const normalizedWorkspace = normalizeAbsolutePath(workspaceDir);
    const pattern = new RegExp(`${escapeRegExp(normalizedWorkspace)}/[^\\s\`'"<>)]*`, "g");
    for (const match of text.matchAll(pattern)) {
      const normalized = stripPathPunctuation(match[0] ?? "");
      if (normalized) {
        paths.push(normalized);
      }
    }
  }
  return dedupeStrings(paths);
}

export async function collectRecentReferencedLocalOutputFiles(
  paths: Iterable<string>,
  input: LocalOutputFileRuntime,
  sinceMs: number,
  untilMs = Date.now()
): Promise<LocalOutputFile[]> {
  if (input.config?.detectCreatedFiles === false) {
    return [];
  }
  const workspaceDirs = resolveLocalOutputWorkspaceDirs(input);
  if (workspaceDirs.length === 0) {
    return [];
  }
  const maxFileBytes = input.config?.createdFilesMaxFileBytes ?? DEFAULT_MAX_FILE_BYTES;
  const maxFileCount = input.config?.createdFilesMaxCount ?? DEFAULT_MAX_FILE_COUNT;
  const excludeRules = buildExcludeRules(input.config);
  const entries: LocalOutputFileEntry[] = [];
  const seen = new Set<string>();
  const lowerBoundMs = Math.max(0, sinceMs - 1);
  const upperBoundMs = untilMs + 60_000;

  for (const rawPath of paths) {
    const path = resolve(expandHome(rawPath));
    if (seen.has(path)) {
      continue;
    }
    seen.add(path);
    const workspaceDir = findContainingWorkspace(path, workspaceDirs);
    if (!workspaceDir) {
      continue;
    }
    const relativePath = normalizeRelativePath(relative(workspaceDir, path));
    if (!relativePath || shouldExclude(relativePath, excludeRules)) {
      continue;
    }
    try {
      const stat = await lstat(path);
      if (!stat.isFile() || stat.size <= 0 || stat.size > maxFileBytes) {
        continue;
      }
      if (stat.mtimeMs < lowerBoundMs || stat.mtimeMs > upperBoundMs) {
        continue;
      }
      entries.push({
        path,
        workspaceDir,
        relativePath,
        size: stat.size,
        mtimeMs: stat.mtimeMs
      });
    } catch {
      continue;
    }
  }

  const latestEntries = entries.sort((left, right) => right.mtimeMs - left.mtimeMs).slice(0, Math.max(0, maxFileCount));
  return entriesToOutputFiles(latestEntries, input);
}

export async function collectReferencedLocalOutputFiles(
  paths: Iterable<string>,
  before: LocalOutputFileSnapshot | undefined,
  input: LocalOutputFileRuntime
): Promise<LocalOutputFile[]> {
  if (!before || input.config?.detectCreatedFiles === false) {
    return [];
  }
  const maxFileBytes = input.config?.createdFilesMaxFileBytes ?? DEFAULT_MAX_FILE_BYTES;
  const maxFileCount = input.config?.createdFilesMaxCount ?? DEFAULT_MAX_FILE_COUNT;
  const excludeRules = buildExcludeRules(input.config);
  const entries: LocalOutputFileEntry[] = [];
  const seen = new Set<string>();

  for (const rawPath of paths) {
    const path = resolve(expandHome(rawPath));
    if (seen.has(path)) {
      continue;
    }
    seen.add(path);
    const workspaceDir = findContainingWorkspace(path, before.workspaceDirs);
    if (!workspaceDir) {
      continue;
    }
    const relativePath = normalizeRelativePath(relative(workspaceDir, path));
    if (!relativePath || shouldExclude(relativePath, excludeRules)) {
      continue;
    }
    try {
      const stat = await lstat(path);
      if (!stat.isFile() || stat.size <= 0 || stat.size > maxFileBytes) {
        continue;
      }
      const entry = {
        path,
        workspaceDir,
        relativePath,
        size: stat.size,
        mtimeMs: stat.mtimeMs
      };
      if (!isCreatedOrModified(entry, before.files.get(path))) {
        continue;
      }
      entries.push(entry);
    } catch {
      continue;
    }
  }

  const latestEntries = entries.sort((left, right) => right.mtimeMs - left.mtimeMs).slice(0, Math.max(0, maxFileCount));
  return entriesToOutputFiles(latestEntries, input);
}

async function entriesToOutputFiles(
  entries: LocalOutputFileEntry[],
  input: LocalOutputFileRuntime
): Promise<LocalOutputFile[]> {
  const files: LocalOutputFile[] = [];
  for (const entry of entries) {
    try {
      const bytes = await readFile(entry.path);
      files.push({
        id: buildLocalOutputFileId(entry, bytes),
        file_name: entry.relativePath,
        mime_type: inferMimeType(entry.relativePath),
        size: entry.size,
        base64: bytes.toString("base64")
      });
    } catch (error) {
      input.logger?.warn?.(
        `[53aihub] failed to read local output file ${entry.relativePath}: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }
  return files;
}

function isCreatedOrModified(after: LocalOutputFileEntry, before: LocalOutputFileEntry | undefined): boolean {
  if (!before) {
    return true;
  }
  return after.size !== before.size || after.mtimeMs > before.mtimeMs + 1;
}

function inferWorkspaceDirs(input: LocalOutputFileRuntime): string[] {
  const candidates: string[] = [];
  const hints = [input.configPath, input.stateDir].filter((value): value is string => Boolean(value));
  for (const hint of hints) {
    const normalized = hint.split(sep).join("/");
    if (normalized.includes("/.qclaw/") || normalized.endsWith("/.qclaw")) {
      candidates.push(resolve(dirname(hint), "workspace"));
    }
    if (normalized.includes("/.openclaw/") || normalized.endsWith("/.openclaw")) {
      candidates.push(resolve(dirname(hint), "workspace"));
    }
  }

  const qclawWorkspace = resolve(homedir(), ".qclaw", "workspace");
  const openclawWorkspace = resolve(homedir(), ".openclaw", "workspace");
  if (existsSync(qclawWorkspace)) {
    candidates.push(qclawWorkspace);
  }
  if (existsSync(openclawWorkspace)) {
    candidates.push(openclawWorkspace);
  }
  return dedupeStrings(candidates);
}

async function collectWorkspaceFiles(
  workspaceDir: string,
  currentDir: string,
  excludeRules: ExcludeRules,
  output: Map<string, LocalOutputFileEntry>,
  logger?: LocalOutputFileRuntime["logger"]
): Promise<void> {
  if (!existsSync(currentDir)) {
    return;
  }

  let entries: Awaited<ReturnType<typeof readdir>>;
  try {
    entries = await readdir(currentDir, { withFileTypes: true });
  } catch (error) {
    logger?.warn?.(
      `[53aihub] failed to scan local output directory ${currentDir}: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
    return;
  }

  for (const entry of entries) {
    if (entry.name.startsWith(".")) {
      continue;
    }
    const path = resolve(currentDir, entry.name);
    const relativePath = normalizeRelativePath(relative(workspaceDir, path));
    if (!relativePath || shouldExclude(relativePath, excludeRules)) {
      continue;
    }
    if (entry.isDirectory()) {
      await collectWorkspaceFiles(workspaceDir, path, excludeRules, output, logger);
      continue;
    }
    if (!entry.isFile()) {
      continue;
    }
    try {
      const stat = await lstat(path);
      if (!stat.isFile()) {
        continue;
      }
      output.set(path, {
        path,
        workspaceDir,
        relativePath,
        size: stat.size,
        mtimeMs: stat.mtimeMs
      });
    } catch {
      continue;
    }
  }
}

function buildExcludeRules(config?: LocalOutputFileConfig): ExcludeRules {
  const rawPatterns = [...DEFAULT_EXCLUDE_PATTERNS, ...(config?.createdFilesExclude ?? [])];
  const segments = new Set(DEFAULT_EXCLUDE_SEGMENTS);
  for (const pattern of rawPatterns) {
    const segment = extractExcludedSegment(pattern);
    if (segment) {
      segments.add(segment);
    }
  }
  return {
    patterns: rawPatterns.map(globToRegExp),
    segments
  };
}

function globToRegExp(pattern: string): RegExp {
  const normalized = normalizeRelativePath(pattern);
  const escaped = normalized
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\*\*/g, "__DOUBLE_STAR__")
    .replace(/\*/g, "[^/]*");
  return new RegExp(`(^|/)${escaped.replace(/__DOUBLE_STAR__/g, ".*")}($|/)`, "i");
}

function extractExcludedSegment(pattern: string): string {
  const normalized = normalizeRelativePath(pattern);
  const match = normalized.match(/^(?:\*\*\/)?([^/*]+)\/\*\*$/);
  return match?.[1] ?? "";
}

function shouldExclude(relativePath: string, rules: ExcludeRules): boolean {
  const parts = relativePath.split("/");
  if (parts.some((part) => part.startsWith(".") || rules.segments.has(part))) {
    return true;
  }
  return rules.patterns.some((pattern) => pattern.test(relativePath));
}

function collectTextFragments(value: unknown, depth = 0): string[] {
  if (value == null || depth > 5) {
    return [];
  }
  if (typeof value === "string") {
    return [value];
  }
  if (Array.isArray(value)) {
    return value.flatMap((entry) => collectTextFragments(entry, depth + 1));
  }
  if (typeof value !== "object") {
    return [];
  }
  const record = value as Record<string, unknown>;
  const fragments: string[] = [];
  for (const key of ["content", "text", "message", "output", "result", "path", "file_path", "filePath"]) {
    fragments.push(...collectTextFragments(record[key], depth + 1));
  }
  for (const key of ["data", "payload", "files", "attachments", "artifact", "artifacts", "media"]) {
    fragments.push(...collectTextFragments(record[key], depth + 1));
  }
  return fragments;
}

function findContainingWorkspace(path: string, workspaceDirs: string[]): string {
  const normalizedPath = normalizeAbsolutePath(path);
  return (
    workspaceDirs
      .map((workspaceDir) => normalizeAbsolutePath(workspaceDir))
      .find((workspaceDir) => normalizedPath === workspaceDir || normalizedPath.startsWith(`${workspaceDir}/`)) ?? ""
  );
}

function normalizeAbsolutePath(path: string): string {
  return resolve(expandHome(path)).split(sep).join("/");
}

function stripPathPunctuation(path: string): string {
  return path.replace(/[.,;:!?，。；：！？、]+$/g, "").replace(/]+$/g, "");
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildLocalOutputFileId(entry: LocalOutputFileEntry, bytes: Buffer): string {
  return `local-${createHash("sha256")
    .update(entry.relativePath)
    .update("\0")
    .update(String(entry.size))
    .update("\0")
    .update(bytes)
    .digest("hex")
    .slice(0, 24)}`;
}

function inferMimeType(fileName: string): string {
  const ext = fileName.split(".").pop()?.toLowerCase() ?? "";
  const mimeByExt: Record<string, string> = {
    css: "text/css",
    csv: "text/csv",
    gif: "image/gif",
    htm: "text/html",
    html: "text/html",
    jpeg: "image/jpeg",
    jpg: "image/jpeg",
    js: "text/javascript",
    json: "application/json",
    log: "text/plain",
    m4a: "audio/mp4",
    md: "text/markdown",
    mov: "video/quicktime",
    mp3: "audio/mpeg",
    mp4: "video/mp4",
    pdf: "application/pdf",
    png: "image/png",
    svg: "image/svg+xml",
    txt: "text/plain",
    wav: "audio/wav",
    webm: "video/webm",
    webp: "image/webp",
    xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
  };
  return mimeByExt[ext] ?? "application/octet-stream";
}

function expandHome(path: string): string {
  return path === "~" || path.startsWith("~/") ? resolve(homedir(), path.slice(2)) : path;
}

function normalizeRelativePath(path: string): string {
  return path.split(sep).join("/").replace(/^\/+/, "");
}

function dedupeStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    result.push(trimmed);
  }
  return result;
}

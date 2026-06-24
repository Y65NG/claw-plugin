import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { cp, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir, homedir } from "node:os";
import { basename, dirname, join, resolve, sep } from "node:path";
import { inflateRawSync } from "node:zlib";

import { resolveHostConfigPath } from "./host";

type HubAuthConfig = {
  botId: string;
  secret: string;
  wsUrl: string;
};

export type EnsureHubSkillRequest = {
  skill_id?: string;
  skill_name?: string;
  display_name?: string;
  version?: string;
  package_url?: string;
  sha256?: string;
  zip_name?: string;
};

export type EnsureHubSkillResult = {
  ok: boolean;
  status: "installed" | "up_to_date" | "failed";
  skill_id?: string;
  skill_name?: string;
  display_name?: string;
  version?: string;
  sha256?: string;
  install_path?: string;
  config_path?: string;
  error?: string;
};

type SkillInstallerInput = {
  request: EnsureHubSkillRequest;
  configPath?: string;
  stateDir: string;
  hub: HubAuthConfig;
  logger?: {
    info?(message: string): void;
    warn?(message: string): void;
  };
};

type ZipEntry = {
  name: string;
  method: number;
  flags: number;
  compressedSize: number;
  localHeaderOffset: number;
};

const ZIP_EOCD_SIGNATURE = 0x06054b50;
const ZIP_CENTRAL_SIGNATURE = 0x02014b50;
const ZIP_LOCAL_SIGNATURE = 0x04034b50;

export async function ensureHubSkillInstalled(input: SkillInstallerInput): Promise<EnsureHubSkillResult> {
  const skillName = sanitizeSkillName(input.request.skill_name);
  if (!skillName) {
    return failed(input.request, "skill_name is required");
  }
  if (!input.request.package_url) {
    return failed(input.request, "package_url is required");
  }

  try {
    const hostConfigPath = resolveHostConfigPath(input.configPath ?? join(input.stateDir, "openclaw.json"));
    const hostConfig = await readJSONFile(hostConfigPath);
    const installRoot = resolveSkillInstallRoot(hostConfig, hostConfigPath);
    const installPath = join(installRoot, skillName);
    const markerPath = join(installPath, ".53aihub-skill.json");
    const marker = await readJSONFile(markerPath);
    const expectedSHA = normalizeSHA256(input.request.sha256);

    if (
      expectedSHA &&
      marker?.sha256 === expectedSHA &&
      hostConfig?.skills?.entries?.[skillName]?.enabled === true &&
      existsSync(join(installPath, "SKILL.md"))
    ) {
      return {
        ok: true,
        status: "up_to_date",
        skill_id: input.request.skill_id,
        skill_name: skillName,
        display_name: input.request.display_name,
        version: input.request.version,
        sha256: expectedSHA,
        install_path: installPath,
        config_path: hostConfigPath
      };
    }

    const zipBytes = await downloadSkillPackage(input.request.package_url, input.hub);
    const actualSHA = createHash("sha256").update(zipBytes).digest("hex");
    if (expectedSHA && actualSHA !== expectedSHA) {
      return failed(input.request, `skill package sha256 mismatch: expected ${expectedSHA}, got ${actualSHA}`);
    }

    const tempRoot = await mkdtemp(join(tmpdir(), "53aihub-skill-"));
    const extractRoot = join(tempRoot, "extract");
    await mkdir(extractRoot, { recursive: true });
    await extractZipSafely(zipBytes, extractRoot);
    const sourceRoot = await findExtractedSkillRoot(extractRoot);

    await mkdir(installRoot, { recursive: true });
    if (existsSync(installPath)) {
      await rename(installPath, `${installPath}.bak-${timestampSuffix()}`);
    }
    await cp(sourceRoot, installPath, { recursive: true });
    await writeFile(
      markerPath,
      JSON.stringify(
        {
          skill_id: input.request.skill_id,
          skill_name: skillName,
          display_name: input.request.display_name,
          version: input.request.version,
          sha256: actualSHA,
          installed_at: new Date().toISOString(),
          source: "53aihub"
        },
        null,
        2
      ) + "\n",
      "utf8"
    );
    await rm(tempRoot, { recursive: true, force: true });

    await backupHostConfig(hostConfigPath);
    const nextConfig = hostConfig ?? {};
    const skills = ensureRecord(nextConfig, "skills");
    const entries = ensureRecord(skills, "entries");
    entries[skillName] = buildEnabledSkillConfigEntry(entries[skillName]);
    await writeFile(hostConfigPath, JSON.stringify(nextConfig, null, 2) + "\n", "utf8");

    input.logger?.info?.(`[53aihub] installed skill ${skillName} to ${installPath}`);
    return {
      ok: true,
      status: "installed",
      skill_id: input.request.skill_id,
      skill_name: skillName,
      display_name: input.request.display_name,
      version: input.request.version,
      sha256: actualSHA,
      install_path: installPath,
      config_path: hostConfigPath
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    input.logger?.warn?.(`[53aihub] failed to install skill ${skillName}: ${message}`);
    return failed(input.request, message);
  }
}

export async function readInstalledHubSkillInstructions(input: {
  skillName?: string;
  configPath?: string;
  stateDir: string;
  maxChars?: number;
}): Promise<string | undefined> {
  const skillName = sanitizeSkillName(input.skillName);
  if (!skillName) {
    return undefined;
  }
  try {
    const hostConfigPath = resolveHostConfigPath(input.configPath ?? join(input.stateDir, "openclaw.json"));
    const hostConfig = await readJSONFile(hostConfigPath);
    const installRoot = resolveSkillInstallRoot(hostConfig, hostConfigPath);
    const content = (await readFile(join(installRoot, skillName, "SKILL.md"), "utf8")).trim();
    if (!content) {
      return undefined;
    }
    const maxChars = Math.max(2000, input.maxChars ?? 12000);
    return content.length > maxChars ? `${content.slice(0, maxChars)}\n...` : content;
  } catch {
    return undefined;
  }
}

async function downloadSkillPackage(packageURL: string, hub: HubAuthConfig): Promise<Buffer> {
  const url = resolveHubURL(packageURL, hub.wsUrl);
  const response = await fetch(url, {
    headers: buildHubAuthHeaders(hub)
  });
  if (!response.ok) {
    throw new Error(`download skill package failed: HTTP ${response.status}`);
  }
  return Buffer.from(await response.arrayBuffer());
}

function resolveHubURL(rawURL: string, wsUrl: string): string {
  try {
    return new URL(rawURL).toString();
  } catch {
    const base = new URL(wsUrl);
    base.protocol = base.protocol === "wss:" ? "https:" : "http:";
    base.pathname = "/";
    base.search = "";
    base.hash = "";
    return new URL(rawURL, base).toString();
  }
}

function buildHubAuthHeaders(hub: HubAuthConfig): Record<string, string> {
  const authBase64 = Buffer.from(`${hub.botId}:${hub.secret}`).toString("base64");
  return {
    Authorization: `Bearer ${hub.secret}`,
    "Proxy-Authorization": `Basic ${authBase64}`,
    "X-Bot-Id": hub.botId,
    "X-Api-Key": hub.secret
  };
}

async function extractZipSafely(zipBytes: Buffer, destination: string): Promise<void> {
  const entries = readZipEntries(zipBytes);
  for (const entry of entries) {
    if (!entry.name || entry.name.endsWith("/")) {
      continue;
    }
    const safeName = normalizeZipEntryName(entry.name);
    const content = readZipEntryContent(zipBytes, entry);
    const target = resolve(destination, safeName);
    const normalizedDest = resolve(destination);
    if (target !== normalizedDest && !target.startsWith(`${normalizedDest}${sep}`)) {
      throw new Error(`zip entry escapes destination: ${entry.name}`);
    }
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, content);
  }
}

function readZipEntries(zipBytes: Buffer): ZipEntry[] {
  const eocdOffset = findEndOfCentralDirectory(zipBytes);
  const entryCount = zipBytes.readUInt16LE(eocdOffset + 10);
  const centralOffset = zipBytes.readUInt32LE(eocdOffset + 16);
  const entries: ZipEntry[] = [];
  let offset = centralOffset;
  for (let index = 0; index < entryCount; index += 1) {
    if (zipBytes.readUInt32LE(offset) !== ZIP_CENTRAL_SIGNATURE) {
      throw new Error("invalid zip central directory");
    }
    const flags = zipBytes.readUInt16LE(offset + 8);
    const method = zipBytes.readUInt16LE(offset + 10);
    const compressedSize = zipBytes.readUInt32LE(offset + 20);
    const fileNameLength = zipBytes.readUInt16LE(offset + 28);
    const extraLength = zipBytes.readUInt16LE(offset + 30);
    const commentLength = zipBytes.readUInt16LE(offset + 32);
    const localHeaderOffset = zipBytes.readUInt32LE(offset + 42);
    const name = zipBytes.subarray(offset + 46, offset + 46 + fileNameLength).toString("utf8");
    entries.push({ name, method, flags, compressedSize, localHeaderOffset });
    offset += 46 + fileNameLength + extraLength + commentLength;
  }
  return entries;
}

function findEndOfCentralDirectory(zipBytes: Buffer): number {
  const minOffset = Math.max(0, zipBytes.length - 65_557);
  for (let offset = zipBytes.length - 22; offset >= minOffset; offset -= 1) {
    if (zipBytes.readUInt32LE(offset) === ZIP_EOCD_SIGNATURE) {
      return offset;
    }
  }
  throw new Error("invalid zip: missing central directory");
}

function readZipEntryContent(zipBytes: Buffer, entry: ZipEntry): Buffer {
  if ((entry.flags & 0x1) !== 0) {
    throw new Error(`encrypted zip entry is not supported: ${entry.name}`);
  }
  const offset = entry.localHeaderOffset;
  if (zipBytes.readUInt32LE(offset) !== ZIP_LOCAL_SIGNATURE) {
    throw new Error(`invalid zip local header: ${entry.name}`);
  }
  const fileNameLength = zipBytes.readUInt16LE(offset + 26);
  const extraLength = zipBytes.readUInt16LE(offset + 28);
  const dataStart = offset + 30 + fileNameLength + extraLength;
  const compressed = zipBytes.subarray(dataStart, dataStart + entry.compressedSize);
  if (entry.method === 0) {
    return Buffer.from(compressed);
  }
  if (entry.method === 8) {
    return inflateRawSync(compressed);
  }
  throw new Error(`unsupported zip compression method ${entry.method}: ${entry.name}`);
}

function normalizeZipEntryName(name: string): string {
  const normalized = name.replace(/\\/g, "/").replace(/^\/+/, "");
  if (!normalized || normalized.includes("\0") || /^[A-Za-z]:/.test(normalized)) {
    throw new Error(`invalid zip entry path: ${name}`);
  }
  const segments = normalized.split("/").filter(Boolean);
  if (segments.some((segment) => segment === "." || segment === "..")) {
    throw new Error(`zip entry path traversal rejected: ${name}`);
  }
  return segments.join("/");
}

async function findExtractedSkillRoot(extractRoot: string): Promise<string> {
  if (existsSync(join(extractRoot, "SKILL.md"))) {
    return extractRoot;
  }
  const rootEntries = await import("node:fs/promises").then((fs) => fs.readdir(extractRoot, { withFileTypes: true }));
  const dirs = rootEntries.filter((entry) => entry.isDirectory()).map((entry) => join(extractRoot, entry.name));
  const matching = dirs.find((dir) => existsSync(join(dir, "SKILL.md")));
  if (!matching) {
    throw new Error("skill package missing SKILL.md");
  }
  return matching;
}

function resolveSkillInstallRoot(hostConfig: Record<string, unknown> | null, configPath: string): string {
  const configSkillRoot = resolve(dirname(configPath), "skills");
  const skills = toRecord(hostConfig?.skills);
  const load = toRecord(skills.load);
  const extraDirs = Array.isArray(load.extraDirs) ? load.extraDirs : [];
  const expandedExtraDirs = extraDirs
    .filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
    .map((entry) => resolve(expandHome(entry)));

  const configSkillRootEntry = expandedExtraDirs.find((entry) => entry === configSkillRoot);
  if (configSkillRootEntry) {
    return configSkillRootEntry;
  }

  const qclawUserSkillRoot = expandedExtraDirs.find((entry) => entry.endsWith(`${sep}.qclaw${sep}skills`));
  if (qclawUserSkillRoot) {
    return qclawUserSkillRoot;
  }

  if (configPath.includes(`${sep}.qclaw${sep}`)) {
    return configSkillRoot;
  }

  if (expandedExtraDirs[0]) {
    return expandedExtraDirs[0];
  }
  return configSkillRoot;
}

export const __skillInstallerTestExports = {
  buildEnabledSkillConfigEntry,
  resolveSkillInstallRoot
};

async function readJSONFile(path: string): Promise<Record<string, unknown> | null> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
  } catch {
    return null;
  }
}

async function backupHostConfig(configPath: string): Promise<void> {
  if (!existsSync(configPath)) {
    await mkdir(dirname(configPath), { recursive: true });
    await writeFile(configPath, "{}\n", "utf8");
    return;
  }
  await cp(configPath, `${configPath}.bak-${timestampSuffix()}`);
}

function ensureRecord(record: Record<string, unknown>, key: string): Record<string, unknown> {
  const existing = toRecord(record[key]);
  record[key] = existing;
  return existing;
}

function buildEnabledSkillConfigEntry(existingEntry: unknown): Record<string, unknown> {
  const next = {
    ...toRecord(existingEntry),
    enabled: true
  };
  delete next.source;
  delete next.skill_id;
  delete next.version;
  delete next.sha256;
  delete next.path;
  return next;
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function sanitizeSkillName(value: unknown): string {
  const name = typeof value === "string" ? value.trim() : "";
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,119}$/.test(name)) {
    return "";
  }
  return name;
}

function normalizeSHA256(value: unknown): string {
  const raw = typeof value === "string" ? value.trim().toLowerCase() : "";
  return /^[a-f0-9]{64}$/.test(raw) ? raw : "";
}

function expandHome(path: string): string {
  return path === "~" || path.startsWith("~/") ? resolve(homedir(), path.slice(2)) : path;
}

function timestampSuffix(): string {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function failed(request: EnsureHubSkillRequest, error: string): EnsureHubSkillResult {
  return {
    ok: false,
    status: "failed",
    skill_id: request.skill_id,
    skill_name: request.skill_name,
    display_name: request.display_name,
    version: request.version,
    sha256: normalizeSHA256(request.sha256),
    error
  };
}

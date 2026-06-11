import { existsSync } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { request } from "node:http";
import { homedir } from "node:os";
import { basename, dirname, join } from "node:path";

export type WorkBuddyRuntimeWorker = Record<string, unknown> & {
  pid?: number;
  sessionId?: string;
  cwd?: string;
  endpoint?: string;
  url?: string;
  version?: string;
  isCurrent?: boolean;
  healthy?: boolean;
};

export type WorkBuddyRuntimePlugin = Record<string, unknown> & {
  id: string;
  name: string;
  title?: string;
  description?: string;
  version?: string;
  status?: string;
  marketplace?: string;
  installedPath?: string;
  enabled?: boolean;
  skills?: WorkBuddyRuntimeSkill[];
};

export type WorkBuddyRuntimeSkill = Record<string, unknown> & {
  id: string;
  name: string;
  title?: string;
  description?: string;
  pluginName?: string;
  pluginVersion?: string;
  marketplace?: string;
  enabled?: boolean;
};

export type WorkBuddyRuntimeCronTask = Record<string, unknown> & {
  id?: string;
  name?: string;
  title?: string;
  enabled?: boolean;
  status?: string;
};

export type WorkBuddyRuntimeLocalSession = Record<string, unknown> & {
  sessionId?: string;
  endpoint?: string;
  url?: string;
  pid?: number;
  cwd?: string;
  version?: string;
  healthy?: boolean;
};

export type WorkBuddyRuntimeSnapshot = {
  info: Record<string, unknown>;
  workers: WorkBuddyRuntimeWorker[];
  plugins: WorkBuddyRuntimePlugin[];
  skills: WorkBuddyRuntimeSkill[];
  cronTasks: WorkBuddyRuntimeCronTask[];
  localSessions: WorkBuddyRuntimeLocalSession[];
  apiBaseUrls: string[];
  lastLoadedAt: string;
  errors: string[];
};

export type LoadWorkBuddyRuntimeInput = {
  workbuddyHome?: string;
  sessionId?: string;
  apiBaseUrls?: string[];
  timeoutMs?: number;
};

const DEFAULT_HTTP_TIMEOUT_MS = 800;

export async function pokeWorkBuddySessionRefresh(input: LoadWorkBuddyRuntimeInput = {}): Promise<{
  attempted: number;
  ok: number;
  endpoints: string[];
}> {
  const workbuddyHome = input.workbuddyHome || join(homedir(), ".workbuddy");
  const localSessions = await readLocalSessionFiles(workbuddyHome);
  const endpoints = uniqueStrings([
    ...normalizeApiBaseUrls(input.apiBaseUrls || []),
    ...localSessions.flatMap((session) => normalizeApiBaseUrls([session.endpoint, session.url]))
  ]);
  let ok = 0;
  await Promise.all(endpoints.flatMap((endpoint) => [
    fetchWorkBuddyApi(endpoint, "/api/v1/workers", input.timeoutMs)
      .then(() => {
        ok += 1;
      })
      .catch(() => {}),
    fetchWorkBuddyApi(endpoint, "/api/v1/sessions", input.timeoutMs)
      .then(() => {
        ok += 1;
      })
      .catch(() => {})
  ]));
  return {
    attempted: endpoints.length * 2,
    ok,
    endpoints
  };
}

export async function loadWorkBuddyRuntime(input: LoadWorkBuddyRuntimeInput = {}): Promise<WorkBuddyRuntimeSnapshot> {
  const workbuddyHome = input.workbuddyHome || join(homedir(), ".workbuddy");
  const sessionId = input.sessionId || "53aihub-workbuddy-shared";
  const errors: string[] = [];
  const localSessions = await readLocalSessionFiles(workbuddyHome);
  const apiBaseUrls = uniqueStrings([
    ...normalizeApiBaseUrls(input.apiBaseUrls || []),
    ...localSessions.flatMap((session) => normalizeApiBaseUrls([session.endpoint, session.url]))
  ]);
  const pluginFallback = await readLocalPlugins(workbuddyHome).catch((error) => {
    errors.push(`plugins: ${error instanceof Error ? error.message : String(error)}`);
    return [] as WorkBuddyRuntimePlugin[];
  });

  let info: Record<string, unknown> = {};
  let workers: WorkBuddyRuntimeWorker[] = localSessions.map((session) => ({
    ...session,
    sessionId: session.sessionId,
    endpoint: session.endpoint || session.url,
    healthy: isFreshSession(session)
  }));
  let plugins = pluginFallback;
  let cronTasks: WorkBuddyRuntimeCronTask[] = [];

  for (const baseUrl of apiBaseUrls) {
    const [infoResult, workersResult, pluginsResult, tasksResult] = await Promise.allSettled([
      fetchWorkBuddyApi(baseUrl, "/api/v1/info", input.timeoutMs),
      fetchWorkBuddyApi(baseUrl, "/api/v1/workers", input.timeoutMs),
      fetchWorkBuddyApi(baseUrl, "/api/v1/plugins", input.timeoutMs),
      fetchWorkBuddyApi(baseUrl, `/api/v1/scheduled-tasks?sessionId=${encodeURIComponent(sessionId)}`, input.timeoutMs)
    ]);

    if (infoResult.status === "fulfilled" && Object.keys(infoResult.value).length && !Object.keys(info).length) {
      info = unwrapDataRecord(infoResult.value);
    } else if (infoResult.status === "rejected") {
      errors.push(`${baseUrl}/info: ${infoResult.reason instanceof Error ? infoResult.reason.message : String(infoResult.reason)}`);
    }

    if (workersResult.status === "fulfilled") {
      workers = mergeByKey(workers, unwrapDataArray(workersResult.value).map((worker) => normalizeWorker(worker)));
    } else {
      errors.push(`${baseUrl}/workers: ${workersResult.reason instanceof Error ? workersResult.reason.message : String(workersResult.reason)}`);
    }

    if (pluginsResult.status === "fulfilled") {
      plugins = mergePlugins(plugins, unwrapDataArray(pluginsResult.value).map((plugin) => normalizePlugin(plugin)));
    } else {
      errors.push(`${baseUrl}/plugins: ${pluginsResult.reason instanceof Error ? pluginsResult.reason.message : String(pluginsResult.reason)}`);
    }

    if (tasksResult.status === "fulfilled") {
      cronTasks = mergeByKey(
        cronTasks,
        unwrapTasks(tasksResult.value).map((task) => normalizeCronTask(task))
      );
    } else {
      errors.push(`${baseUrl}/scheduled-tasks: ${tasksResult.reason instanceof Error ? tasksResult.reason.message : String(tasksResult.reason)}`);
    }
  }

  const skills = collectSkills(plugins);
  return {
    info,
    workers,
    plugins,
    skills,
    cronTasks,
    localSessions,
    apiBaseUrls,
    lastLoadedAt: new Date().toISOString(),
    errors
  };
}

async function readLocalSessionFiles(workbuddyHome: string): Promise<WorkBuddyRuntimeLocalSession[]> {
  const sessionsDir = join(workbuddyHome, "sessions");
  if (!existsSync(sessionsDir)) {
    return [];
  }
  const entries = await readdir(sessionsDir, { withFileTypes: true }).catch(() => []);
  const sessions: WorkBuddyRuntimeLocalSession[] = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) {
      continue;
    }
    const path = join(sessionsDir, entry.name);
    const parsed = await readJsonFile(path).catch(() => undefined);
    if (!parsed) {
      continue;
    }
    sessions.push({
      ...parsed,
      pid: toNumber(parsed.pid) || toNumber(basename(entry.name, ".json")),
      sessionId: readString(parsed.sessionId),
      endpoint: readString(parsed.endpoint) || readString(parsed.url),
      url: readString(parsed.url) || readString(parsed.endpoint),
      healthy: isFreshSession(parsed)
    });
  }
  return sessions.sort((left, right) => Number(right.updatedAt || 0) - Number(left.updatedAt || 0));
}

async function readLocalPlugins(workbuddyHome: string): Promise<WorkBuddyRuntimePlugin[]> {
  const enabledPlugins = await readEnabledPlugins(workbuddyHome);
  const pluginRoots = [join(workbuddyHome, "plugins", "marketplaces")];
  const manifests: string[] = [];
  for (const root of pluginRoots) {
    manifests.push(...await findPluginManifests(root));
  }

  const plugins: WorkBuddyRuntimePlugin[] = [];
  for (const manifestPath of manifests) {
    const parsed = await readJsonFile(manifestPath).catch(() => undefined);
    if (!parsed) {
      continue;
    }
    const installedPath = dirname(dirname(manifestPath));
    const marketplace = inferMarketplaceFromPluginPath(manifestPath);
    const name = readString(parsed.name) || basename(installedPath);
    const enabled = isPluginEnabled(enabledPlugins, name, marketplace);
    const plugin = normalizePlugin({
      ...parsed,
      marketplace,
      installedPath,
      status: enabled ? "enabled" : "installed",
      enabled
    });
    const discoveredSkills = await readPluginSkillFolders(installedPath, plugin);
    plugin.skills = mergeSkills(plugin.skills || [], discoveredSkills);
    plugins.push(plugin);
  }
  return mergePlugins([], plugins);
}

async function findPluginManifests(root: string): Promise<string[]> {
  if (!existsSync(root)) {
    return [];
  }
  const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
  const files: string[] = [];
  for (const entry of entries) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...await findPluginManifests(path));
    } else if (entry.isFile() && path.endsWith("/.codebuddy-plugin/plugin.json")) {
      files.push(path);
    }
  }
  return files;
}

async function readPluginSkillFolders(pluginRoot: string, plugin: WorkBuddyRuntimePlugin): Promise<WorkBuddyRuntimeSkill[]> {
  const skillsDir = join(pluginRoot, "skills");
  if (!existsSync(skillsDir)) {
    const topLevelSkill = join(pluginRoot, "SKILL.md");
    if (!existsSync(topLevelSkill)) {
      return [];
    }
    const description = await readSkillDescription(topLevelSkill);
    return [{
      id: `${plugin.name}`,
      name: plugin.name,
      title: plugin.title || plugin.name,
      description: description || plugin.description,
      pluginName: plugin.name,
      pluginVersion: plugin.version,
      marketplace: plugin.marketplace,
      enabled: plugin.enabled
    }];
  }

  const entries = await readdir(skillsDir, { withFileTypes: true }).catch(() => []);
  const skills: WorkBuddyRuntimeSkill[] = [];
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const skillPath = join(skillsDir, entry.name, "SKILL.md");
    if (!existsSync(skillPath)) {
      continue;
    }
    const description = await readSkillDescription(skillPath);
    skills.push({
      id: `${plugin.name}:${entry.name}`,
      name: entry.name,
      title: entry.name,
      description: description || plugin.description,
      pluginName: plugin.name,
      pluginVersion: plugin.version,
      marketplace: plugin.marketplace,
      enabled: plugin.enabled
    });
  }
  return skills;
}

async function readSkillDescription(path: string): Promise<string> {
  const raw = await readFile(path, "utf8").catch(() => "");
  const firstContentLine = raw
    .split(/\r?\n/)
    .map((line) => line.replace(/^#+\s*/, "").trim())
    .find((line) => line && !line.startsWith("---"));
  return firstContentLine || "";
}

async function readEnabledPlugins(workbuddyHome: string): Promise<Record<string, boolean>> {
  const settings = await readJsonFile(join(workbuddyHome, "settings.json")).catch(() => undefined);
  const enabledPlugins = toRecord(settings?.enabledPlugins);
  const result: Record<string, boolean> = {};
  for (const [key, value] of Object.entries(enabledPlugins)) {
    result[key] = value === true;
  }
  return result;
}

function isPluginEnabled(enabledPlugins: Record<string, boolean>, name: string, marketplace?: string): boolean {
  if (!Object.keys(enabledPlugins).length) {
    return true;
  }
  const keys = [name, marketplace ? `${name}@${marketplace}` : ""].filter(Boolean);
  return keys.some((key) => enabledPlugins[key] === true);
}

function inferMarketplaceFromPluginPath(path: string): string | undefined {
  const parts = path.split("/");
  const pluginsIndex = parts.lastIndexOf("plugins");
  if (pluginsIndex > 1) {
    return parts[pluginsIndex - 1];
  }
  return undefined;
}

async function fetchWorkBuddyApi(baseUrl: string, path: string, timeoutMs = DEFAULT_HTTP_TIMEOUT_MS): Promise<Record<string, unknown>> {
  const url = new URL(path, normalizeBaseUrl(baseUrl));
  return new Promise((resolve, reject) => {
    const req = request(
      url,
      {
        method: "GET",
        headers: {
          Accept: "application/json",
          "x-codebuddy-request": "1"
        },
        timeout: timeoutMs
      },
      (res) => {
        let raw = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          raw += chunk;
        });
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 400) {
            reject(new Error(`HTTP ${res.statusCode}: ${raw.slice(0, 160)}`));
            return;
          }
          try {
            const parsed = JSON.parse(raw || "{}");
            resolve(toRecord(parsed));
          } catch {
            reject(new Error(`invalid JSON: ${raw.slice(0, 160)}`));
          }
        });
      }
    );
    req.on("timeout", () => {
      req.destroy(new Error(`timeout after ${timeoutMs}ms`));
    });
    req.on("error", reject);
    req.end();
  });
}

function normalizeBaseUrl(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}

function normalizeApiBaseUrls(values: unknown[]): string[] {
  return values
    .map((value) => typeof value === "string" ? value.trim() : "")
    .filter((value) => /^https?:\/\/(?:127\.0\.0\.1|localhost|\[::1\])(?::\d+)?/i.test(value))
    .map((value) => value.replace(/\/+$/, ""));
}

function unwrapDataRecord(value: Record<string, unknown>): Record<string, unknown> {
  const data = toRecord(value.data);
  return Object.keys(data).length ? data : value;
}

function unwrapDataArray(value: Record<string, unknown>): Record<string, unknown>[] {
  const data = value.data;
  if (Array.isArray(data)) {
    return data.map(toRecord).filter((item) => Object.keys(item).length);
  }
  return [];
}

function unwrapTasks(value: Record<string, unknown>): Record<string, unknown>[] {
  const data = toRecord(value.data);
  if (Array.isArray(data.tasks)) {
    return data.tasks.map(toRecord).filter((item) => Object.keys(item).length);
  }
  if (Array.isArray(value.tasks)) {
    return value.tasks.map(toRecord).filter((item) => Object.keys(item).length);
  }
  return unwrapDataArray(value);
}

function normalizeWorker(value: Record<string, unknown>): WorkBuddyRuntimeWorker {
  return {
    ...value,
    pid: toNumber(value.pid),
    sessionId: readString(value.sessionId) || readString(value.session_id),
    endpoint: readString(value.endpoint) || readString(value.url),
    url: readString(value.url) || readString(value.endpoint),
    healthy: isFreshSession(value)
  };
}

function normalizePlugin(value: Record<string, unknown>): WorkBuddyRuntimePlugin {
  const name = readString(value.name) || readString(value.id) || "plugin";
  const marketplace = readString(value.marketplace);
  const status = readString(value.status) || (value.enabled === false ? "installed" : "enabled");
  const enabled = value.enabled === false ? false : status !== "disabled";
  const plugin: WorkBuddyRuntimePlugin = {
    ...value,
    id: readString(value.id) || (marketplace ? `${name}@${marketplace}` : name),
    name,
    title: readString(value.title) || name,
    description: readString(value.description),
    version: readString(value.version),
    status,
    marketplace,
    installedPath: readString(value.installedPath) || readString(value.installed_path),
    enabled
  };
  plugin.skills = normalizePluginSkills(value.skills, plugin);
  return plugin;
}

function normalizePluginSkills(value: unknown, plugin: WorkBuddyRuntimePlugin): WorkBuddyRuntimeSkill[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((entry, index) => {
      if (typeof entry === "string" && entry.trim()) {
        return {
          id: `${plugin.id}:${entry.trim()}`,
          name: entry.trim(),
          title: entry.trim(),
          pluginName: plugin.name,
          pluginVersion: plugin.version,
          marketplace: plugin.marketplace,
          enabled: plugin.enabled
        };
      }
      const record = toRecord(entry);
      const name = readString(record.name) || readString(record.id) || `skill-${index + 1}`;
      return {
        ...record,
        id: readString(record.id) || `${plugin.id}:${name}`,
        name,
        title: readString(record.title) || name,
        description: readString(record.description),
        pluginName: plugin.name,
        pluginVersion: plugin.version,
        marketplace: plugin.marketplace,
        enabled: plugin.enabled
      };
    })
    .filter((skill) => skill.name);
}

function normalizeCronTask(value: Record<string, unknown>): WorkBuddyRuntimeCronTask {
  return {
    ...value,
    id: readString(value.id) || readString(value.taskId) || readString(value.name),
    name: readString(value.name),
    title: readString(value.title) || readString(value.name),
    enabled: value.enabled === false ? false : undefined,
    status: readString(value.status)
  };
}

function collectSkills(plugins: WorkBuddyRuntimePlugin[]): WorkBuddyRuntimeSkill[] {
  const skills = plugins.flatMap((plugin) => {
    const pluginSkills = plugin.skills || [];
    if (pluginSkills.length) {
      return pluginSkills;
    }
    return [{
      id: plugin.id,
      name: plugin.name,
      title: plugin.title || plugin.name,
      description: plugin.description,
      pluginName: plugin.name,
      pluginVersion: plugin.version,
      marketplace: plugin.marketplace,
      enabled: plugin.enabled
    }];
  });
  return mergeSkills([], skills.filter((skill) => skill.enabled !== false));
}

function mergePlugins(current: WorkBuddyRuntimePlugin[], incoming: WorkBuddyRuntimePlugin[]): WorkBuddyRuntimePlugin[] {
  const merged = new Map<string, WorkBuddyRuntimePlugin>();
  for (const plugin of [...current, ...incoming]) {
    const key = plugin.id || `${plugin.name}@${plugin.marketplace || ""}`;
    const previous = merged.get(key);
    merged.set(key, {
      ...(previous || {}),
      ...plugin,
      skills: mergeSkills(previous?.skills || [], plugin.skills || [])
    });
  }
  return [...merged.values()].sort((left, right) => left.name.localeCompare(right.name));
}

function mergeSkills(current: WorkBuddyRuntimeSkill[], incoming: WorkBuddyRuntimeSkill[]): WorkBuddyRuntimeSkill[] {
  const merged = new Map<string, WorkBuddyRuntimeSkill>();
  for (const skill of [...current, ...incoming]) {
    const key = skill.id || `${skill.pluginName || ""}:${skill.name}`;
    merged.set(key, { ...(merged.get(key) || {}), ...skill });
  }
  return [...merged.values()].sort((left, right) => left.name.localeCompare(right.name));
}

function mergeByKey<T extends Record<string, unknown>>(current: T[], incoming: T[]): T[] {
  const merged = new Map<string, T>();
  for (const item of [...current, ...incoming]) {
    const key = readString(item.id) || readString(item.sessionId) || readString(item.name) || JSON.stringify(item);
    merged.set(key, { ...(merged.get(key) || {}), ...item });
  }
  return [...merged.values()];
}

async function readJsonFile(path: string): Promise<Record<string, unknown>> {
  const raw = await readFile(path, "utf8");
  const parsed = JSON.parse(raw);
  return toRecord(parsed);
}

function isFreshSession(value: Record<string, unknown>): boolean {
  const updatedAt = toNumber(value.updatedAt) || toNumber(value.lastHeartbeat);
  if (!updatedAt) {
    return false;
  }
  return Date.now() - updatedAt < 120_000;
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function readString(value: unknown): string {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function toNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

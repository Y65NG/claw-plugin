import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { delimiter, join, resolve } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export type CodexInstallation = {
  binPath: string;
  version: string;
  source: "env" | "path" | "known-path";
};

export type DetectCodexInstallationInput = {
  env?: NodeJS.ProcessEnv;
  candidatePaths?: string[];
  exists?: (path: string) => boolean;
  execFile?: typeof execFileAsync;
};

const CODEX_BIN_ENV_KEYS = ["HUB53AI_CODEX_BIN", "CODEX_BIN", "CODEX_PATH"] as const;

export function getDefaultCodexBinaryCandidates(env: NodeJS.ProcessEnv = process.env): Array<{
  path: string;
  source: CodexInstallation["source"];
}> {
  const candidates: Array<{ path: string; source: CodexInstallation["source"] }> = [];

  for (const key of CODEX_BIN_ENV_KEYS) {
    const value = env[key];
    if (typeof value === "string" && value.trim()) {
      candidates.push({ path: value.trim(), source: "env" });
    }
  }

  const pathValue = env.PATH || "";
  for (const directory of pathValue.split(delimiter).filter(Boolean)) {
    candidates.push({ path: join(directory, process.platform === "win32" ? "codex.cmd" : "codex"), source: "path" });
    if (process.platform === "win32") {
      candidates.push({ path: join(directory, "codex.exe"), source: "path" });
    }
  }

  candidates.push(
    { path: "/opt/homebrew/bin/codex", source: "known-path" },
    { path: "/usr/local/bin/codex", source: "known-path" },
    { path: "/Applications/Codex.app/Contents/Resources/codex", source: "known-path" },
    { path: join(homedir(), ".local", "bin", "codex"), source: "known-path" }
  );

  const seen = new Set<string>();
  return candidates
    .map((candidate) => ({ ...candidate, path: resolve(candidate.path) }))
    .filter((candidate) => {
      if (seen.has(candidate.path)) {
        return false;
      }
      seen.add(candidate.path);
      return true;
    });
}

export async function detectCodexInstallation(
  input: DetectCodexInstallationInput = {}
): Promise<CodexInstallation> {
  const env = input.env ?? process.env;
  const exists = input.exists ?? existsSync;
  const exec = input.execFile ?? execFileAsync;
  const candidates = input.candidatePaths
    ? input.candidatePaths.map((path) => ({ path: resolve(path), source: "known-path" as const }))
    : getDefaultCodexBinaryCandidates(env);

  const failures: string[] = [];
  for (const candidate of candidates) {
    if (!exists(candidate.path)) {
      continue;
    }
    try {
      const result = await exec(candidate.path, ["--version"], {
        timeout: 5_000,
        maxBuffer: 64 * 1024
      });
      const version = String(result.stdout || result.stderr || "").trim();
      if (!version.toLowerCase().includes("codex")) {
        failures.push(`${candidate.path}: unexpected version output`);
        continue;
      }
      return {
        binPath: candidate.path,
        version,
        source: candidate.source
      };
    } catch (error) {
      failures.push(`${candidate.path}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  throw new Error(
    [
      "could not auto-detect a local Codex executable.",
      "Install Codex CLI or Codex.app, ensure `codex` is on PATH, then rerun the installer.",
      ...(failures.length ? ["", "Detection failures:", ...failures.map((failure) => `- ${failure}`)] : [])
    ].join("\n")
  );
}

import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { __skillInstallerTestExports } from "../src/skill-installer";

const { buildEnabledSkillConfigEntry, resolveSkillInstallRoot } = __skillInstallerTestExports;

describe("skill installer", () => {
  it("prefers the QClaw user skill directory over earlier app bundled skill directories", () => {
    const configPath = join(homedir(), ".qclaw", "openclaw.json");
    const qclawSkillRoot = join(homedir(), ".qclaw", "skills");

    expect(
      resolveSkillInstallRoot(
        {
          skills: {
            load: {
              extraDirs: [
                "/Users/y65ng/Library/Application Support/QClaw/openclaw/config/skills",
                "~/.openclaw/workspace/skills",
                "~/.agents/skills",
                "~/.qclaw/skills",
                "~/.qclaw/skillhub-skills"
              ]
            }
          }
        },
        configPath
      )
    ).toBe(resolve(qclawSkillRoot));
  });

  it("falls back to the config-adjacent skills directory for QClaw configs", () => {
    const configPath = join(homedir(), ".qclaw", "openclaw.json");

    expect(resolveSkillInstallRoot(null, configPath)).toBe(resolve(dirname(configPath), "skills"));
  });

  it("enables QClaw skill entries without plugin metadata fields", () => {
    expect(
      buildEnabledSkillConfigEntry({
        enabled: false,
        config: { mode: "test" },
        source: "53aihub",
        skill_id: "UkLWZg",
        version: "0.1.0",
        sha256: "abc",
        path: "/Users/y65ng/.qclaw/skills/openclaw_pdf_probe"
      })
    ).toEqual({
      enabled: true,
      config: { mode: "test" }
    });
  });
});

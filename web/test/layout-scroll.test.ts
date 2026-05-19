import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

describe("layout scrolling styles", () => {
  it("keeps the session list and conversation body as independent scroll containers", () => {
    const css = readFileSync(resolve(import.meta.dirname, "../src/App.css"), "utf8");

    expect(css).toMatch(/\.sessions-panel\s*\{[\s\S]*grid-template-rows:\s*auto minmax\(0,\s*1fr\)/);
    expect(css).toMatch(/\.session-list\s*\{[\s\S]*overflow-y:\s*auto/);
    expect(css).toMatch(/\.session-list\s*\{[\s\S]*min-height:\s*0/);

    expect(css).toMatch(/\.conversation-panel\s*\{[\s\S]*grid-template-rows:\s*auto minmax\(0,\s*1fr\) auto/);
    expect(css).toMatch(/\.conversation-body\s*\{[\s\S]*overflow-y:\s*auto/);
    expect(css).toMatch(/\.conversation-body\s*\{[\s\S]*min-height:\s*0/);
  });

  it("lets activity card text wrap instead of getting clipped by the timestamp column", () => {
    const css = readFileSync(resolve(import.meta.dirname, "../src/App.css"), "utf8");

    expect(css).toMatch(/\.activity-card\s*\{[\s\S]*display:\s*flex[\s\S]*min-height:\s*96px[\s\S]*overflow:\s*visible/);
    expect(css).toMatch(/\.activity-header\s*\{[\s\S]*display:\s*flex[\s\S]*flex-direction:\s*column/);
    expect(css).toMatch(/\.activity-summary\s*\{[\s\S]*flex-direction:\s*column[\s\S]*min-width:\s*0/);
    expect(css).toMatch(/\.activity-meta\s*\{[\s\S]*width:\s*100%[\s\S]*justify-content:\s*space-between/);
    expect(css).toMatch(/\.activity-summary strong,\s*[\s\S]*\.activity-summary span\s*\{[\s\S]*overflow-wrap:\s*anywhere/);
    expect(css).toMatch(/\.activity-summary strong,\s*[\s\S]*\.activity-summary span\s*\{[\s\S]*display:\s*block/);
    expect(css).toMatch(/\.activity-details-toggle\s*\{[\s\S]*display:\s*inline-flex[\s\S]*line-height:\s*1\.2/);
  });

  it("lets stacked layouts grow vertically instead of squeezing the conversation panel", () => {
    const css = readFileSync(resolve(import.meta.dirname, "../src/App.css"), "utf8");

    expect(css).toMatch(/@media \(max-width: 1200px\)\s*\{[\s\S]*\.app-shell\s*\{[\s\S]*height:\s*auto/);
    expect(css).toMatch(/@media \(max-width: 1200px\)\s*\{[\s\S]*\.app-shell\s*\{[\s\S]*overflow-y:\s*auto/);
    expect(css).toMatch(/@media \(max-width: 1200px\)\s*\{[\s\S]*\.sessions-panel,\s*[\s\S]*\.conversation-panel\s*\{[\s\S]*min-height:\s*calc\(100vh - 32px\)/);
    expect(css).toMatch(/@media \(max-width: 1200px\)\s*\{[\s\S]*\.activity-header\s*\{[\s\S]*flex-direction:\s*column/);
    expect(css).toMatch(/@media \(max-width: 1200px\)\s*\{[\s\S]*\.activity-meta\s*\{[\s\S]*justify-content:\s*space-between[\s\S]*width:\s*100%/);
  });
});

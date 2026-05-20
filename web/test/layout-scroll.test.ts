import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

describe("layout scrolling styles", () => {
  it("keeps the session list and conversation body as independent scroll containers", () => {
    const css = readFileSync(resolve(import.meta.dirname, "../src/App.css"), "utf8");

    expect(css).toMatch(/\.sessions-panel,\s*[\s\S]*\.events-panel,\s*[\s\S]*\.sidebar-panel\s*\{[\s\S]*grid-template-rows:\s*auto minmax\(0,\s*1fr\)/);
    expect(css).toMatch(/\.session-list,\s*[\s\S]*\.sidebar-scroll,\s*[\s\S]*\.events-scroll\s*\{[\s\S]*overflow-y:\s*auto/);
    expect(css).toMatch(/\.session-list,\s*[\s\S]*\.sidebar-scroll,\s*[\s\S]*\.events-scroll\s*\{[\s\S]*min-height:\s*0/);

    expect(css).toMatch(/\.conversation-panel\s*\{[\s\S]*grid-template-rows:\s*auto minmax\(0,\s*1fr\) auto/);
    expect(css).toMatch(/\.conversation-body\s*\{[\s\S]*overflow-y:\s*auto/);
    expect(css).toMatch(/\.conversation-body\s*\{[\s\S]*min-height:\s*0/);
  });

  it("lets event row text wrap instead of getting clipped by the timestamp column", () => {
    const css = readFileSync(resolve(import.meta.dirname, "../src/App.css"), "utf8");

    expect(css).toMatch(/\.event-row summary\s*\{[\s\S]*display:\s*grid[\s\S]*grid-template-columns:\s*minmax\(0,\s*1fr\) auto/);
    expect(css).toMatch(/\.event-main\s*\{[\s\S]*min-width:\s*0/);
    expect(css).toMatch(/\.event-title\s*\{[\s\S]*overflow-wrap:\s*anywhere/);
    expect(css).toMatch(/\.event-summary\s*\{[\s\S]*overflow-wrap:\s*anywhere/);
    expect(css).toMatch(/\.event-row pre\s*\{[\s\S]*white-space:\s*pre-wrap[\s\S]*word-break:\s*break-word/);
  });

  it("lets stacked layouts grow vertically instead of squeezing the conversation panel", () => {
    const css = readFileSync(resolve(import.meta.dirname, "../src/App.css"), "utf8");

    expect(css).toMatch(/@media \(max-width: 1320px\)\s*\{[\s\S]*\.app-shell\s*\{[\s\S]*height:\s*auto/);
    expect(css).toMatch(/@media \(max-width: 1320px\)\s*\{[\s\S]*\.app-shell\s*\{[\s\S]*overflow-y:\s*auto/);
    expect(css).toMatch(/@media \(max-width: 1320px\)\s*\{[\s\S]*\.sessions-panel,\s*[\s\S]*\.conversation-panel\s*\{[\s\S]*min-height:\s*calc\(100vh - 36px\)/);
    expect(css).toMatch(/@media \(max-width: 1320px\)\s*\{[\s\S]*\.events-panel\s*\{[\s\S]*grid-column:\s*1 \/ 2/);
    expect(css).toMatch(/@media \(max-width: 1320px\)\s*\{[\s\S]*\.sidebar-panel\s*\{[\s\S]*grid-column:\s*2 \/ 3/);
  });
});

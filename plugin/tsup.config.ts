import { defineConfig } from "tsup";

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/install-qclaw.ts",
    "src/codebuddy-channel.ts",
    "src/codex-channel.ts",
    "src/workbuddy-supervisor.ts"
  ],
  format: ["cjs"],
  dts: true,
  platform: "node",
  noExternal: ["ws", "yaml", "@modelcontextprotocol/sdk", /^@inquirer\//]
});

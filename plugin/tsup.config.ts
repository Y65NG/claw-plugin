import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts", "src/install-qclaw.ts"],
  format: ["cjs"],
  dts: true,
  platform: "node",
  noExternal: ["ws", "yaml"]
});

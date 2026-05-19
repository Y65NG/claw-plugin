#!/usr/bin/env node

import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const { runInstallCommand } = require("../dist/install-qclaw.cjs");
const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

Promise.resolve(runInstallCommand({ argv: process.argv.slice(2), packageRoot })).catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});

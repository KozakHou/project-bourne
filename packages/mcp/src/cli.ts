#!/usr/bin/env node

import {
  defaultDependencies,
  doctor,
  launchServer,
  selectRuntime,
} from "./runtime.js";

async function main(arguments_: string[]): Promise<void> {
  const deps = defaultDependencies();
  const known = new Set(["--doctor", "--no-bootstrap"]);
  const unknown = arguments_.filter((argument) => !known.has(argument));
  if (unknown.length) {
    deps.stderr.write(`project-bourne-mcp: unknown option ${unknown[0]}\n`);
    process.exitCode = 2;
    return;
  }
  if (arguments_.includes("--doctor")) {
    process.exitCode = doctor(deps);
    return;
  }
  try {
    const selection = selectRuntime(deps, {
      noBootstrap: arguments_.includes("--no-bootstrap"),
    });
    const result = await launchServer(selection, deps);
    if (result.signal) {
      process.kill(process.pid, result.signal);
      return;
    }
    process.exitCode = result.code ?? 1;
  } catch (error) {
    deps.stderr.write(
      `project-bourne-mcp: ${error instanceof Error ? error.message : "launcher failed"}\n`,
    );
    process.exitCode = 1;
  }
}

await main(process.argv.slice(2));

import { EventEmitter } from "node:events";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import assert from "node:assert/strict";
import type { ChildProcess, SpawnOptions, SpawnSyncOptions } from "node:child_process";

import {
  NPM_VERSION,
  PYTHON_VERSION,
  cacheDirectory,
  doctor,
  launchServer,
  probePython,
  pythonCandidates,
  selectRuntime,
  type RuntimeDependencies,
  type SyncProcessResult,
} from "../src/runtime.js";

function syncResult(
  value: { status?: number | null; stdout?: string; stderr?: string; error?: Error } = {},
): SyncProcessResult {
  const result: SyncProcessResult = {
    stdout: value.stdout ?? "",
    status: value.status === undefined ? 0 : value.status,
  };
  if (value.error) result.error = value.error;
  return result;
}

function probeValue(
  python: [number, number, number] = [3, 12, 1],
  bourne: string | null = PYTHON_VERSION,
  mcp = true,
): SyncProcessResult {
  return syncResult({ stdout: JSON.stringify({ python, bourne, mcp }) });
}

function dependencies(
  root: string,
  runSync: RuntimeDependencies["spawnSync"],
  run = (() => {
    throw new Error("unexpected spawn");
  }) as RuntimeDependencies["spawn"],
): RuntimeDependencies {
  const messages: string[] = [];
  return {
    platform: "linux",
    environment: { XDG_CACHE_HOME: join(root, "cache") },
    home: root,
    stderr: { write: (message: string | Uint8Array) => {
      messages.push(String(message));
      return true;
    } },
    spawnSync: runSync,
    spawn: run,
    bourneRequirement: `bourneprov[mcp]==${PYTHON_VERSION}`,
  };
}

test("versions and cache are exactly coupled and platform appropriate", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne node Unicode 科學 "));
  const deps = dependencies(root, (() => probeValue()) as RuntimeDependencies["spawnSync"]);
  assert.equal(NPM_VERSION, "0.6.0-dev.0");
  assert.equal(PYTHON_VERSION, "0.6.0.dev0");
  assert.equal(
    cacheDirectory(deps),
    join(root, "cache", "project-bourne", "mcp", PYTHON_VERSION),
  );
});

test("an existing exact Python and Bourne runtime is selected without bootstrap", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne-node-"));
  const calls: Array<{ command: string; args: readonly string[]; options: SpawnSyncOptions }> = [];
  const runSync: RuntimeDependencies["spawnSync"] = ((command, args, options) => {
    calls.push({ command: String(command), args: args ?? [], options: options ?? {} });
    return probeValue();
  }) as RuntimeDependencies["spawnSync"];
  const selected = selectRuntime(dependencies(root, runSync), { noBootstrap: false });
  assert.equal(selected.source, "installed");
  assert.equal(selected.command.command, "python3");
  assert.equal(calls.length, 2);
  assert.ok(calls.every((call) => call.options.shell === false));
  assert.ok(calls.every((call) => call.args.includes("-I")));
});

test("BOURNE_PYTHON paths with spaces and Unicode remain one argv element", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne-node-"));
  const expected = join(root, "Python 科學", "bin", "python 3");
  const calls: string[] = [];
  const deps = dependencies(root, ((command) => {
    calls.push(String(command));
    return probeValue();
  }) as RuntimeDependencies["spawnSync"]);
  deps.environment.BOURNE_PYTHON = expected;
  const selected = selectRuntime(deps, { noBootstrap: true });
  assert.equal(selected.command.command, expected);
  assert.equal(calls[0], expected);
});

test("no-bootstrap, missing Python, and incompatible Python fail explicitly", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne-node-"));
  const noBourne = dependencies(
    root,
    (() => probeValue([3, 12, 0], null, false)) as RuntimeDependencies["spawnSync"],
  );
  assert.throws(
    () => selectRuntime(noBourne, { noBootstrap: true }),
    /--no-bootstrap/,
  );
  const missing = dependencies(
    mkdtempSync(join(tmpdir(), "bourne-node-")),
    (() => syncResult({ status: null, error: new Error("ENOENT") })) as RuntimeDependencies["spawnSync"],
  );
  assert.throws(
    () => selectRuntime(missing, { noBootstrap: false }),
    /No compatible Python/,
  );
  const old = dependencies(
    mkdtempSync(join(tmpdir(), "bourne-node-")),
    (() => probeValue([3, 9, 20], null, false)) as RuntimeDependencies["spawnSync"],
  );
  assert.throws(
    () => selectRuntime(old, { noBootstrap: false }),
    /No compatible Python/,
  );
});

test("an incompatible Bourne version is never treated as a compatible runtime", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne-node-"));
  const incompatible = dependencies(
    root,
    (() => probeValue([3, 12, 0], "0.5.0", true)) as RuntimeDependencies["spawnSync"],
  );
  assert.throws(
    () => selectRuntime(incompatible, { noBootstrap: true }),
    /No compatible bourneprov 0\.6\.0\.dev0 MCP runtime/,
  );
});

test("Windows uses py argv and a private LocalAppData runtime path", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne Windows Unicode 科學 "));
  const deps = dependencies(root, (() => probeValue()) as RuntimeDependencies["spawnSync"]);
  deps.platform = "win32";
  deps.environment.LOCALAPPDATA = join(root, "Local App Data");
  const candidates = pythonCandidates(deps);
  assert.deepEqual(candidates[0], { command: "py", prefix: ["-3"], label: "py -3" });
  assert.equal(
    cacheDirectory(deps),
    join(root, "Local App Data", "project-bourne", "mcp", PYTHON_VERSION),
  );
});

test("bootstrap uses a private versioned venv and an exact install without a shell", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne node bootstrap 科學 "));
  const calls: Array<{ command: string; args: readonly string[]; options: SpawnSyncOptions }> = [];
  const runSync: RuntimeDependencies["spawnSync"] = ((command, args, options) => {
    const normalized = args ?? [];
    calls.push({ command: String(command), args: normalized, options: options ?? {} });
    if (String(command).includes(join("runtime", "bin", "python"))) return probeValue();
    if (normalized.includes("-c")) return probeValue([3, 12, 0], null, false);
    return syncResult();
  }) as RuntimeDependencies["spawnSync"];
  const deps = dependencies(root, runSync);
  const selected = selectRuntime(deps, { noBootstrap: false });
  assert.equal(selected.source, "bootstrapped");
  const venv = calls.find((call) => call.args.includes("venv"));
  const pip = calls.find((call) => call.args.includes("pip"));
  assert.ok(venv);
  assert.ok(pip);
  assert.ok(venv.args.join("\0").includes(cacheDirectory(deps)));
  assert.ok(pip.args.includes(`bourneprov[mcp]==${PYTHON_VERSION}`));
  assert.ok(calls.every((call) => call.options.shell === false));
  assert.ok(!calls.some((call) => call.args.includes("latest")));
  assert.ok(!calls.some((call) => call.args.includes("sudo")));
});

test("doctor only probes and never bootstraps or launches", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne-node-"));
  const calls: readonly string[][] = [];
  const deps = dependencies(root, ((_, args) => {
    (calls as string[][]).push([...(args ?? [])]);
    return probeValue([3, 12, 0], null, false);
  }) as RuntimeDependencies["spawnSync"]);
  assert.equal(doctor(deps), 0);
  assert.ok(calls.every((args) => args.includes("-c")));
  assert.ok(calls.every((args) => !args.includes("venv") && !args.includes("pip")));
});

class FakeChild extends EventEmitter {
  killed = false;
  signals: (NodeJS.Signals | number | undefined)[] = [];

  kill(signal?: NodeJS.Signals | number): boolean {
    this.killed = true;
    this.signals.push(signal);
    return true;
  }
}

test("launch uses structured argv, forwards signals, and preserves exit status", async () => {
  const root = mkdtempSync(join(tmpdir(), "bourne-node-"));
  const child = new FakeChild();
  let invocation: { command: string; args: readonly string[]; options: SpawnOptions } | undefined;
  const run: RuntimeDependencies["spawn"] = ((command, args, options) => {
    invocation = { command: String(command), args: args ?? [], options: options ?? {} };
    return child as unknown as ChildProcess;
  }) as RuntimeDependencies["spawn"];
  const deps = dependencies(root, (() => probeValue()) as RuntimeDependencies["spawnSync"], run);
  const before = new Set(process.listeners("SIGTERM"));
  const launched = launchServer(
    {
      command: { command: join(root, "Python Space", "python"), prefix: [], label: "test" },
      source: "installed",
      cache: cacheDirectory(deps),
    },
    deps,
  );
  const forwarding = process.listeners("SIGTERM").find((listener) => !before.has(listener));
  assert.ok(forwarding);
  forwarding("SIGTERM");
  assert.deepEqual(child.signals, ["SIGTERM"]);
  child.emit("exit", 37, null);
  const result = await launched;
  assert.deepEqual(result, { code: 37, signal: null });
  assert.equal(invocation?.command, join(root, "Python Space", "python"));
  assert.deepEqual(invocation?.args, ["-m", "bourneprov", "mcp"]);
  assert.equal(invocation?.options.shell, false);
  assert.equal(invocation?.options.stdio, "inherit");
  assert.equal(process.listeners("SIGTERM").length, before.size);
});

test("malformed probe output is rejected rather than guessed", () => {
  const root = mkdtempSync(join(tmpdir(), "bourne-node-"));
  const deps = dependencies(
    root,
    (() => syncResult({ stdout: "not json" })) as RuntimeDependencies["spawnSync"],
  );
  const result = probePython(pythonCandidates(deps)[0]!, deps);
  assert.equal(result.probe, null);
  assert.match(result.diagnostic ?? "", /JSON/);
});

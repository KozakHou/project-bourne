import {
  spawn,
  spawnSync,
  type ChildProcess,
  type SpawnOptions,
  type SpawnSyncOptions,
} from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export const NPM_VERSION = "0.8.0-dev.0";
export const PYTHON_VERSION = "0.8.0.dev0";
export const MINIMUM_PYTHON = [3, 10] as const;

const PROBE = [
  "import importlib.metadata as metadata",
  "import importlib.util as util",
  "import json",
  "import sys",
  "try:",
  "    bourne = metadata.version('bourneprov')",
  "except metadata.PackageNotFoundError:",
  "    bourne = None",
  "print(json.dumps({'python': list(sys.version_info[:3]), 'bourne': bourne, 'mcp': util.find_spec('mcp') is not None}))",
].join("\n");

export interface PythonCommand {
  command: string;
  prefix: string[];
  label: string;
}

export interface RuntimeProbe {
  python: [number, number, number];
  bourne: string | null;
  mcp: boolean;
}

export interface ProbedCandidate {
  candidate: PythonCommand;
  probe: RuntimeProbe | null;
  diagnostic: string | null;
}

export interface LauncherEnvironment {
  [key: string]: string | undefined;
}

export interface SyncProcessResult {
  status: number | null;
  stdout: string | Buffer;
  error?: Error;
}

export type SpawnSyncFunction = (
  command: string,
  arguments_: readonly string[],
  options: SpawnSyncOptions,
) => SyncProcessResult;

export type SpawnFunction = (
  command: string,
  arguments_: readonly string[],
  options: SpawnOptions,
) => ChildProcess;

export interface RuntimeDependencies {
  platform: NodeJS.Platform;
  environment: LauncherEnvironment;
  home: string;
  stderr: Pick<NodeJS.WriteStream, "write">;
  spawnSync: SpawnSyncFunction;
  spawn: SpawnFunction;
  bourneRequirement: string;
}

export interface RuntimeSelection {
  command: PythonCommand;
  source: "installed" | "cache" | "bootstrapped";
  cache: string;
}

export interface LaunchResult {
  code: number | null;
  signal: NodeJS.Signals | null;
}

export const defaultDependencies = (): RuntimeDependencies => ({
  platform: process.platform,
  environment: process.env,
  home: homedir(),
  stderr: process.stderr,
  spawnSync: (command, arguments_, options) =>
    spawnSync(command, [...arguments_], options),
  spawn: (command, arguments_, options) =>
    spawn(command, [...arguments_], options),
  bourneRequirement: `bourneprov[mcp]==${PYTHON_VERSION}`,
});

export function cacheDirectory(deps: RuntimeDependencies): string {
  let base: string;
  if (deps.platform === "darwin") {
    base = join(deps.home, "Library", "Caches");
  } else if (deps.platform === "win32") {
    base = deps.environment.LOCALAPPDATA ?? join(deps.home, "AppData", "Local");
  } else {
    base = deps.environment.XDG_CACHE_HOME ?? join(deps.home, ".cache");
  }
  return join(base, "project-bourne", "mcp", PYTHON_VERSION);
}

export function pythonCandidates(deps: RuntimeDependencies): PythonCommand[] {
  const candidates: PythonCommand[] = [];
  if (deps.environment.BOURNE_PYTHON) {
    candidates.push({
      command: deps.environment.BOURNE_PYTHON,
      prefix: [],
      label: "BOURNE_PYTHON",
    });
  }
  if (deps.platform === "win32") {
    candidates.push(
      { command: "py", prefix: ["-3"], label: "py -3" },
      { command: "python", prefix: [], label: "python" },
    );
  } else {
    candidates.push(
      { command: "python3", prefix: [], label: "python3" },
      { command: "python", prefix: [], label: "python" },
    );
  }
  return candidates.filter(
    (candidate, index, all) =>
      all.findIndex(
        (item) => item.command === candidate.command && item.prefix.join("\0") === candidate.prefix.join("\0"),
      ) === index,
  );
}

export function cachedPython(cache: string, platform: NodeJS.Platform): PythonCommand {
  return {
    command: join(cache, "runtime", platform === "win32" ? "Scripts" : "bin", platform === "win32" ? "python.exe" : "python"),
    prefix: [],
    label: "private cache",
  };
}

export function probePython(
  candidate: PythonCommand,
  deps: RuntimeDependencies,
): ProbedCandidate {
  let result: SyncProcessResult;
  try {
    result = deps.spawnSync(
      candidate.command,
      [...candidate.prefix, "-I", "-c", PROBE],
      {
        encoding: "utf8",
        shell: false,
        timeout: 10_000,
        env: deps.environment as NodeJS.ProcessEnv,
        windowsHide: true,
      },
    );
  } catch (error) {
    return {
      candidate,
      probe: null,
      diagnostic: error instanceof Error ? error.message : "probe failed",
    };
  }
  if (result.error || result.status !== 0) {
    return {
      candidate,
      probe: null,
      diagnostic: result.error?.message ?? `probe exited ${String(result.status)}`,
    };
  }
  try {
    const value = JSON.parse(String(result.stdout)) as Partial<RuntimeProbe>;
    if (
      !Array.isArray(value.python) ||
      value.python.length !== 3 ||
      !value.python.every(Number.isInteger) ||
      (value.bourne !== null && typeof value.bourne !== "string") ||
      typeof value.mcp !== "boolean"
    ) {
      throw new Error("unexpected probe structure");
    }
    return {
      candidate,
      probe: value as RuntimeProbe,
      diagnostic: null,
    };
  } catch (error) {
    return {
      candidate,
      probe: null,
      diagnostic: error instanceof Error ? error.message : "invalid probe output",
    };
  }
}

export function isSupportedPython(probe: RuntimeProbe): boolean {
  const [major, minor] = probe.python;
  return major > MINIMUM_PYTHON[0] || (major === MINIMUM_PYTHON[0] && minor >= MINIMUM_PYTHON[1]);
}

export function isCompatibleRuntime(probe: RuntimeProbe): boolean {
  return isSupportedPython(probe) && probe.bourne === PYTHON_VERSION && probe.mcp;
}

function report(deps: RuntimeDependencies, message: string): void {
  deps.stderr.write(`${message}\n`);
}

function runBootstrapStep(
  deps: RuntimeDependencies,
  command: PythonCommand,
  arguments_: string[],
  label: string,
): void {
  const result = deps.spawnSync(
    command.command,
    [...command.prefix, ...arguments_],
    {
      shell: false,
      stdio: ["ignore", process.stderr, process.stderr],
      env: deps.environment as NodeJS.ProcessEnv,
      windowsHide: true,
    },
  );
  if (result.error || result.status !== 0) {
    throw new Error(`${label} failed${result.error ? `: ${result.error.message}` : ` with exit ${String(result.status)}`}`);
  }
}

export function selectRuntime(
  deps: RuntimeDependencies,
  options: { noBootstrap: boolean },
): RuntimeSelection {
  const cache = cacheDirectory(deps);
  const installed = pythonCandidates(deps).map((candidate) => probePython(candidate, deps));
  const compatible = installed.find((item) => item.probe && isCompatibleRuntime(item.probe));
  if (compatible) {
    return { command: compatible.candidate, source: "installed", cache };
  }

  const cached = cachedPython(cache, deps.platform);
  if (existsSync(cached.command)) {
    const cachedProbe = probePython(cached, deps);
    if (cachedProbe.probe && isCompatibleRuntime(cachedProbe.probe)) {
      return { command: cached, source: "cache", cache };
    }
  }

  if (options.noBootstrap) {
    throw new Error(
      `No compatible bourneprov ${PYTHON_VERSION} MCP runtime was found and --no-bootstrap was specified.`,
    );
  }

  const bootstrapPython = installed.find(
    (item): item is ProbedCandidate & { probe: RuntimeProbe } =>
      item.probe !== null && isSupportedPython(item.probe),
  );
  if (!bootstrapPython) {
    throw new Error("No compatible Python >= 3.10 was found for the isolated Bourne runtime.");
  }

  report(deps, "Project Bourne MCP runtime not found.");
  report(deps, `Bootstrapping isolated bourneprov ${PYTHON_VERSION} runtime in ${cache}.`);
  mkdirSync(cache, { recursive: true });
  runBootstrapStep(
    deps,
    bootstrapPython.candidate,
    ["-m", "venv", join(cache, "runtime")],
    "private virtual environment creation",
  );
  runBootstrapStep(
    deps,
    cached,
    ["-m", "pip", "install", deps.bourneRequirement],
    "exact Bourne runtime installation",
  );
  const finalProbe = probePython(cached, deps);
  if (!finalProbe.probe || !isCompatibleRuntime(finalProbe.probe)) {
    throw new Error("The isolated Bourne runtime failed its compatibility check.");
  }
  return { command: cached, source: "bootstrapped", cache };
}

export function doctor(deps: RuntimeDependencies): number {
  const cache = cacheDirectory(deps);
  report(deps, `Project Bourne MCP launcher ${NPM_VERSION}`);
  report(deps, `Node ${process.version} (${deps.platform})`);
  report(deps, `Expected bourneprov ${PYTHON_VERSION} with MCP support`);
  report(deps, `Private cache: ${cache}`);
  let selected: string | null = null;
  for (const candidate of pythonCandidates(deps)) {
    const result = probePython(candidate, deps);
    if (!result.probe) {
      report(deps, `${candidate.label}: unavailable (${result.diagnostic ?? "unknown error"})`);
      continue;
    }
    report(
      deps,
      `${candidate.label}: Python ${result.probe.python.join(".")}; bourneprov ${result.probe.bourne ?? "not installed"}; MCP ${result.probe.mcp ? "available" : "unavailable"}`,
    );
    if (selected === null && isCompatibleRuntime(result.probe)) {
      selected = `${candidate.label} (installed)`;
    }
  }
  const cacheProbe = existsSync(cachedPython(cache, deps.platform).command)
    ? probePython(cachedPython(cache, deps.platform), deps)
    : null;
  if (selected === null && cacheProbe?.probe && isCompatibleRuntime(cacheProbe.probe)) {
    selected = "compatible private cache";
  }
  report(deps, `Selected runtime: ${selected ?? "none"}`);
  return 0;
}

export function launchServer(
  selection: RuntimeSelection,
  deps: RuntimeDependencies,
): Promise<LaunchResult> {
  return new Promise((resolve, reject) => {
    let child: ChildProcess;
    try {
      child = deps.spawn(
        selection.command.command,
        [...selection.command.prefix, "-m", "bourneprov", "mcp"],
        {
          shell: false,
          stdio: "inherit",
          env: deps.environment as NodeJS.ProcessEnv,
          windowsHide: true,
        },
      );
    } catch (error) {
      reject(error);
      return;
    }
    const forwarded = new Map<NodeJS.Signals, () => void>();
    for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as NodeJS.Signals[]) {
      const handler = (): void => {
        if (!child.killed) child.kill(signal);
      };
      forwarded.set(signal, handler);
      process.on(signal, handler);
    }
    const cleanup = (): void => {
      for (const [signal, handler] of forwarded) process.off(signal, handler);
    };
    child.once("error", (error) => {
      cleanup();
      reject(error);
    });
    child.once("exit", (code, signal) => {
      cleanup();
      resolve({ code, signal });
    });
  });
}

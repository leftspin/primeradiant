#!/opt/homebrew/bin/node
import { accessSync, constants } from "node:fs";
import { spawn } from "node:child_process";

const dbPath = "/Volumes/Microverse/openclaw/state/.openclaw/subspace-daemon/data/daemon.sqlite3";
const loopPath = "/Users/mike/.local/libexec/primeradiant/t328-live-watcher-loop.sh";

function run(command, args) {
  let terminationTimer;
  let terminating = false;
  const child = spawn(command, args, {
    cwd: "/Users/mike/.openclaw/workspace",
    detached: true,
    env: {
      ...process.env,
      PATH: "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
    },
    stdio: "inherit",
  });

  for (const signal of ["SIGTERM", "SIGINT"]) {
    process.on(signal, () => {
      if (terminating) return;
      terminating = true;
      try {
        process.kill(-child.pid, signal);
      } catch (error) {
        if (error.code !== "ESRCH") throw error;
      }
      terminationTimer = setTimeout(() => {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch (error) {
          if (error.code !== "ESRCH") throw error;
        }
        process.exit(signal === "SIGTERM" ? 143 : 130);
      }, 5000);
      terminationTimer.unref();
    });
  }

  child.on("exit", (code, signal) => {
    if (terminationTimer) clearTimeout(terminationTimer);
    if (signal) {
      process.exit(signal === "SIGTERM" ? 143 : signal === "SIGINT" ? 130 : 1);
    }
    process.exit(code ?? 0);
  });
}

if (process.argv.includes("--probe")) {
  accessSync(dbPath, constants.R_OK);
  run("/usr/bin/sqlite3", [
    "-readonly",
    dbPath,
    "select id || '|' || message_timestamp from daemon_event order by id desc limit 1;",
  ]);
} else {
  run("/bin/bash", [loopPath]);
}

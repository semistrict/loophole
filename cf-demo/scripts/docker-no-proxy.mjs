#!/usr/bin/env node

/**
 * Thin docker CLI wrapper that strips DOCKER_HOST so CLI commands
 * (build, ps, rm) always talk to the real Docker socket, not our
 * FUSE-injecting proxy. Workerd's engine API still uses DOCKER_HOST.
 */

import { execFileSync } from "node:child_process";

const env = { ...process.env };
delete env.DOCKER_HOST;

try {
  execFileSync("docker", process.argv.slice(2), { stdio: "inherit", env });
} catch (err) {
  process.exit(err.status ?? 1);
}

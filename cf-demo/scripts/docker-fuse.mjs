#!/usr/bin/env node

/**
 * Docker socket proxy that injects FUSE support into container creation.
 *
 * Workerd creates containers via the Docker Engine API (not the CLI), so
 * WRANGLER_DOCKER_BIN only affects build/ps/rm commands. To add --privileged
 * and /dev/fuse to containers, we proxy the Docker socket and intercept
 * POST /containers/create requests.
 *
 * Usage:
 *   node scripts/docker-fuse.mjs &          # starts proxy on /tmp/docker-fuse.sock
 *   DOCKER_HOST=unix:///tmp/docker-fuse.sock pnpm run dev
 *
 * Or use the "dev:fuse" npm script which does both.
 */

import http from "node:http";
import net from "node:net";
import fs from "node:fs";

const REAL_SOCKET = process.env.REAL_DOCKER_SOCK || "/var/run/docker.sock";
const PROXY_SOCKET = process.env.PROXY_DOCKER_SOCK || "/tmp/docker-fuse.sock";

// Clean up stale socket file
try { fs.unlinkSync(PROXY_SOCKET); } catch {}

const server = http.createServer(async (req, res) => {
  // Intercept container creation to inject privileged + fuse device
  if (req.method === "POST" && req.url?.match(/\/containers\/create/)) {
    let body = "";
    for await (const chunk of req) body += chunk;

    try {
      const config = JSON.parse(body);

      // Inject the permissions gVisor needs in local Docker.
      config.HostConfig = config.HostConfig || {};
      config.HostConfig.Privileged = true;
      config.HostConfig.CapAdd = ["ALL"];
      config.HostConfig.SecurityOpt = [
        "seccomp=unconfined",
        "apparmor=unconfined",
        "label=disable",
      ];
      config.Env = config.Env || [];
      const envMap = new Map(
        config.Env.map((entry) => {
          const idx = entry.indexOf("=");
          return idx === -1
            ? [entry, ""]
            : [entry.slice(0, idx), entry.slice(idx + 1)];
        }),
      );
      envMap.set("LOOPHOLE_SANDBOXD_RUNSC_UNSAFE_NONROOT", "true");
      config.Env = [...envMap.entries()].map(([name, value]) => `${name}=${value}`);
      config.HostConfig.Devices = config.HostConfig.Devices || [];
      const hasFuse = config.HostConfig.Devices.some(
        (d) => d.PathOnHost === "/dev/fuse",
      );
      if (!hasFuse) {
        config.HostConfig.Devices.push({
          PathOnHost: "/dev/fuse",
          PathInContainer: "/dev/fuse",
          CgroupPermissions: "rwm",
        });
      }

      body = JSON.stringify(config);
      console.log("[docker-fuse] Injected privileged + unconfined seccomp/apparmor + /dev/fuse into container create");
    } catch {
      // If body isn't JSON, pass through unchanged
    }

    // Forward modified request to real Docker
    const options = {
      socketPath: REAL_SOCKET,
      path: req.url,
      method: req.method,
      headers: { ...req.headers, "content-length": Buffer.byteLength(body) },
    };

    const proxyReq = http.request(options, (proxyRes) => {
      res.writeHead(proxyRes.statusCode, proxyRes.headers);
      proxyRes.pipe(res);
    });
    proxyReq.on("error", (err) => {
      console.error("[docker-fuse] proxy error:", err.message);
      res.writeHead(502);
      res.end("Docker proxy error");
    });
    proxyReq.end(body);
    return;
  }

  // All other requests: pipe through transparently
  const options = {
    socketPath: REAL_SOCKET,
    path: req.url,
    method: req.method,
    headers: req.headers,
  };

  const proxyReq = http.request(options, (proxyRes) => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res);
  });
  proxyReq.on("error", (err) => {
    console.error("[docker-fuse] proxy error:", err.message);
    res.writeHead(502);
    res.end("Docker proxy error");
  });
  req.pipe(proxyReq);
});

// Handle CONNECT / upgrade for attach/exec streams
server.on("upgrade", (req, clientSocket, head) => {
  const dockerSocket = net.createConnection({ path: REAL_SOCKET }, () => {
    const reqLine = `${req.method} ${req.url} HTTP/${req.httpVersion}\r\n`;
    const headers = Object.entries(req.headers)
      .map(([k, v]) => `${k}: ${v}`)
      .join("\r\n");
    dockerSocket.write(reqLine + headers + "\r\n\r\n");
    if (head.length) dockerSocket.write(head);
    dockerSocket.pipe(clientSocket);
    clientSocket.pipe(dockerSocket);
  });
  dockerSocket.on("error", () => clientSocket.destroy());
  clientSocket.on("error", () => dockerSocket.destroy());
});

server.listen(PROXY_SOCKET, () => {
  console.log(`[docker-fuse] Proxy listening on ${PROXY_SOCKET}`);
  console.log(`[docker-fuse] Forwarding to ${REAL_SOCKET}`);
  console.log(`[docker-fuse] Injecting: Privileged=true, /dev/fuse device`);
});

process.on("SIGINT", () => { try { fs.unlinkSync(PROXY_SOCKET); } catch {} process.exit(0); });
process.on("SIGTERM", () => { try { fs.unlinkSync(PROXY_SOCKET); } catch {} process.exit(0); });

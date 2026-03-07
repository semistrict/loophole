// Loophole Web Console — just-bash shell on an lwext4 volume via WASM.

import { Bash } from "just-bash/browser";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import { LoopholeFs } from "./loophole-fs.ts";
import { createS3Adapter } from "./s3.ts";

const status = document.getElementById("status")!;
const termEl = document.getElementById("terminal")!;
const pickerEl = document.getElementById("volume-picker")!;
const volumeListEl = document.getElementById("volume-list")! as HTMLUListElement;
const newVolumeInput = document.getElementById("new-volume-name")! as HTMLInputElement;
const createBtn = document.getElementById("create-btn")! as HTMLButtonElement;
const pickerError = document.getElementById("picker-error")!;
const actionsEl = document.getElementById("actions")!;
const snapshotBtn = document.getElementById("snapshot-btn")! as HTMLButtonElement;
const cloneBtn = document.getElementById("clone-btn")! as HTMLButtonElement;
const unmountBtn = document.getElementById("unmount-btn")! as HTMLButtonElement;

// Dialog elements
const dialogOverlay = document.getElementById("dialog-overlay")!;
const dialogTitle = document.getElementById("dialog-title")!;
const dialogInput = document.getElementById("dialog-input")! as HTMLInputElement;
const dialogOk = document.getElementById("dialog-ok")! as HTMLButtonElement;
const dialogCancel = document.getElementById("dialog-cancel")! as HTMLButtonElement;
const dialogError = document.getElementById("dialog-error")!;

// --- S3 config from URL params or defaults ---
const params = new URLSearchParams(location.search);
const s3Endpoint = params.get("s3") || "http://localhost:9000";
const bucket = params.get("bucket") || "testbucket";
const s3Prefix = params.get("prefix") || "web/";

let lh: any;

// Show a small dialog and return the entered name, or null if cancelled.
function showDialog(title: string, placeholder: string): Promise<string | null> {
  dialogTitle.textContent = title;
  dialogInput.placeholder = placeholder;
  dialogInput.value = "";
  dialogError.textContent = "";
  dialogOverlay.classList.add("active");
  dialogInput.focus();

  return new Promise<string | null>((resolve) => {
    function cleanup() {
      dialogOverlay.classList.remove("active");
      dialogOk.removeEventListener("click", onOk);
      dialogCancel.removeEventListener("click", onCancel);
      dialogInput.removeEventListener("keydown", onKey);
    }
    function onOk() {
      const val = dialogInput.value.trim();
      if (!val) { dialogError.textContent = "Enter a name."; return; }
      if (!/^[a-zA-Z0-9_-]+$/.test(val)) {
        dialogError.textContent = "Use only letters, numbers, hyphens, underscores.";
        return;
      }
      cleanup();
      resolve(val);
    }
    function onCancel() { cleanup(); resolve(null); }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Enter") onOk();
      if (e.key === "Escape") onCancel();
    }
    dialogOk.addEventListener("click", onOk);
    dialogCancel.addEventListener("click", onCancel);
    dialogInput.addEventListener("keydown", onKey);
  });
}

async function initWasm(): Promise<void> {
  status.textContent = "loading wasm...";

  // Load Go WASM support
  const wasmExecResp = await fetch("./wasm_exec.js");
  const wasmExecText = await wasmExecResp.text();
  new Function(wasmExecText)();

  // Configure S3
  const s3 = createS3Adapter({
    endpoint: s3Endpoint,
    bucket,
    region: "us-east-1",
    credentials: {
      accessKeyId: params.get("ak") || "rustfsadmin",
      secretAccessKey: params.get("sk") || "rustfsadmin",
    },
    prefix: s3Prefix,
  });

  (globalThis as any).__loophole_s3 = s3;

  // Load and start WASM
  status.textContent = "starting wasm...";
  const wasmResp = await fetch("./loophole.wasm");
  const wasmBytes = await wasmResp.arrayBuffer();
  const go = new (globalThis as any).Go();
  const { instance } = await WebAssembly.instantiate(wasmBytes, go.importObject);
  go.run(instance);

  // Wait for Go runtime
  await new Promise((r) => setTimeout(r, 200));

  lh = (globalThis as any).loophole;
  if (!lh) throw new Error("WASM init failed — globalThis.loophole not set");
}

async function showVolumePicker(): Promise<string> {
  status.textContent = "loading volumes...";
  pickerError.textContent = "";
  actionsEl.style.display = "none";
  pickerEl.style.display = "flex";
  termEl.style.display = "none";

  // Clear old terminal content
  termEl.innerHTML = "";

  // Fetch existing volumes
  let volumes: string[] = [];
  try {
    volumes = Array.from(await lh.listVolumes());
  } catch (err: any) {
    console.warn("listVolumes failed:", err);
  }

  // Populate list
  volumeListEl.innerHTML = "";
  // Remove any leftover empty-msg
  const oldMsg = pickerEl.querySelector(".empty-msg");
  if (oldMsg) oldMsg.remove();

  if (volumes.length === 0) {
    volumeListEl.classList.add("empty");
    const msg = document.createElement("div");
    msg.className = "empty-msg";
    msg.textContent = "No volumes found.";
    volumeListEl.parentElement!.insertBefore(msg, volumeListEl.nextSibling);
  } else {
    volumeListEl.classList.remove("empty");
    for (const name of volumes) {
      const li = document.createElement("li");
      li.textContent = name;
      volumeListEl.appendChild(li);
    }
  }

  newVolumeInput.value = "";
  status.textContent = "select a volume";

  return new Promise<string>((resolve) => {
    function cleanup() {
      volumeListEl.removeEventListener("click", onListClick);
      createBtn.removeEventListener("click", doCreate);
      newVolumeInput.removeEventListener("keydown", onKey);
    }

    function onListClick(e: Event) {
      const li = (e.target as HTMLElement).closest("li");
      if (li) { cleanup(); resolve(li.textContent!); }
    }

    function doCreate() {
      const name = newVolumeInput.value.trim();
      if (!name) { pickerError.textContent = "Enter a volume name."; return; }
      if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
        pickerError.textContent = "Use only letters, numbers, hyphens, underscores.";
        return;
      }
      cleanup();
      resolve(name);
    }

    function onKey(e: KeyboardEvent) {
      if (e.key === "Enter") doCreate();
    }

    volumeListEl.addEventListener("click", onListClick);
    createBtn.addEventListener("click", doCreate);
    newVolumeInput.addEventListener("keydown", onKey);
  });
}

function setActionsEnabled(enabled: boolean) {
  snapshotBtn.disabled = !enabled;
  cloneBtn.disabled = !enabled;
  unmountBtn.disabled = !enabled;
}

async function startShell(volumeName: string) {
  // Hide picker, show terminal
  pickerEl.style.display = "none";
  termEl.style.display = "block";
  actionsEl.style.display = "flex";
  setActionsEnabled(false);

  // --- xterm setup ---
  const term = new Terminal({
    cursorBlink: true,
    fontSize: 14,
    fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
    theme: {
      background: "#1a1a2e",
      foreground: "#e0e0e0",
      cursor: "#e94560",
      selectionBackground: "#0f3460",
    },
  });
  const fitAddon = new FitAddon();
  term.loadAddon(fitAddon);
  term.open(termEl);
  fitAddon.fit();
  const onResize = () => fitAddon.fit();
  window.addEventListener("resize", onResize);

  term.writeln("");

  // Create and mount volume
  const mountpoint = "/mnt";
  status.textContent = "creating volume...";
  try {
    await lh.create(volumeName, 64 * 1024 * 1024); // 64MB
  } catch (err: any) {
    // Volume may already exist — try mounting directly
    term.writeln(`\x1b[90m(create: ${err.message})\x1b[0m`);
  }

  status.textContent = "mounting...";
  await lh.mount(volumeName, mountpoint);

  // Set up just-bash with loophole FS
  const fs = new LoopholeFs(lh, mountpoint);
  const bash = new Bash({
    fs,
    cwd: "/",
    env: {
      HOME: "/",
      USER: "loophole",
      SHELL: "/bin/bash",
      TERM: "xterm-256color",
      PS1: "\\[\\e[32;1m\\]loophole\\[\\e[0m\\]:\\[\\e[34;1m\\]\\w\\[\\e[0m\\]$ ",
    },
  });

  setActionsEnabled(true);
  status.textContent = volumeName;
  term.writeln(`\x1b[32mMounted volume \x1b[1m${volumeName}\x1b[0;32m (${s3Endpoint}/${bucket})\x1b[0m`);
  term.writeln(`\x1b[90mType commands. This is just-bash on an lwext4 filesystem.\x1b[0m`);
  term.writeln("");

  // --- Toolbar actions ---
  let unmounted = false;

  const doUnmount = async () => {
    if (unmounted) return;
    unmounted = true;
    status.textContent = "unmounting...";
    try { await lh.unmount(mountpoint); } catch {}
    term.dispose();
    window.removeEventListener("resize", onResize);
    // Go back to volume picker
    loop();
  };

  const doSnapshot = async () => {
    const name = await showDialog(`Snapshot "${volumeName}"`, "snapshot name");
    if (!name) return;
    term.writeln(`\x1b[90mCreating snapshot "${name}"...\x1b[0m`);
    try {
      await lh.snapshot(mountpoint, name);
      term.writeln(`\x1b[32mSnapshot "${name}" created.\x1b[0m`);
    } catch (err: any) {
      term.writeln(`\x1b[31mSnapshot failed: ${err.message}\x1b[0m`);
    }
    term.write(getPrompt());
  };

  const doClone = async () => {
    const name = await showDialog(`Clone "${volumeName}"`, "clone name");
    if (!name) return;
    term.writeln(`\x1b[90mCloning to "${name}"...\x1b[0m`);
    try {
      await lh.clone(mountpoint, name, name);
      term.writeln(`\x1b[32mClone "${name}" created.\x1b[0m`);
    } catch (err: any) {
      term.writeln(`\x1b[31mClone failed: ${err.message}\x1b[0m`);
    }
    term.write(getPrompt());
  };

  unmountBtn.addEventListener("click", doUnmount);
  snapshotBtn.addEventListener("click", doSnapshot);
  cloneBtn.addEventListener("click", doClone);

  // Best-effort unmount on page close
  const cleanup = () => { if (!unmounted) { unmounted = true; try { lh.unmount(mountpoint); } catch {} } };
  window.addEventListener("pagehide", cleanup);
  window.addEventListener("beforeunload", cleanup);

  // --- Interactive shell loop ---
  let lineBuffer = "";
  const history: string[] = [];
  let historyIndex = -1;

  function getPrompt(): string {
    const cwd = bash.getCwd();
    return `\x1b[32;1mloophole\x1b[0m:\x1b[34;1m${cwd}\x1b[0m$ `;
  }

  function writePrompt() {
    term.write(getPrompt());
  }

  writePrompt();

  term.onKey(async ({ key, domEvent }) => {
    if (unmounted) return;
    const code = domEvent.keyCode;

    if (code === 13) {
      term.writeln("");
      const cmd = lineBuffer.trim();
      lineBuffer = "";
      historyIndex = -1;

      if (cmd) {
        history.push(cmd);
        try {
          const result = await bash.exec(cmd);
          if (result.stdout) {
            term.write(result.stdout.replace(/\n/g, "\r\n"));
          }
          if (result.stderr) {
            term.write(`\x1b[31m${result.stderr.replace(/\n/g, "\r\n")}\x1b[0m`);
          }
        } catch (err: any) {
          term.writeln(`\x1b[31mError: ${err.message}\x1b[0m`);
        }
      }
      writePrompt();
    } else if (code === 8) {
      if (lineBuffer.length > 0) {
        lineBuffer = lineBuffer.slice(0, -1);
        term.write("\b \b");
      }
    } else if (code === 38) {
      if (history.length > 0) {
        if (historyIndex === -1) historyIndex = history.length;
        if (historyIndex > 0) {
          historyIndex--;
          term.write("\r" + getPrompt() + " ".repeat(lineBuffer.length) + "\r" + getPrompt());
          lineBuffer = history[historyIndex];
          term.write(lineBuffer);
        }
      }
    } else if (code === 40) {
      if (historyIndex !== -1) {
        historyIndex++;
        term.write("\r" + getPrompt() + " ".repeat(lineBuffer.length) + "\r" + getPrompt());
        if (historyIndex >= history.length) {
          historyIndex = -1;
          lineBuffer = "";
        } else {
          lineBuffer = history[historyIndex];
          term.write(lineBuffer);
        }
      }
    } else if (code === 9) {
      // Tab — ignore
    } else if (domEvent.ctrlKey && code === 67) {
      term.writeln("^C");
      lineBuffer = "";
      writePrompt();
    } else if (domEvent.ctrlKey && code === 76) {
      term.clear();
      writePrompt();
    } else if (!domEvent.ctrlKey && !domEvent.altKey && !domEvent.metaKey && key.length === 1) {
      lineBuffer += key;
      term.write(key);
    }
  });

  // Return a cleanup function for when we unmount via toolbar
  return () => {
    unmountBtn.removeEventListener("click", doUnmount);
    snapshotBtn.removeEventListener("click", doSnapshot);
    cloneBtn.removeEventListener("click", doClone);
    window.removeEventListener("pagehide", cleanup);
    window.removeEventListener("beforeunload", cleanup);
  };
}

let cleanupShell: (() => void) | null = null;

async function loop() {
  if (cleanupShell) { cleanupShell(); cleanupShell = null; }
  const volumeName = await showVolumePicker();
  cleanupShell = await startShell(volumeName);
}

async function init() {
  await initWasm();
  await loop();
}

init().catch((err) => {
  status.textContent = "error";
  pickerError.textContent = err.message;
  console.error(err);
});

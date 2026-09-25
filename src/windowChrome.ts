import { isTauri } from "@tauri-apps/api/core";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { icons, renderIcons } from "./icons";

// Adapted from Loci Lite's useWindowChrome hook and Tauri window wrapper.
let isMaximized = false;
let resizeListenerStarted = false;

function appWindow() {
  return getCurrentWindow();
}

function syncMaximizeControl(): void {
  document.documentElement.classList.toggle("window-maximized", isMaximized);
  const button = document.querySelector<HTMLButtonElement>("#window-maximize");
  if (!button) return;
  button.setAttribute("aria-label", isMaximized ? "Restore" : "Maximize");
  button.setAttribute("title", isMaximized ? "Restore" : "Maximize");
  button.innerHTML = isMaximized ? icons.restore : icons.maximize;
  renderIcons(button);
}

async function refreshMaximized(): Promise<void> {
  isMaximized = await appWindow().isMaximized();
  syncMaximizeControl();
}

async function toggleMaximize(): Promise<void> {
  await appWindow().toggleMaximize();
  await refreshMaximized();
}

export function bindWindowChrome(): void {
  if (!isTauri()) return;

  syncMaximizeControl();
  if (!resizeListenerStarted) {
    resizeListenerStarted = true;
    void appWindow().onResized(() => { void refreshMaximized(); });
    void refreshMaximized();
  }

  document.querySelector("#window-minimize")?.addEventListener("click", () => { void appWindow().minimize(); });
  document.querySelector("#window-maximize")?.addEventListener("click", () => { void toggleMaximize(); });
  document.querySelector("#window-close")?.addEventListener("click", () => { void appWindow().close(); });
  document.querySelector(".window-chrome-drag")?.addEventListener("mousedown", (event) => {
    const mouse = event as MouseEvent;
    if (mouse.buttons !== 1) return;
    if (mouse.detail === 2) {
      void toggleMaximize();
      return;
    }
    void appWindow().startDragging();
  });
}

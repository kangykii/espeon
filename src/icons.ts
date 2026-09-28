import {
  Activity, Archive, ArchiveRestore, ArrowRight, ChartNoAxesColumn, Check, ChevronDown,
  ChevronRight, Copy, ExternalLink, FileText, GitBranch, History, Layers3,
  Maximize, Minus, Moon, PanelLeft, Pencil, Plus, Search, Settings, Square,
  Sun, X, createIcons,
} from "lucide";

const lucideIcons = {
  Activity, Archive, ArchiveRestore, ArrowRight, ChartNoAxesColumn, Check, ChevronDown,
  ChevronRight, Copy, ExternalLink, FileText, GitBranch, History, Layers3,
  Maximize, Minus, Moon, PanelLeft, Pencil, Plus, Search, Settings, Square, Sun, X,
};

const icon = (name: string) => `<i data-lucide="${name}" aria-hidden="true"></i>`;

export const icons = {
  panel: icon("panel-left"),
  inspect: icon("layers-3"),
  search: icon("search"),
  plus: icon("plus"),
  stop: icon("square"),
  arrow: icon("arrow-right"),
  chevron: icon("chevron-right"),
  chevronDown: icon("chevron-down"),
  activity: icon("activity"),
  position: icon("chart-no-axes-column"),
  evidence: icon("file-text"),
  history: icon("history"),
  sun: icon("sun"),
  moon: icon("moon"),
  branch: icon("git-branch"),
  external: icon("external-link"),
  settings: icon("settings"),
  minimize: icon("minus"),
  maximize: icon("maximize"),
  restore: icon("copy"),
  close: icon("x"),
  rename: icon("pencil"),
  check: icon("check"),
  archive: icon("archive"),
  restoreArchive: icon("archive-restore"),
};

export function renderIcons(root: HTMLElement): void {
  createIcons({ icons: lucideIcons, root });
}

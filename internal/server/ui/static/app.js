// GlowKron Single-Page Application (Vanilla ES6+, Zero-CDN, Air-Gapped)
document.addEventListener("DOMContentLoaded", () => {
  "use strict";

  // ==========================================
  // APPLICATION STATE
  // ==========================================
  const state = {
    activeTab: "workflows",
    workflows: [],
    runs: [],
    runStepCache: {}, // runId -> steps array
    selectedWorkflow: null, // Currently selected workflow object
    inspectingRunId: null, // If set, DAG reflects this specific run's execution
    inspectingRun: null, // Full run object for inspected run
    activeStreamRunId: null, // Run ID currently hooked to SSE
    selectedStepId: null, // Step ID currently opened in Inspector Drawer

    // SSE Streamer & Terminal State
    eventSource: null,
    reconnectAttempts: 0,
    maxReconnectAttempts: 5,
    reconnectTimer: null,
    stepPollTimer: null,
    autoScroll: true,
    userScrolledUp: false,
    terminalStreamTab: "all", // "all" | "stdout" | "stderr" | "telemetry"
    terminalSearchQuery: "",
    terminalShowUTC: false, // false = relative, true = UTC
    terminalWordWrap: false,
    terminalExpanded: false,
    terminalFullscreen: false,
    rawLogLines: [], // Array of log line objects { text, stream, timestamp, relSec }
    maxLogLines: 5000,
    streamBytesReceived: 0,
    streamStartTime: null,
    pendingLogBatch: [],
    rafScheduled: false,

    // DAG Canvas Pan/Zoom State
    zoomScale: 1.0,
    panX: 0,
    panY: 0,
    isPanning: false,
    panStartX: 0,
    panStartY: 0,
    basePanX: 0,
    basePanY: 0,

    // Runs View Filters
    statusFilter: "all",
    searchQuery: "",

    // Command Palette
    cmdPaletteOpen: false,
    cmdSelectedIndex: 0,
    cmdFilteredItems: [],

    // Live Execution Elapsed Timer
    runTimerInterval: null,
    runElapsedSeconds: 0,
  };

  // ==========================================
  // DOM ELEMENTS
  // ==========================================
  const elements = {
    // Shell & Navigation
    sidebar: document.getElementById("sidebar"),
    sidebarToggleBtn: document.getElementById("sidebar-toggle-btn"),
    sidebarCollapseBtn: document.getElementById("sidebar-collapse-btn"),
    brandHomeLink: document.getElementById("brand-home-link"),
    navItems: document.querySelectorAll(".nav-item"),
    viewSections: document.querySelectorAll(".view-section"),
    pageContextTitle: document.getElementById("page-context-title"),
    globalRefreshBtn: document.getElementById("btn-global-refresh"),
    daemonStatusBadge: document.getElementById("daemon-status-badge"),
    daemonBadgeLabel: document.getElementById("daemon-badge-label"),
    daemonMetaLabel: document.getElementById("daemon-meta-label"),
    headerSearchBtn: document.getElementById("header-search-btn"),
    headerActiveRun: document.getElementById("header-active-run"),
    headerActiveRunText: document.getElementById("header-active-run-text"),
    headerActiveRunBtn: document.getElementById("header-active-run-btn"),
    liveUtcClock: document.getElementById("live-utc-clock"),
    toastContainer: document.getElementById("toast-container"),

    // Sidebar Node Monitors
    badgeWorkflowsCount: document.getElementById("badge-workflows-count"),
    badgeRunningCount: document.getElementById("badge-running-count"),
    terminalLiveIndicator: document.getElementById("terminal-live-indicator"),
    resourceCpuVal: document.getElementById("resource-cpu-val"),
    resourceCpuBar: document.getElementById("resource-cpu-bar"),
    resourceMemVal: document.getElementById("resource-mem-val"),
    resourceMemBar: document.getElementById("resource-mem-bar"),
    sidebarStatusDot: document.getElementById("sidebar-status-dot"),
    sidebarDaemonText: document.getElementById("sidebar-daemon-text"),
    sidebarWorkflowsDir: document.getElementById("sidebar-workflows-dir"),
    sidebarServerTime: document.getElementById("sidebar-server-time"),

    // Workflows View
    workflowCountBadge: document.getElementById("workflow-count-badge"),
    workflowTableBody: document.getElementById("workflow-table-body"),

    // Context Topbar & DAG Control Bar
    dagControlBar: document.getElementById("dag-control-bar"),
    dagTitle: document.getElementById("dag-title"),
    dagSubtitle: document.getElementById("dag-subtitle"),
    dagRunBadge: document.getElementById("dag-run-badge"),
    dagRunIdTag: document.getElementById("dag-run-id-tag"),
    dagElapsedTime: document.getElementById("dag-elapsed-time"),
    dagStepsProgress: document.getElementById("dag-steps-progress"),
    dagProgressFill: document.getElementById("dag-progress-fill"),
    dagProgressPct: document.getElementById("dag-progress-pct"),
    dagActionRerun: document.getElementById("dag-action-rerun"),
    dagResetTemplateBtn: document.getElementById("dag-reset-template-btn"),
    dagRefreshBtn: document.getElementById("dag-refresh-btn"),
    dagActionDownloadLogs: document.getElementById("dag-action-download-logs"),

    // Interactive DAG Canvas
    dagCanvasContainer: document.getElementById("dag-canvas-container"),
    dagStageIndicators: document.getElementById("dag-stage-indicators"),
    dagViewport: document.getElementById("dag-viewport"),
    dagSvg: document.getElementById("dag-svg"),
    dagSvgEdgesGroup: document.getElementById("dag-svg-edges-group"),
    dagContainer: document.getElementById("dag-container"),

    // Canvas Floating Controls
    canvasZoomIn: document.getElementById("canvas-zoom-in"),
    canvasZoomOut: document.getElementById("canvas-zoom-out"),
    canvasFitViewport: document.getElementById("canvas-fit-viewport"),
    canvasZoomReset: document.getElementById("canvas-zoom-reset"),

    // Radar HUD
    radarHud: document.getElementById("radar-hud"),
    radarHudBody: document.getElementById("radar-hud-body"),
    radarViewportBox: document.getElementById("radar-viewport-box"),

    // Slide-Over Step Inspector Drawer
    nodeInspector: document.getElementById("node-inspector"),
    inspectorStepTitle: document.getElementById("inspector-step-title"),
    inspectorStepBadge: document.getElementById("inspector-step-badge"),
    inspectorCloseBtn: document.getElementById("inspector-close-btn"),
    inspectorCmdBox: document.getElementById("inspector-cmd-box"),
    inspectorCopyCmd: document.getElementById("inspector-copy-cmd"),
    inspectorWorkdir: document.getElementById("inspector-workdir"),
    inspectorEnvTable: document.getElementById("inspector-env-table"),
    inspectorDepsList: document.getElementById("inspector-deps-list"),
    inspectorThroughput: document.getElementById("inspector-throughput"),
    inspectorStdoutSection: document.getElementById("inspector-stdout-section"),
    inspectorStdoutBox: document.getElementById("inspector-stdout-box"),
    inspectorCopyStdout: document.getElementById("inspector-copy-stdout"),
    inspectorStderrSection: document.getElementById("inspector-stderr-section"),
    inspectorStderrBox: document.getElementById("inspector-stderr-box"),
    inspectorCopyStderr: document.getElementById("inspector-copy-stderr"),

    // Docked Split Live SSE Terminal
    terminalPanel: document.getElementById("terminal-panel"),
    tabStreamAll: document.getElementById("tab-stream-all"),
    tabStreamStdout: document.getElementById("tab-stream-stdout"),
    tabStreamStderr: document.getElementById("tab-stream-stderr"),
    tabStreamTelemetry: document.getElementById("tab-stream-telemetry"),
    tabCountAll: document.getElementById("tab-count-all"),
    tabCountStdout: document.getElementById("tab-count-stdout"),
    tabCountStderr: document.getElementById("tab-count-stderr"),
    terminalSearchInput: document.getElementById("terminal-search-input"),
    terminalMatchCount: document.getElementById("terminal-match-count"),
    terminalSseRate: document.getElementById("terminal-sse-rate"),
    terminalSseBytes: document.getElementById("terminal-sse-bytes"),
    terminalToggleTimestamps: document.getElementById("terminal-toggle-timestamps"),
    terminalToggleWrap: document.getElementById("terminal-toggle-wrap"),
    inspectDagBtn: document.getElementById("inspect-dag-btn"),
    copyTerminalBtn: document.getElementById("copy-terminal-btn"),
    clearTerminalBtn: document.getElementById("clear-terminal-btn"),
    autoScrollToggle: document.getElementById("auto-scroll-toggle"),
    autoScrollLabel: document.getElementById("auto-scroll-label"),
    terminalToggleExpand: document.getElementById("terminal-toggle-expand"),
    expandIcon: document.getElementById("expand-icon"),
    terminalToggleFullscreen: document.getElementById("terminal-toggle-fullscreen"),
    terminalLogs: document.getElementById("terminal-logs"),
    terminalRunId: document.getElementById("terminal-run-id"),
    terminalConnStatus: document.getElementById("terminal-conn-status"),
    terminalMetaLines: document.getElementById("terminal-meta-lines"),
    terminalMetaBytes: document.getElementById("terminal-meta-bytes"),
    terminalMetaStreamStatus: document.getElementById("terminal-meta-stream-status"),
    terminalScrollHint: document.getElementById("terminal-scroll-hint"),

    // Runs View
    statTotalRuns: document.getElementById("stat-total-runs"),
    statRunningRuns: document.getElementById("stat-running-runs"),
    statCompletedRuns: document.getElementById("stat-completed-runs"),
    statFailedRuns: document.getElementById("stat-failed-runs"),
    filterPills: document.querySelectorAll(".filter-pill"),
    runsSearchInput: document.getElementById("runs-search-input"),
    runsSearchClear: document.getElementById("runs-search-clear"),
    runsTableBody: document.getElementById("runs-table-body"),

    // Settings View
    settingsDaemonStatus: document.getElementById("settings-daemon-status"),
    settingsVersion: document.getElementById("settings-version"),
    settingsWorkflowsDir: document.getElementById("settings-workflows-dir"),
    settingsServerTime: document.getElementById("settings-server-time"),

    // Command Palette Modal
    commandPaletteModal: document.getElementById("command-palette-modal"),
    cmdPaletteInput: document.getElementById("cmd-palette-input"),
    cmdPaletteResults: document.getElementById("cmd-palette-results"),
  };

  // ==========================================
  // NAVIGATION & TAB SWITCHING
  // ==========================================
  function switchTab(tabId) {
    state.activeTab = tabId;

    elements.navItems.forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === tabId);
    });

    elements.viewSections.forEach((section) => {
      section.classList.toggle("active", section.id === `view-${tabId}`);
    });

    const titles = {
      workflows: "Workflows & DAGs",
      runs: "Executions & History",
      terminal: "Live Terminal Console",
      dag: "DAG Topology Visualizer",
      settings: "Daemon Settings & Telemetry",
    };
    if (elements.pageContextTitle) {
      elements.pageContextTitle.textContent = titles[tabId] || "Dashboard";
    }

    // Close mobile drawer if opened
    if (elements.sidebar.classList.contains("open")) {
      elements.sidebar.classList.remove("open");
    }

    if (tabId === "runs") {
      loadRuns();
    } else if (tabId === "workflows" || tabId === "dag") {
      loadWorkflows();
      if (tabId === "dag" && elements.dagControlBar) {
        elements.dagControlBar.scrollIntoView({ behavior: "smooth" });
      }
    } else if (tabId === "terminal") {
      expandTerminal(true);
    }
  }

  elements.navItems.forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  if (elements.brandHomeLink) {
    elements.brandHomeLink.addEventListener("click", () => switchTab("workflows"));
  }

  // Sidebar Rail Collapse Toggle (240px <-> 64px)
  if (elements.sidebarCollapseBtn) {
    elements.sidebarCollapseBtn.addEventListener("click", () => {
      elements.sidebar.classList.toggle("collapsed");
      setTimeout(() => fitDAGToViewport(), 300);
    });
  }

  // Mobile Sidebar Drawer Toggle
  if (elements.sidebarToggleBtn) {
    elements.sidebarToggleBtn.addEventListener("click", () => {
      elements.sidebar.classList.toggle("open");
    });
  }

  // Global Refresh Action
  if (elements.globalRefreshBtn) {
    elements.globalRefreshBtn.addEventListener("click", async () => {
      elements.globalRefreshBtn.disabled = true;
      showToast({ title: "Syncing", message: "Refreshing daemon telemetry & pipelines...", type: "info", duration: 1500 });
      await Promise.all([checkHealth(), loadWorkflows(), loadRuns()]);
      if (state.inspectingRunId) {
        await refreshInspectedRunSteps(state.inspectingRunId);
      }
      setTimeout(() => {
        elements.globalRefreshBtn.disabled = false;
      }, 500);
    });
  }

  // Header active run button
  if (elements.headerActiveRunBtn) {
    elements.headerActiveRunBtn.addEventListener("click", () => {
      if (state.activeStreamRunId) {
        switchTab("workflows");
        if (elements.terminalPanel) {
          elements.terminalPanel.scrollIntoView({ behavior: "smooth" });
        }
      }
    });
  }

  // ==========================================
  // TOAST NOTIFICATIONS
  // ==========================================
  function showToast({ title, message, type = "info", duration = 4000, actions = [] }) {
    if (!elements.toastContainer) return;

    const toast = document.createElement("div");
    toast.className = `toast toast-${type}`;

    let actionsHtml = "";
    if (actions && actions.length > 0) {
      actionsHtml = `
        <div style="display:flex;gap:0.4rem;margin-top:0.4rem;">
          ${actions.map((act, i) => `<button class="btn btn-secondary btn-sm toast-act-btn" data-act-idx="${i}">${escapeHTML(act.label)}</button>`).join("")}
        </div>
      `;
    }

    toast.innerHTML = `
      <div class="toast-header">
        <span class="toast-title">
          <span class="status-indicator-dot" style="display:inline-block;width:6px;height:6px;border-radius:50%;background:currentColor;"></span>
          ${escapeHTML(title)}
        </span>
        <button class="toast-close" aria-label="Close notification">&times;</button>
      </div>
      <div class="toast-msg">${escapeHTML(message)}</div>
      ${actionsHtml}
    `;

    toast.querySelectorAll(".toast-act-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.actIdx, 10);
        if (actions[idx] && typeof actions[idx].onClick === "function") {
          actions[idx].onClick();
        }
        removeToast(toast);
      });
    });

    toast.querySelector(".toast-close").addEventListener("click", () => removeToast(toast));

    elements.toastContainer.appendChild(toast);

    setTimeout(() => {
      removeToast(toast);
    }, duration);
  }

  function removeToast(toast) {
    if (!toast || !toast.parentNode) return;
    toast.style.opacity = "0";
    toast.style.transform = "translateX(100%)";
    toast.style.transition = "all 0.2s ease";
    setTimeout(() => {
      if (toast.parentNode) toast.parentNode.removeChild(toast);
    }, 200);
  }

  // ==========================================
  // DAEMON HEALTH CHECK & SYSTEM TELEMETRY
  // ==========================================
  async function checkHealth() {
    try {
      const res = await fetch("/api/health");
      if (res.ok) {
        const data = await res.json();
        elements.daemonStatusBadge.className = "daemon-badge";
        elements.daemonBadgeLabel.textContent = "DAEMON: HEALTHY";
        elements.daemonMetaLabel.textContent = `(${data.version || "v0.1.0"}, :8080)`;
        elements.sidebarDaemonText.textContent = "Online";
        elements.sidebarWorkflowsDir.textContent = data.workflows_dir || "~";

        if (elements.settingsDaemonStatus) elements.settingsDaemonStatus.textContent = "HEALTHY (Zero-CDN)";
        if (elements.settingsVersion) elements.settingsVersion.textContent = data.version || "v0.1.0";
        if (elements.settingsWorkflowsDir) elements.settingsWorkflowsDir.textContent = data.workflows_dir || "~";
      } else {
        markDaemonOffline();
      }
    } catch {
      markDaemonOffline();
    }
  }

  function markDaemonOffline() {
    elements.daemonStatusBadge.className = "daemon-badge badge-offline";
    elements.daemonBadgeLabel.textContent = "DAEMON: OFFLINE";
    elements.daemonMetaLabel.textContent = "(Disconnected)";
    elements.sidebarDaemonText.textContent = "Offline";
    if (elements.settingsDaemonStatus) elements.settingsDaemonStatus.textContent = "OFFLINE (Unreachable)";
  }

  // Live Local Clock & Resource Fluctuation Ticker
  setInterval(() => {
    const now = new Date();
    const timeStr = now.toLocaleTimeString();

    if (elements.liveUtcClock) elements.liveUtcClock.textContent = timeStr;
    if (elements.sidebarServerTime) elements.sidebarServerTime.textContent = timeStr;
    if (elements.settingsServerTime) elements.settingsServerTime.textContent = timeStr;

    // Simulate high-density real-time telemetry fluctuations
    const cpuPct = (1.0 + Math.sin(now.getTime() / 4000) * 0.4).toFixed(1);
    if (elements.resourceCpuVal) elements.resourceCpuVal.textContent = `${cpuPct}%`;
    if (elements.resourceCpuBar) elements.resourceCpuBar.style.width = `${Math.min(100, Math.round(parseFloat(cpuPct) * 10))}%`;
  }, 1000);

  // ==========================================
  // WORKFLOWS & REGISTRY
  // ==========================================
  async function loadWorkflows() {
    try {
      const res = await fetch("/api/jobs");
      if (res.ok) {
        state.workflows = await res.json();
        elements.badgeWorkflowsCount.textContent = state.workflows.length;
        elements.workflowCountBadge.textContent = `${state.workflows.length} workflow${state.workflows.length === 1 ? "" : "s"}`;
        renderWorkflowsTable();

        // Auto-select first workflow if none currently selected
        if (!state.selectedWorkflow && state.workflows.length > 0) {
          selectWorkflow(state.workflows[0].id || state.workflows[0].name);
        }
      }
    } catch (err) {
      console.error("Failed to load workflows:", err);
    }
  }

  function renderWorkflowsTable() {
    if (!state.workflows || state.workflows.length === 0) {
      elements.workflowTableBody.innerHTML = `
        <tr>
          <td colspan="6" class="table-loading">No workflows registered in daemon directory.</td>
        </tr>
      `;
      return;
    }

    elements.workflowTableBody.innerHTML = state.workflows
      .map((wf) => {
        const isSelected = state.selectedWorkflow && state.selectedWorkflow.id === wf.id;
        const rowClass = isSelected ? "row-active" : "";
        const cron = wf.cron_schedule || "Manual / Ad-hoc";
        const timeout = wf.timeout_sec ? `${wf.timeout_sec}s` : "None";
        const status = wf.enabled ? "ACTIVE" : "DISABLED";
        const badgeClass = wf.enabled ? "badge-completed" : "badge-muted";

        return `
          <tr class="${rowClass}" data-wf-id="${escapeHTML(wf.id)}">
            <td>
              <div style="display:flex;align-items:center;gap:0.45rem;">
                <span class="status-indicator-dot" style="display:inline-block;width:6px;height:6px;border-radius:50%;background:${wf.enabled ? "var(--status-completed)" : "var(--text-muted)"};"></span>
                <strong style="color:var(--text-primary);cursor:pointer;" class="wf-link" data-id="${escapeHTML(wf.id)}">${escapeHTML(wf.name || wf.id)}</strong>
              </div>
            </td>
            <td><span class="badge ${badgeClass}">${status}</span></td>
            <td><code style="font-family:var(--font-mono);font-size:0.75rem;color:var(--accent-sky);">${escapeHTML(cron)}</code></td>
            <td style="font-family:var(--font-mono);font-size:0.75rem;">${timeout}</td>
            <td style="color:var(--text-secondary);max-width:280px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${escapeHTML(wf.description || "DAG Workflow")}</td>
            <td class="text-right">
              <button class="btn btn-secondary btn-sm btn-view-dag" data-id="${escapeHTML(wf.id)}" title="Inspect DAG Topology">
                Inspect
              </button>
              <button class="btn btn-primary btn-sm btn-trigger-wf" data-id="${escapeHTML(wf.id)}" title="Run Workflow">
                Run
              </button>
            </td>
          </tr>
        `;
      })
      .join("");

    // Bind row and button events
    elements.workflowTableBody.querySelectorAll(".wf-link, .btn-view-dag").forEach((el) => {
      el.addEventListener("click", (e) => {
        e.stopPropagation();
        selectWorkflow(el.dataset.id);
      });
    });

    elements.workflowTableBody.querySelectorAll(".btn-trigger-wf").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        triggerWorkflow(btn.dataset.id);
      });
    });

    elements.workflowTableBody.querySelectorAll("tr").forEach((row) => {
      row.addEventListener("click", () => {
        const id = row.dataset.wfId;
        if (id) selectWorkflow(id);
      });
    });
  }

  // Trigger ad-hoc execution of a workflow
  async function triggerWorkflow(workflowId) {
    try {
      showToast({ title: "Dispatching", message: `Triggering execution for ${workflowId}...`, type: "info" });
      const res = await fetch(`/api/jobs/${encodeURIComponent(workflowId)}/run?async=true`, {
        method: "POST",
      });

      if (res.ok) {
        const data = await res.json();
        const runId = data.id || data.run_id;

        showToast({
          title: "Run Dispatched",
          message: `Run #${runId.substring(0, 8)} started successfully.`,
          type: "success",
          actions: [
            {
              label: "Tail Logs",
              onClick: () => {
                streamLogs(runId);
              },
            },
          ],
        });

        // Immediately inspect this run in the DAG and hook log stream
        await inspectRunDAG(runId);
        streamLogs(runId);
        loadRuns();
      } else {
        const errText = await res.text();
        showToast({ title: "Run Failed", message: `HTTP ${res.status}: ${errText}`, type: "error" });
      }
    } catch (err) {
      showToast({ title: "Network Error", message: `Trigger failed: ${err.message}`, type: "error" });
    }
  }

  // Select baseline workflow template
  async function selectWorkflow(id) {
    try {
      const res = await fetch(`/api/jobs/${encodeURIComponent(id)}`);
      if (res.ok) {
        state.selectedWorkflow = await res.json();
        state.inspectingRunId = null;
        state.inspectingRun = null;
        state.selectedStepId = null;
        renderWorkflowsTable();
        renderDAG();
      }
    } catch (err) {
      console.error("Failed to fetch workflow detail:", err);
    }
  }

  // Inspect specific run in DAG viewer
  async function inspectRunDAG(runId) {
    state.inspectingRunId = runId;

    try {
      const runRes = await fetch(`/api/runs/${encodeURIComponent(runId)}`);
      if (runRes.ok) {
        state.inspectingRun = await runRes.json();
      }

      if (
        state.inspectingRun &&
        (!state.selectedWorkflow || state.selectedWorkflow.id !== state.inspectingRun.job_id)
      ) {
        const wfRes = await fetch(`/api/jobs/${encodeURIComponent(state.inspectingRun.job_id)}`);
        if (wfRes.ok) {
          state.selectedWorkflow = await wfRes.json();
        }
      }

      await refreshInspectedRunSteps(runId);
      renderDAG();
    } catch (err) {
      console.error("Failed to inspect run DAG:", err);
    }
  }

  async function refreshInspectedRunSteps(runId) {
    try {
      const res = await fetch(`/api/runs/${encodeURIComponent(runId)}/steps`);
      if (res.ok) {
        const steps = await res.json();
        state.runStepCache[runId] = steps;
        if (state.inspectingRunId === runId) {
          updateDAGInPlace(steps);
        }
      }
    } catch (err) {
      console.error("Failed to refresh step runs:", err);
    }
  }

  // ==========================================
  // TOPOLOGICAL DAG ENGINE & SVG VECTOR CANVAS
  // ==========================================

  // Kahn's algorithm: topological partition into parallel dependency stages
  function computeTopologicalStages(steps) {
    if (!steps || steps.length === 0) return [];

    const inDegree = {};
    const dependents = {};

    steps.forEach((s) => {
      inDegree[s.id] = (s.depends_on || []).length;
      dependents[s.id] = [];
    });

    steps.forEach((s) => {
      (s.depends_on || []).forEach((depId) => {
        if (dependents[depId]) {
          dependents[depId].push(s.id);
        }
      });
    });

    const stages = [];
    const visited = new Set();
    let currentWave = steps.filter((s) => inDegree[s.id] === 0);

    if (currentWave.length === 0) {
      return [steps];
    }

    while (currentWave.length > 0) {
      stages.push(currentWave);
      currentWave.forEach((s) => visited.add(s.id));

      const nextWave = [];
      steps.forEach((s) => {
        if (!visited.has(s.id)) {
          const remainingDeps = (s.depends_on || []).filter((dep) => !visited.has(dep));
          if (remainingDeps.length === 0) {
            nextWave.push(s);
          }
        }
      });

      if (nextWave.length === 0 && visited.size < steps.length) {
        const remaining = steps.filter((s) => !visited.has(s.id));
        stages.push(remaining);
        break;
      }

      currentWave = nextWave;
    }

    return stages;
  }

  // Render Full Interactive SVG DAG Canvas
  function renderDAG() {
    if (!state.selectedWorkflow) {
      elements.dagTitle.textContent = "Select a Workflow";
      elements.dagRunBadge.style.display = "none";
      elements.dagRunIdTag.style.display = "none";
      elements.dagContainer.innerHTML = `
        <div class="dag-empty-state" style="margin: 4rem auto; text-align: center; color: var(--text-muted);">
          <p>Select a workflow above or inspect a run to view its execution graph.</p>
        </div>
      `;
      elements.dagSvgEdgesGroup.innerHTML = "";
      return;
    }

    const steps = state.selectedWorkflow.steps || [];
    const isRun = Boolean(state.inspectingRunId);

    // Update Control Bar Header
    elements.dagTitle.textContent = state.selectedWorkflow.name || state.selectedWorkflow.id;
    if (isRun) {
      const runStatus = state.inspectingRun ? (state.inspectingRun.status || "RUNNING") : "RUNNING";
      elements.dagRunBadge.style.display = "inline-flex";
      elements.dagRunBadge.className = `badge badge-${runStatus.toLowerCase()}`;
      elements.dagRunBadge.textContent = runStatus;

      elements.dagRunIdTag.style.display = "inline-flex";
      elements.dagRunIdTag.textContent = `#run-${state.inspectingRunId.substring(0, 8)}`;
      elements.dagResetTemplateBtn.style.display = "inline-flex";
      startRunElapsedTimer();
    } else {
      elements.dagRunBadge.style.display = "none";
      elements.dagRunIdTag.style.display = "none";
      elements.dagResetTemplateBtn.style.display = "none";
      stopRunElapsedTimer();
      elements.dagElapsedTime.textContent = "Template";
      elements.dagStepsProgress.textContent = `${steps.length} steps`;
      elements.dagProgressFill.style.width = "100%";
      elements.dagProgressPct.textContent = "100%";
    }

    // Step status mapping
    const stepRunMap = {};
    if (isRun && state.runStepCache[state.inspectingRunId]) {
      state.runStepCache[state.inspectingRunId].forEach((sr) => {
        stepRunMap[sr.step_id] = sr;
      });
    }

    // Compute topological dependency stages
    const stages = computeTopologicalStages(steps);

    // Render Stage Track Overline Indicators
    const stageTitles = [
      "Ingestion & Source",
      "Parallel Fan-Out Processing",
      "Transformation & Validation",
      "Sink, Delivery & Ingress",
      "Finalize & Webhook Notify",
    ];

    elements.dagStageIndicators.innerHTML = stages
      .map((stage, idx) => {
        const title = stageTitles[idx] || `Stage Processing 0${idx}`;
        return `<span class="stage-overline-pill">Stage 0${idx} // ${title}</span>`;
      })
      .join("");

    // Render DOM Stage Columns and Node Cards
    elements.dagContainer.innerHTML = stages
      .map((stage, stageIndex) => {
        return `
          <div class="dag-stage-col" data-stage="${stageIndex}">
            ${stage
              .map((step) => {
                const stepRun = stepRunMap[step.id];
                const status = stepRun ? (stepRun.status || "PENDING") : "PENDING";
                const statusClass = `status-${status.toLowerCase()}`;
                const isRunning = status === "RUNNING";

                let durationStr = "waiting";
                let exitCodeStr = "";
                if (stepRun) {
                  if (stepRun.end_time && stepRun.start_time) {
                    const dur = ((new Date(stepRun.end_time) - new Date(stepRun.start_time)) / 1000).toFixed(2);
                    durationStr = `${dur}s`;
                  } else if (isRunning) {
                    durationStr = "running...";
                  }
                  if (stepRun.exit_code !== undefined && stepRun.exit_code !== null) {
                    exitCodeStr = `exit: ${stepRun.exit_code}`;
                  }
                }

                const argsStr = (step.args || []).join(" ");
                const cmdDisplay = `${step.command || ""} ${argsStr}`.trim();
                const deps = step.depends_on || [];

                return `
                  <div class="dag-node ${statusClass}" data-step-id="${escapeHTML(step.id)}" id="node-${escapeHTML(step.id)}">
                    ${isRunning ? `<span class="dag-node-ping beacon-pulse"><span class="beacon-pulse-ring"></span><span class="beacon-pulse-dot"></span></span>` : ""}
                    <div class="dag-node-header">
                      <span class="badge badge-${status.toLowerCase()}">${status}</span>
                      <span class="dag-node-timer">${durationStr}</span>
                    </div>

                    <div class="dag-node-id">
                      <svg class="icon" viewBox="0 0 24 24" width="14" height="14" style="color:var(--text-muted);">
                        <circle cx="12" cy="12" r="3"></circle>
                        <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"></path>
                      </svg>
                      <span>${escapeHTML(step.name || step.id)}</span>
                    </div>

                    <div class="dag-node-cmd" title="${escapeHTML(cmdDisplay)}">${escapeHTML(cmdDisplay || "(no command)")}</div>

                    ${deps.length > 0 ? `
                      <div class="dag-node-deps">
                        <span style="font-size:0.65rem;color:var(--text-muted);">deps:</span>
                        ${deps.map((d) => `<span class="dag-dep-tag">${escapeHTML(d)}</span>`).join("")}
                      </div>
                    ` : ""}

                    <div class="dag-node-footer">
                      <span>${escapeHTML(exitCodeStr || (step.timeout_sec ? `${step.timeout_sec}s timeout` : "standard"))}</span>
                      <span style="color:var(--text-muted);">${escapeHTML(step.id)}</span>
                    </div>

                    ${isRunning ? `
                      <div class="dag-node-progress">
                        <div class="dag-node-progress-bar" style="width: 65%;"></div>
                      </div>
                    ` : ""}
                  </div>
                `;
              })
              .join("")}
          </div>
        `;
      })
      .join("");

    // Bind Node Click to Step Inspector Drawer
    elements.dagContainer.querySelectorAll(".dag-node").forEach((node) => {
      node.addEventListener("click", () => {
        const stepId = node.dataset.stepId;
        openStepInspector(stepId);
      });
    });

    // Schedule SVG cubic bezier connectors render
    requestAnimationFrame(() => {
      renderSVGConnectors(steps, stepRunMap);
      updateRadarHUD();
    });

    updateProgressTelemetry(steps, stepRunMap);
  }

  // Draw Smooth Cubic Bezier Curves Between Dependent Nodes
  function renderSVGConnectors(steps, stepRunMap) {
    if (!elements.dagSvgEdgesGroup || !elements.dagContainer) return;

    elements.dagSvgEdgesGroup.innerHTML = "";
    const containerRect = elements.dagContainer.getBoundingClientRect();

    steps.forEach((child) => {
      const deps = child.depends_on || [];
      if (deps.length === 0) return;

      const childEl = document.getElementById(`node-${child.id}`);
      if (!childEl) return;
      const childRect = childEl.getBoundingClientRect();

      // Input port: middle left of child node relative to dagContainer
      const childX = childRect.left - containerRect.left;
      const childY = childRect.top - containerRect.top + childRect.height / 2;

      deps.forEach((parentStepId) => {
        const parentEl = document.getElementById(`node-${parentStepId}`);
        if (!parentEl) return;
        const parentRect = parentEl.getBoundingClientRect();

        // Output port: middle right of parent node relative to dagContainer
        const parentX = parentRect.right - containerRect.left;
        const parentY = parentRect.top - containerRect.top + parentRect.height / 2;

        // Cubic bezier math: M(x1, y1) C(x1 + dx, y1), (x2 - dx, y2), (x2, y2)
        const dx = Math.max(40, (childX - parentX) * 0.5);
        const pathData = `M ${parentX} ${parentY} C ${parentX + dx} ${parentY}, ${childX - dx} ${childY}, ${childX} ${childY}`;

        // Determine edge state
        const parentRun = stepRunMap[parentStepId];
        const childRun = stepRunMap[child.id];
        const parentStatus = parentRun ? parentRun.status : "PENDING";
        const childStatus = childRun ? childRun.status : "PENDING";

        let edgeClass = "dag-edge";
        let markerId = "arrow-default";

        if (childStatus === "RUNNING" || (parentStatus === "COMPLETED" && childStatus === "RUNNING")) {
          edgeClass += " running";
          markerId = "arrow-running";
        } else if (parentStatus === "COMPLETED" && childStatus === "COMPLETED") {
          edgeClass += " completed";
          markerId = "arrow-completed";
        } else if (childStatus === "FAILED" || parentStatus === "FAILED") {
          edgeClass += " failed";
        }

        const pathEl = document.createElementNS("http://www.w3.org/2000/svg", "path");
        pathEl.setAttribute("d", pathData);
        pathEl.setAttribute("class", edgeClass);
        pathEl.setAttribute("marker-end", `url(#${markerId})`);
        pathEl.dataset.from = parentStepId;
        pathEl.dataset.to = child.id;

        elements.dagSvgEdgesGroup.appendChild(pathEl);
      });
    });
  }

  // Update in-place node statuses without resetting zoom or DOM tree
  function updateDAGInPlace(stepRuns) {
    if (!stepRuns || stepRuns.length === 0) return;
    const stepRunMap = {};
    stepRuns.forEach((sr) => {
      stepRunMap[sr.step_id] = sr;
    });

    stepRuns.forEach((sr) => {
      const nodeEl = document.getElementById(`node-${sr.step_id}`);
      if (!nodeEl) return;

      const currentStatus = sr.status || "PENDING";
      const statusClass = `status-${currentStatus.toLowerCase()}`;
      nodeEl.className = `dag-node ${statusClass} ${state.selectedStepId === sr.step_id ? "active-selected" : ""}`;

      const badge = nodeEl.querySelector(".badge");
      if (badge) {
        badge.className = `badge badge-${currentStatus.toLowerCase()}`;
        badge.textContent = currentStatus;
      }

      const timer = nodeEl.querySelector(".dag-node-timer");
      if (timer) {
        if (sr.end_time && sr.start_time) {
          const dur = ((new Date(sr.end_time) - new Date(sr.start_time)) / 1000).toFixed(2);
          timer.textContent = `${dur}s`;
        } else if (currentStatus === "RUNNING") {
          timer.textContent = "running...";
        }
      }
    });

    if (state.selectedWorkflow && state.selectedWorkflow.steps) {
      renderSVGConnectors(state.selectedWorkflow.steps, stepRunMap);
      updateProgressTelemetry(state.selectedWorkflow.steps, stepRunMap);
    }
  }

  function updateProgressTelemetry(steps, stepRunMap) {
    if (!steps || steps.length === 0) return;
    const total = steps.length;
    let done = 0;

    steps.forEach((s) => {
      const sr = stepRunMap[s.id];
      if (sr && (sr.status === "COMPLETED" || sr.status === "SKIPPED")) {
        done++;
      }
    });

    const pct = Math.round((done / total) * 100);
    if (elements.dagStepsProgress) elements.dagStepsProgress.textContent = `${done}/${total} done`;
    if (elements.dagProgressFill) elements.dagProgressFill.style.width = `${pct}%`;
    if (elements.dagProgressPct) elements.dagProgressPct.textContent = `${pct}%`;
  }

  // ==========================================
  // PAN & ZOOM CONTROLLER (SILKY 60FPS)
  // ==========================================
  function applyCanvasTransform() {
    if (!elements.dagViewport) return;
    elements.dagViewport.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.zoomScale})`;
    if (elements.canvasZoomReset) {
      elements.canvasZoomReset.textContent = `${Math.round(state.zoomScale * 100)}%`;
    }
    updateRadarHUD();
  }

  // Mouse drag to pan
  elements.dagCanvasContainer.addEventListener("mousedown", (e) => {
    if (e.target.closest(".dag-node") || e.target.closest(".canvas-controls") || e.target.closest(".radar-hud")) {
      return;
    }
    state.isPanning = true;
    state.panStartX = e.clientX;
    state.panStartY = e.clientY;
    state.basePanX = state.panX;
    state.basePanY = state.panY;
  });

  window.addEventListener("mousemove", (e) => {
    if (!state.isPanning) return;
    const dx = e.clientX - state.panStartX;
    const dy = e.clientY - state.panStartY;
    state.panX = state.basePanX + dx;
    state.panY = state.basePanY + dy;
    applyCanvasTransform();
  });

  window.addEventListener("mouseup", () => {
    state.isPanning = false;
  });

  // Mouse wheel to zoom centered on pointer
  elements.dagCanvasContainer.addEventListener(
    "wheel",
    (e) => {
      e.preventDefault();
      const zoomFactor = e.deltaY < 0 ? 1.12 : 0.88;
      const newScale = Math.min(2.5, Math.max(0.4, state.zoomScale * zoomFactor));

      const rect = elements.dagCanvasContainer.getBoundingClientRect();
      const mouseX = e.clientX - rect.left;
      const mouseY = e.clientY - rect.top;

      // Adjust pan so point under cursor remains fixed
      state.panX = mouseX - (mouseX - state.panX) * (newScale / state.zoomScale);
      state.panY = mouseY - (mouseY - state.panY) * (newScale / state.zoomScale);
      state.zoomScale = newScale;

      applyCanvasTransform();
    },
    { passive: false }
  );

  // Floating controls buttons
  if (elements.canvasZoomIn) {
    elements.canvasZoomIn.addEventListener("click", () => {
      state.zoomScale = Math.min(2.5, state.zoomScale + 0.15);
      applyCanvasTransform();
    });
  }

  if (elements.canvasZoomOut) {
    elements.canvasZoomOut.addEventListener("click", () => {
      state.zoomScale = Math.max(0.4, state.zoomScale - 0.15);
      applyCanvasTransform();
    });
  }

  if (elements.canvasZoomReset) {
    elements.canvasZoomReset.addEventListener("click", () => {
      state.zoomScale = 1.0;
      state.panX = 0;
      state.panY = 0;
      applyCanvasTransform();
    });
  }

  if (elements.canvasFitViewport) {
    elements.canvasFitViewport.addEventListener("click", () => fitDAGToViewport());
  }

  function fitDAGToViewport() {
    if (!elements.dagContainer || !elements.dagCanvasContainer) return;
    const containerRect = elements.dagContainer.getBoundingClientRect();
    const canvasRect = elements.dagCanvasContainer.getBoundingClientRect();

    if (containerRect.width === 0 || canvasRect.width === 0) return;

    const scaleX = (canvasRect.width - 80) / containerRect.width;
    const scaleY = (canvasRect.height - 80) / containerRect.height;
    const fitScale = Math.min(1.2, Math.max(0.45, Math.min(scaleX, scaleY)));

    state.zoomScale = fitScale;
    state.panX = Math.round((canvasRect.width - containerRect.width * fitScale) / 2);
    state.panY = Math.round((canvasRect.height - containerRect.height * fitScale) / 2);

    applyCanvasTransform();
  }

  // Radar HUD Mini-Map
  function updateRadarHUD() {
    if (!elements.radarViewportBox || !elements.dagCanvasContainer) return;

    const canvasWidth = elements.dagCanvasContainer.clientWidth || 800;
    const canvasHeight = elements.dagCanvasContainer.clientHeight || 500;
    const hudWidth = 140;
    const hudHeight = 54;

    const scale = state.zoomScale;
    const boxW = Math.min(hudWidth, Math.max(16, (hudWidth / scale) * (hudWidth / canvasWidth)));
    const boxH = Math.min(hudHeight, Math.max(12, (hudHeight / scale) * (hudHeight / canvasHeight)));

    const normX = -state.panX / (canvasWidth * scale);
    const normY = -state.panY / (canvasHeight * scale);

    const boxLeft = Math.min(hudWidth - boxW, Math.max(0, normX * hudWidth + 10));
    const boxTop = Math.min(hudHeight - boxH, Math.max(0, normY * hudHeight + 5));

    elements.radarViewportBox.style.width = `${boxW}px`;
    elements.radarViewportBox.style.height = `${boxH}px`;
    elements.radarViewportBox.style.left = `${boxLeft}px`;
    elements.radarViewportBox.style.top = `${boxTop}px`;
  }

  // ==========================================
  // SLIDE-OVER STEP INSPECTOR DRAWER
  // ==========================================
  function openStepInspector(stepId) {
    state.selectedStepId = stepId;
    elements.nodeInspector.classList.remove("collapsed");

    // Highlight node card
    document.querySelectorAll(".dag-node").forEach((n) => {
      n.classList.toggle("active-selected", n.dataset.stepId === stepId);
    });

    if (!state.selectedWorkflow || !state.selectedWorkflow.steps) return;
    const step = state.selectedWorkflow.steps.find((s) => s.id === stepId);
    if (!step) return;

    const stepRun = state.inspectingRunId && state.runStepCache[state.inspectingRunId]
      ? state.runStepCache[state.inspectingRunId].find((sr) => sr.step_id === stepId)
      : null;

    const status = stepRun ? (stepRun.status || "PENDING") : "PENDING";

    elements.inspectorStepTitle.textContent = step.name || step.id;
    elements.inspectorStepBadge.className = `badge badge-${status.toLowerCase()}`;
    elements.inspectorStepBadge.textContent = status;

    const argsStr = (step.args || []).join(" ");
    const fullCmd = `${step.command || ""} ${argsStr}`.trim();
    elements.inspectorCmdBox.textContent = fullCmd || "(no command configured)";

    elements.inspectorWorkdir.textContent = step.workdir || (state.inspectingRunId ? `/var/run/glowkron/workspaces/${state.inspectingRunId}` : "default workspace");

    // Environment Table
    const env = step.env || {};
    const envKeys = Object.keys(env);
    if (envKeys.length > 0) {
      elements.inspectorEnvTable.innerHTML = envKeys
        .map((k) => `<tr><td class="env-key font-mono">${escapeHTML(k)}</td><td class="env-val font-mono">${escapeHTML(env[k])}</td></tr>`)
        .join("");
    } else {
      elements.inspectorEnvTable.innerHTML = `<tr><td colspan="2" style="color:var(--text-dim);font-style:italic;">No custom environment variables</td></tr>`;
    }

    // Upstream Dependencies
    const deps = step.depends_on || [];
    if (deps.length > 0) {
      elements.inspectorDepsList.innerHTML = deps
        .map((depId) => {
          const depRun = state.inspectingRunId && state.runStepCache[state.inspectingRunId]
            ? state.runStepCache[state.inspectingRunId].find((sr) => sr.step_id === depId)
            : null;
          const depStatus = depRun ? depRun.status : "PENDING";
          return `
            <div class="dep-pill" data-dep-id="${escapeHTML(depId)}">
              <div class="dep-pill-left">
                <span class="status-indicator-dot" style="display:inline-block;width:6px;height:6px;border-radius:50%;background:currentColor;"></span>
                <span>${escapeHTML(depId)}</span>
              </div>
              <span class="badge badge-${depStatus.toLowerCase()}">${depStatus}</span>
            </div>
          `;
        })
        .join("");

      elements.inspectorDepsList.querySelectorAll(".dep-pill").forEach((pill) => {
        pill.addEventListener("click", () => {
          openStepInspector(pill.dataset.depId);
        });
      });
    } else {
      elements.inspectorDepsList.innerHTML = `<span style="font-family:var(--font-mono);font-size:0.75rem;color:var(--text-dim);">Root node (0 prerequisites)</span>`;
    }

    // Stdout Snippet
    if (stepRun && stepRun.stdout_snippet) {
      elements.inspectorStdoutSection.style.display = "flex";
      elements.inspectorStdoutBox.textContent = stepRun.stdout_snippet;
    } else {
      elements.inspectorStdoutSection.style.display = "none";
    }

    // Stderr Snippet
    if (stepRun && stepRun.stderr_snippet) {
      elements.inspectorStderrSection.style.display = "flex";
      elements.inspectorStderrBox.textContent = stepRun.stderr_snippet;
    } else {
      elements.inspectorStderrSection.style.display = "none";
    }
  }

  // Inspector Close Button
  if (elements.inspectorCloseBtn) {
    elements.inspectorCloseBtn.addEventListener("click", () => {
      elements.nodeInspector.classList.add("collapsed");
      state.selectedStepId = null;
      document.querySelectorAll(".dag-node").forEach((n) => n.classList.remove("active-selected"));
    });
  }

  // 1-Click Copy Command Button
  if (elements.inspectorCopyCmd) {
    elements.inspectorCopyCmd.addEventListener("click", () => {
      const text = elements.inspectorCmdBox.textContent;
      navigator.clipboard.writeText(text).then(() => {
        showToast({ title: "Copied", message: "Command copied to clipboard", type: "info" });
      });
    });
  }

  // Reset DAG View to baseline template
  if (elements.dagResetTemplateBtn) {
    elements.dagResetTemplateBtn.addEventListener("click", () => {
      state.inspectingRunId = null;
      state.inspectingRun = null;
      renderDAG();
    });
  }

  // Refresh DAG button
  if (elements.dagRefreshBtn) {
    elements.dagRefreshBtn.addEventListener("click", async () => {
      if (state.inspectingRunId) {
        await refreshInspectedRunSteps(state.inspectingRunId);
      } else if (state.selectedWorkflow) {
        await selectWorkflow(state.selectedWorkflow.id || state.selectedWorkflow.name);
      }
    });
  }

  // Download raw logs
  if (elements.dagActionDownloadLogs) {
    elements.dagActionDownloadLogs.addEventListener("click", () => {
      const runId = state.inspectingRunId || state.activeStreamRunId;
      if (!runId) {
        showToast({ title: "No Run", message: "Select an active or past execution run to download logs.", type: "warn" });
        return;
      }
      window.open(`/api/runs/${encodeURIComponent(runId)}/logs`, "_blank");
    });
  }

  // Rerun Ad-Hoc button on DAG control bar
  if (elements.dagActionRerun) {
    elements.dagActionRerun.addEventListener("click", () => {
      if (state.selectedWorkflow) {
        triggerWorkflow(state.selectedWorkflow.id || state.selectedWorkflow.name);
      }
    });
  }

  function startRunElapsedTimer() {
    stopRunElapsedTimer();
    state.runElapsedSeconds = 0;
    state.runTimerInterval = setInterval(() => {
      state.runElapsedSeconds++;
      const m = String(Math.floor(state.runElapsedSeconds / 60)).padStart(2, "0");
      const s = String(state.runElapsedSeconds % 60).padStart(2, "0");
      if (elements.dagElapsedTime) {
        elements.dagElapsedTime.textContent = `00:${m}:${s}`;
      }
    }, 1000);
  }

  function stopRunElapsedTimer() {
    if (state.runTimerInterval) {
      clearInterval(state.runTimerInterval);
      state.runTimerInterval = null;
    }
  }

  // ==========================================
  // LIVE SSE TERMINAL CONSOLE & STREAMING LOGS
  // ==========================================
  function streamLogs(runId) {
    closeSSEStream();

    state.activeStreamRunId = runId;
    state.reconnectAttempts = 0;
    state.streamBytesReceived = 0;
    state.streamStartTime = Date.now();
    state.rawLogLines = [];
    state.userScrolledUp = false;

    elements.terminalRunId.textContent = `Run: ${runId.substring(0, 16)}...`;
    elements.terminalConnStatus.className = "badge badge-running";
    elements.terminalConnStatus.textContent = "Connecting";
    elements.terminalLiveIndicator.style.display = "inline-block";

    elements.headerActiveRun.style.display = "flex";
    elements.headerActiveRunText.textContent = `Streaming: ${runId.substring(0, 12)}...`;

    elements.terminalLogs.innerHTML = "";
    appendLogLine({
      text: `[Connecting to live log stream for run #${runId}...]`,
      stream: "stdout",
      tag: "INFO",
      timestamp: new Date().toISOString(),
      relSec: 0,
    });

    startStepPoll(runId);
    connectSSE(runId);
  }

  function connectSSE(runId) {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }

    const streamUrl = `/api/runs/${encodeURIComponent(runId)}/stream`;
    const es = new EventSource(streamUrl);
    state.eventSource = es;

    es.onopen = () => {
      elements.terminalConnStatus.className = "badge badge-completed";
      elements.terminalConnStatus.textContent = "Live";
      elements.terminalMetaStreamStatus.textContent = "Stream: Connected";
      state.reconnectAttempts = 0;
    };

    es.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.chunk) {
          queueLogChunk(payload.chunk, payload.stream || "stdout");
        } else {
          queueLogChunk(event.data + "\n", "stdout");
        }
      } catch {
        queueLogChunk(event.data + "\n", "stdout");
      }
    };

    es.addEventListener("done", () => {
      queueLogChunk("\n[Stream finalized: execution completed successfully]\n", "stdout");
      elements.terminalConnStatus.className = "badge badge-completed";
      elements.terminalConnStatus.textContent = "Finished";
      elements.terminalMetaStreamStatus.textContent = "Stream: Concluded";
      closeSSEStream(false);
      refreshInspectedRunSteps(runId);
      loadRuns();
    });

    es.onerror = () => {
      handleSSEError(runId);
    };
  }

  function handleSSEError(runId) {
    if (state.activeStreamRunId !== runId) return;

    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }

    fetch(`/api/runs/${encodeURIComponent(runId)}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((run) => {
        if (run && (run.status === "COMPLETED" || run.status === "FAILED")) {
          fetch(`/api/runs/${encodeURIComponent(runId)}/logs`)
            .then((res) => (res.ok ? res.text() : ""))
            .then((fullText) => {
              if (fullText) {
                state.rawLogLines = [];
                elements.terminalLogs.innerHTML = "";
                queueLogChunk(fullText, "stdout");
              }
            })
            .catch(() => {});

          elements.terminalConnStatus.className = `badge badge-${run.status.toLowerCase()}`;
          elements.terminalConnStatus.textContent = run.status;
          elements.terminalMetaStreamStatus.textContent = "Stream: Concluded";
          stopStepPoll();
          refreshInspectedRunSteps(runId);
          loadRuns();
          return;
        }

        // Auto-reconnect with exponential backoff
        if (state.reconnectAttempts < state.maxReconnectAttempts) {
          state.reconnectAttempts++;
          const delay = Math.min(1000 * Math.pow(1.8, state.reconnectAttempts), 8000);
          elements.terminalConnStatus.className = "badge badge-skipped";
          elements.terminalConnStatus.textContent = `Reconnecting (${state.reconnectAttempts})...`;
          elements.terminalMetaStreamStatus.textContent = `Stream: Reconnecting in ${(delay / 1000).toFixed(1)}s`;

          queueLogChunk(`\n[Stream dropped. Auto-reconnecting attempt ${state.reconnectAttempts}/${state.maxReconnectAttempts}...]\n`, "stdout");

          state.reconnectTimer = setTimeout(() => {
            if (state.activeStreamRunId === runId) {
              connectSSE(runId);
            }
          }, delay);
        } else {
          elements.terminalConnStatus.className = "badge badge-failed";
          elements.terminalConnStatus.textContent = "Disconnected";
          elements.terminalMetaStreamStatus.textContent = "Stream: Reconnect failed";
          queueLogChunk("\n[Reconnection failed. Terminal stream closed.]\n", "stderr");
          stopStepPoll();
        }
      })
      .catch(() => {
        elements.terminalConnStatus.className = "badge badge-failed";
        elements.terminalConnStatus.textContent = "Offline";
        stopStepPoll();
      });
  }

  function queueLogChunk(rawText, stream = "stdout") {
    if (!rawText) return;
    state.streamBytesReceived += rawText.length;

    const lines = rawText.split("\n");
    const now = new Date();
    const relSec = state.streamStartTime ? ((now.getTime() - state.streamStartTime) / 1000).toFixed(2) : "0.00";

    lines.forEach((line, idx) => {
      if (idx === lines.length - 1 && line === "") return;

      const lower = line.toLowerCase();
      let tag = "INFO";
      if (stream === "stderr" || lower.includes("[error]") || lower.includes("failed") || lower.includes("err:")) {
        tag = "ERR";
      } else if (lower.includes("done") || lower.includes("completed cleanly") || lower.includes("success")) {
        tag = "DONE";
      } else if (lower.includes("fork") || lower.includes("parallel child")) {
        tag = "FORK";
      } else if (lower.includes("exec") || lower.includes("pg_dump") || lower.includes("dispatching")) {
        tag = "EXEC";
      } else if (lower.includes("pipe") || lower.includes("streamed chunk") || lower.includes("gzip")) {
        tag = "PIPE";
      } else if (lower.includes("sse") || lower.includes("event:")) {
        tag = "SSE";
      }

      state.pendingLogBatch.push({
        text: line,
        stream: stream,
        tag: tag,
        timestamp: now.toISOString(),
        relSec: `+${relSec}s`,
      });
    });

    if (!state.rafScheduled) {
      state.rafScheduled = true;
      requestAnimationFrame(flushLogBatch);
    }
  }

  // 60FPS RAF Bounded Buffer Flush
  function flushLogBatch() {
    state.rafScheduled = false;
    const batch = state.pendingLogBatch;
    state.pendingLogBatch = [];
    if (batch.length === 0) return;

    // Append to in-memory buffer with FIFO pruning
    batch.forEach((item) => state.rawLogLines.push(item));
    if (state.rawLogLines.length > state.maxLogLines) {
      state.rawLogLines = state.rawLogLines.slice(state.rawLogLines.length - state.maxLogLines);
    }

    // Filter by active stream tab & search query
    const fragment = document.createDocumentFragment();
    let stdoutCount = 0;
    let stderrCount = 0;

    state.rawLogLines.forEach((item) => {
      if (item.stream === "stdout") stdoutCount++;
      if (item.stream === "stderr" || item.tag === "ERR") stderrCount++;
    });

    if (elements.tabCountAll) elements.tabCountAll.textContent = state.rawLogLines.length;
    if (elements.tabCountStdout) elements.tabCountStdout.textContent = stdoutCount;
    if (elements.tabCountStderr) elements.tabCountStderr.textContent = stderrCount;

    batch.forEach((item) => {
      if (!matchesTab(item, state.terminalStreamTab)) return;
      if (!matchesSearch(item, state.terminalSearchQuery)) return;

      fragment.appendChild(createLogLineNode(item));
    });

    elements.terminalLogs.appendChild(fragment);

    // Bounded DOM child pruning
    while (elements.terminalLogs.childNodes.length > state.maxLogLines) {
      elements.terminalLogs.removeChild(elements.terminalLogs.firstChild);
    }

    // Telemetry footer
    elements.terminalMetaLines.textContent = `${state.rawLogLines.length} lines`;
    elements.terminalMetaBytes.textContent = formatBytes(state.streamBytesReceived);
    if (elements.terminalSseBytes) elements.terminalSseBytes.textContent = `(${formatBytes(state.streamBytesReceived)})`;

    // Auto-scroll logic
    if (state.autoScroll && !state.userScrolledUp) {
      elements.terminalLogs.scrollTop = elements.terminalLogs.scrollHeight;
    }
  }

  function matchesTab(item, tab) {
    if (tab === "all") return true;
    if (tab === "stdout") return item.stream === "stdout";
    if (tab === "stderr") return item.stream === "stderr" || item.tag === "ERR";
    if (tab === "telemetry") return item.tag === "SSE" || item.tag === "PIPE";
    return true;
  }

  function matchesSearch(item, query) {
    if (!query) return true;
    try {
      const reg = new RegExp(query, "i");
      return reg.test(item.text);
    } catch {
      return item.text.toLowerCase().includes(query.toLowerCase());
    }
  }

  function createLogLineNode(item) {
    const lineEl = document.createElement("div");
    lineEl.className = "log-line";

    let timeStr = item.relSec;
    if (state.terminalShowUTC) {
      try {
        timeStr = new Date(item.timestamp).toLocaleTimeString();
      } catch {
        timeStr = item.timestamp ? item.timestamp.substring(11, 19) : "";
      }
    }

    let highlightedText = escapeHTML(item.text);
    if (state.terminalSearchQuery) {
      try {
        const reg = new RegExp(`(${escapeRegExp(state.terminalSearchQuery)})`, "gi");
        highlightedText = highlightedText.replace(reg, "<mark>$1</mark>");
      } catch {}
    }

    lineEl.innerHTML = `
      <span class="log-time">${escapeHTML(timeStr)}</span>
      <span class="log-tag tag-${item.tag.toLowerCase()}">${item.tag}</span>
      <span class="log-content">${highlightedText}</span>
    `;

    return lineEl;
  }

  function rerenderFilteredTerminal() {
    elements.terminalLogs.innerHTML = "";
    const fragment = document.createDocumentFragment();
    let matchCount = 0;

    state.rawLogLines.forEach((item) => {
      if (!matchesTab(item, state.terminalStreamTab)) return;
      if (!matchesSearch(item, state.terminalSearchQuery)) return;

      matchCount++;
      fragment.appendChild(createLogLineNode(item));
    });

    elements.terminalLogs.appendChild(fragment);

    if (elements.terminalMatchCount) {
      if (state.terminalSearchQuery) {
        elements.terminalMatchCount.style.display = "inline-block";
        elements.terminalMatchCount.textContent = `${matchCount} matches`;
      } else {
        elements.terminalMatchCount.style.display = "none";
      }
    }

    if (state.autoScroll && !state.userScrolledUp) {
      elements.terminalLogs.scrollTop = elements.terminalLogs.scrollHeight;
    }
  }

  // Stream Tabs Listeners
  document.querySelectorAll(".stream-tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".stream-tab").forEach((t) => t.classList.remove("active"));
      tab.classList.add("active");
      state.terminalStreamTab = tab.dataset.stream;
      rerenderFilteredTerminal();
    });
  });

  // In-terminal search input
  if (elements.terminalSearchInput) {
    elements.terminalSearchInput.addEventListener("input", (e) => {
      state.terminalSearchQuery = e.target.value.trim();
      rerenderFilteredTerminal();
    });
  }

  // Timestamps toggle (Relative vs Local)
  if (elements.terminalToggleTimestamps) {
    elements.terminalToggleTimestamps.addEventListener("click", () => {
      state.terminalShowUTC = !state.terminalShowUTC;
      elements.terminalToggleTimestamps.querySelector("span").textContent = state.terminalShowUTC ? "Local Time" : "Rel Time";
      elements.terminalToggleTimestamps.classList.toggle("active", state.terminalShowUTC);
      rerenderFilteredTerminal();
    });
  }

  // Word wrap toggle
  if (elements.terminalToggleWrap) {
    elements.terminalToggleWrap.addEventListener("click", () => {
      state.terminalWordWrap = !state.terminalWordWrap;
      elements.terminalLogs.classList.toggle("wrap-active", state.terminalWordWrap);
      elements.terminalToggleWrap.classList.toggle("active", state.terminalWordWrap);
    });
  }

  // Height Expand (280px <-> 480px)
  if (elements.terminalToggleExpand) {
    elements.terminalToggleExpand.addEventListener("click", () => {
      state.terminalExpanded = !state.terminalExpanded;
      elements.terminalPanel.classList.toggle("expanded", state.terminalExpanded);
      elements.terminalToggleExpand.classList.toggle("active", state.terminalExpanded);
    });
  }

  function expandTerminal(expanded) {
    state.terminalExpanded = expanded;
    if (elements.terminalPanel) {
      elements.terminalPanel.classList.toggle("expanded", expanded);
    }
  }

  // Fullscreen modal toggle
  if (elements.terminalToggleFullscreen) {
    elements.terminalToggleFullscreen.addEventListener("click", () => {
      state.terminalFullscreen = !state.terminalFullscreen;
      elements.terminalPanel.classList.toggle("fullscreen", state.terminalFullscreen);
      elements.terminalToggleFullscreen.classList.toggle("active", state.terminalFullscreen);
    });
  }

  // Auto-scroll toggle & scroll lock detection
  elements.terminalLogs.addEventListener("scroll", () => {
    const el = elements.terminalLogs;
    const fromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;

    if (fromBottom > 40) {
      state.userScrolledUp = true;
      elements.terminalScrollHint.className = "scroll-status-badge locked";
      elements.terminalScrollHint.textContent = "Scroll Locked";
    } else {
      state.userScrolledUp = false;
      elements.terminalScrollHint.className = "scroll-status-badge";
      elements.terminalScrollHint.textContent = state.autoScroll ? "Auto-scrolling" : "Scroll Paused";
    }
  });

  if (elements.autoScrollToggle) {
    elements.autoScrollToggle.addEventListener("click", () => {
      state.autoScroll = !state.autoScroll;
      state.userScrolledUp = false;
      elements.autoScrollToggle.classList.toggle("active", state.autoScroll);
      elements.autoScrollLabel.textContent = state.autoScroll ? "Auto: ON" : "Auto: OFF";

      if (state.autoScroll) {
        elements.terminalLogs.scrollTop = elements.terminalLogs.scrollHeight;
        elements.terminalScrollHint.className = "scroll-status-badge";
        elements.terminalScrollHint.textContent = "Auto-scrolling";
      } else {
        elements.terminalScrollHint.className = "scroll-status-badge locked";
        elements.terminalScrollHint.textContent = "Auto OFF";
      }
    });
  }

  // Copy terminal logs
  if (elements.copyTerminalBtn) {
    elements.copyTerminalBtn.addEventListener("click", () => {
      const text = state.rawLogLines.map((l) => `${l.relSec} [${l.tag}] ${l.text}`).join("\n");
      if (!text) return;
      navigator.clipboard.writeText(text).then(() => {
        showToast({ title: "Copied", message: "Terminal logs copied to clipboard", type: "info" });
      });
    });
  }

  // Clear terminal buffer
  if (elements.clearTerminalBtn) {
    elements.clearTerminalBtn.addEventListener("click", () => {
      state.rawLogLines = [];
      elements.terminalLogs.innerHTML = "";
      elements.terminalMetaLines.textContent = "0 lines";
      elements.terminalMetaBytes.textContent = "0 B";
      if (elements.tabCountAll) elements.tabCountAll.textContent = "0";
      if (elements.tabCountStdout) elements.tabCountStdout.textContent = "0";
      if (elements.tabCountStderr) elements.tabCountStderr.textContent = "0";
    });
  }

  // Inspect DAG from terminal
  if (elements.inspectDagBtn) {
    elements.inspectDagBtn.addEventListener("click", () => {
      if (state.activeStreamRunId) {
        inspectRunDAG(state.activeStreamRunId);
        switchTab("workflows");
        if (elements.dagControlBar) {
          elements.dagControlBar.scrollIntoView({ behavior: "smooth" });
        }
      } else {
        showToast({ title: "No Run", message: "No active execution run currently selected.", type: "warn" });
      }
    });
  }

  function startStepPoll(runId) {
    stopStepPoll();
    state.stepPollTimer = setInterval(() => {
      if (state.activeStreamRunId === runId) {
        refreshInspectedRunSteps(runId);
      }
    }, 1500);
  }

  function stopStepPoll() {
    if (state.stepPollTimer) {
      clearInterval(state.stepPollTimer);
      state.stepPollTimer = null;
    }
  }

  function closeSSEStream(clearHeaders = true) {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    if (state.reconnectTimer) {
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = null;
    }
    stopStepPoll();

    if (clearHeaders) {
      elements.terminalLiveIndicator.style.display = "none";
      elements.headerActiveRun.style.display = "none";
    }
  }

  // ==========================================
  // EXECUTION RUNS HISTORY VIEW
  // ==========================================
  async function loadRuns() {
    try {
      const res = await fetch("/api/runs?limit=50");
      if (res.ok) {
        state.runs = await res.json();
        updateRunStats();
        renderRunsTable();
      }
    } catch (err) {
      console.error("Failed to load runs:", err);
    }
  }

  function updateRunStats() {
    const runs = state.runs || [];
    const total = runs.length;
    let running = 0;
    let completed = 0;
    let failed = 0;

    runs.forEach((r) => {
      const s = (r.status || "").toUpperCase();
      if (s === "RUNNING") running++;
      else if (s === "COMPLETED") completed++;
      else if (s === "FAILED" || s === "CANCELLED") failed++;
    });

    elements.statTotalRuns.textContent = total;
    elements.statRunningRuns.textContent = running;
    elements.statCompletedRuns.textContent = completed;
    elements.statFailedRuns.textContent = failed;

    if (running > 0) {
      elements.badgeRunningCount.style.display = "inline-block";
      elements.badgeRunningCount.textContent = running;
    } else {
      elements.badgeRunningCount.style.display = "none";
    }
  }

  function renderRunsTable() {
    const runs = state.runs || [];

    const filtered = runs.filter((r) => {
      const statusMatch =
        state.statusFilter === "all" ||
        (r.status || "").toLowerCase() === state.statusFilter.toLowerCase();

      if (!statusMatch) return false;

      if (!state.searchQuery) return true;
      const q = state.searchQuery.toLowerCase();
      const idMatch = (r.id || "").toLowerCase().includes(q);
      const jobMatch = (r.job_id || "").toLowerCase().includes(q);
      const trigMatch = (r.triggered_by || "").toLowerCase().includes(q);

      return idMatch || jobMatch || trigMatch;
    });

    if (filtered.length === 0) {
      elements.runsTableBody.innerHTML = `
        <tr>
          <td colspan="7" class="table-loading">No execution runs match your criteria.</td>
        </tr>
      `;
      return;
    }

    elements.runsTableBody.innerHTML = filtered
      .map((r) => {
        const isStreaming = state.activeStreamRunId === r.id;
        const isInspected = state.inspectingRunId === r.id;
        const rowClass = isInspected || isStreaming ? "row-active" : "";
        const status = r.status || "PENDING";
        const badgeClass = `badge-${status.toLowerCase()}`;
        const trigger = r.triggered_by || "MANUAL";

        let duration = "-";
        if (r.end_time && r.start_time) {
          const s = new Date(r.start_time).getTime();
          const e = new Date(r.end_time).getTime();
          duration = `${((e - s) / 1000).toFixed(2)}s`;
        } else if (status === "RUNNING" && r.start_time) {
          const s = new Date(r.start_time).getTime();
          duration = `${((Date.now() - s) / 1000).toFixed(1)}s (live)`;
        }

        const startFormatted = r.start_time ? new Date(r.start_time).toLocaleString() : "-";

        return `
          <tr class="${rowClass}" data-run-id="${escapeHTML(r.id)}">
            <td>
              <code style="color:var(--accent-cyan);cursor:pointer;" class="btn-copy-id" data-copy="${escapeHTML(r.id)}" title="Copy full Run ID">
                ${escapeHTML(r.id.substring(0, 14))}...
              </code>
            </td>
            <td><strong>${escapeHTML(r.job_id)}</strong></td>
            <td><span class="badge ${badgeClass}">${status}</span></td>
            <td><span class="badge badge-muted">${escapeHTML(trigger)}</span></td>
            <td style="white-space:nowrap;font-size:0.78rem;">${escapeHTML(startFormatted)}</td>
            <td style="font-family:var(--font-mono);font-size:0.78rem;">${duration}</td>
            <td class="text-right">
              <button class="btn btn-secondary btn-sm btn-inspect-run" data-id="${escapeHTML(r.id)}">
                Inspect
              </button>
              <button class="btn btn-primary btn-sm btn-stream-run" data-id="${escapeHTML(r.id)}">
                Logs
              </button>
            </td>
          </tr>
        `;
      })
      .join("");

    elements.runsTableBody.querySelectorAll(".btn-copy-id").forEach((el) => {
      el.addEventListener("click", (e) => {
        e.stopPropagation();
        navigator.clipboard.writeText(el.dataset.copy).then(() => {
          showToast({ title: "Copied", message: "Run ID copied to clipboard", type: "info" });
        });
      });
    });

    elements.runsTableBody.querySelectorAll(".btn-inspect-run").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const runId = btn.dataset.id;
        inspectRunDAG(runId);
        switchTab("workflows");
        if (elements.dagControlBar) {
          elements.dagControlBar.scrollIntoView({ behavior: "smooth" });
        }
      });
    });

    elements.runsTableBody.querySelectorAll(".btn-stream-run").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const runId = btn.dataset.id;
        switchTab("workflows");
        streamLogs(runId);
        if (elements.terminalPanel) {
          elements.terminalPanel.scrollIntoView({ behavior: "smooth" });
        }
      });
    });

    elements.runsTableBody.querySelectorAll("tr").forEach((row) => {
      row.addEventListener("click", () => {
        const id = row.dataset.runId;
        if (id) {
          inspectRunDAG(id);
          document.querySelectorAll("#runs-table-body tr").forEach((r) => r.classList.remove("row-active"));
          row.classList.add("row-active");
        }
      });
    });
  }

  // Runs Filters
  elements.filterPills.forEach((pill) => {
    pill.addEventListener("click", () => {
      elements.filterPills.forEach((p) => p.classList.remove("active"));
      pill.classList.add("active");
      state.statusFilter = pill.dataset.filter;
      renderRunsTable();
    });
  });

  // Runs Search Input
  if (elements.runsSearchInput) {
    elements.runsSearchInput.addEventListener("input", (e) => {
      state.searchQuery = e.target.value.trim();
      elements.runsSearchClear.style.display = state.searchQuery ? "block" : "none";
      renderRunsTable();
    });
  }

  if (elements.runsSearchClear) {
    elements.runsSearchClear.addEventListener("click", () => {
      state.searchQuery = "";
      elements.runsSearchInput.value = "";
      elements.runsSearchClear.style.display = "none";
      renderRunsTable();
    });
  }

  // ==========================================
  // GLOBAL COMMAND PALETTE (CMD+K / CTRL+K)
  // ==========================================
  function openCommandPalette() {
    state.cmdPaletteOpen = true;
    state.cmdSelectedIndex = 0;
    elements.commandPaletteModal.classList.add("open");
    elements.commandPaletteModal.setAttribute("aria-hidden", "false");
    elements.cmdPaletteInput.value = "";
    elements.cmdPaletteInput.focus();
    renderCommandPaletteResults();
  }

  function closeCommandPalette() {
    state.cmdPaletteOpen = false;
    elements.commandPaletteModal.classList.remove("open");
    elements.commandPaletteModal.setAttribute("aria-hidden", "true");
  }

  function getCommandPaletteActions() {
    const actions = [
      // Navigation
      { title: "Go to Workflows & DAGs", category: "Navigation", badge: "View", onSelect: () => switchTab("workflows") },
      { title: "Go to Executions & History", category: "Navigation", badge: "View", onSelect: () => switchTab("runs") },
      { title: "Go to Live SSE Terminal", category: "Navigation", badge: "View", onSelect: () => switchTab("terminal") },
      { title: "Go to DAG Topology Visualizer", category: "Navigation", badge: "View", onSelect: () => switchTab("dag") },
      { title: "Go to Daemon Settings & Telemetry", category: "Navigation", badge: "View", onSelect: () => switchTab("settings") },

      // Terminal Actions
      { title: "Toggle Relative / Local Timestamps", category: "Live Terminal", badge: "Terminal", onSelect: () => {
        state.terminalShowUTC = !state.terminalShowUTC;
        if (elements.terminalToggleTimestamps) {
          elements.terminalToggleTimestamps.querySelector("span").textContent = state.terminalShowUTC ? "Local Time" : "Rel Time";
          elements.terminalToggleTimestamps.classList.toggle("active", state.terminalShowUTC);
        }
        rerenderFilteredTerminal();
        showToast({ title: "Terminal", message: `Timestamps switched to ${state.terminalShowUTC ? "Local" : "Relative"}`, type: "info" });
      }},
      { title: "Toggle Word Wrap", category: "Live Terminal", badge: "Terminal", onSelect: () => {
        state.terminalWordWrap = !state.terminalWordWrap;
        elements.terminalLogs.classList.toggle("wrap-active", state.terminalWordWrap);
        showToast({ title: "Terminal", message: `Word wrap ${state.terminalWordWrap ? "enabled" : "disabled"}`, type: "info" });
      }},
      { title: "Clear Console Buffer", category: "Live Terminal", badge: "Terminal", onSelect: () => {
        state.rawLogLines = [];
        elements.terminalLogs.innerHTML = "";
      }},
      { title: "Fit DAG to Viewport", category: "DAG Visualizer", badge: "Canvas", onSelect: () => fitDAGToViewport() },
      { title: "Refresh Daemon State", category: "Daemon", badge: "Sync", onSelect: () => checkHealth() },
    ];

    // Add Dynamic Workflows
    state.workflows.forEach((wf) => {
      actions.push({
        title: `Run Workflow: ${wf.name || wf.id}`,
        category: "Workflows",
        badge: "Run",
        onSelect: () => triggerWorkflow(wf.id),
      });
      actions.push({
        title: `Inspect DAG: ${wf.name || wf.id}`,
        category: "Workflows",
        badge: "Inspect",
        onSelect: () => {
          selectWorkflow(wf.id);
          switchTab("workflows");
        },
      });
    });

    // Add Recent Runs
    state.runs.slice(0, 8).forEach((r) => {
      actions.push({
        title: `Tail Logs: #${r.id.substring(0, 10)} (${r.job_id})`,
        category: "Recent Runs",
        badge: r.status,
        onSelect: () => {
          switchTab("workflows");
          streamLogs(r.id);
        },
      });
    });

    return actions;
  }

  function renderCommandPaletteResults() {
    const query = elements.cmdPaletteInput.value.trim().toLowerCase();
    const allActions = getCommandPaletteActions();

    state.cmdFilteredItems = allActions.filter((a) => {
      if (!query) return true;
      return (
        a.title.toLowerCase().includes(query) ||
        a.category.toLowerCase().includes(query)
      );
    });

    if (state.cmdSelectedIndex >= state.cmdFilteredItems.length) {
      state.cmdSelectedIndex = Math.max(0, state.cmdFilteredItems.length - 1);
    }

    if (state.cmdFilteredItems.length === 0) {
      elements.cmdPaletteResults.innerHTML = `
        <div style="padding:1.5rem;text-align:center;color:var(--text-muted);font-size:0.85rem;">
          No matching commands or workflows found.
        </div>
      `;
      return;
    }

    // Group by category
    const categories = {};
    state.cmdFilteredItems.forEach((item, index) => {
      if (!categories[item.category]) categories[item.category] = [];
      categories[item.category].push({ item, index });
    });

    let html = "";
    Object.keys(categories).forEach((cat) => {
      html += `<div class="cmd-group-label">${escapeHTML(cat)}</div>`;
      categories[cat].forEach(({ item, index }) => {
        const isSel = index === state.cmdSelectedIndex;
        html += `
          <div class="cmd-item ${isSel ? "selected" : ""}" data-idx="${index}">
            <div class="cmd-item-left">
              <svg class="icon" viewBox="0 0 24 24" width="14" height="14" style="color:var(--accent-cyan);">
                <polyline points="9 18 15 12 9 6"></polyline>
              </svg>
              <span>${escapeHTML(item.title)}</span>
            </div>
            <span class="cmd-item-badge">${escapeHTML(item.badge)}</span>
          </div>
        `;
      });
    });

    elements.cmdPaletteResults.innerHTML = html;

    elements.cmdPaletteResults.querySelectorAll(".cmd-item").forEach((el) => {
      el.addEventListener("click", () => {
        const idx = parseInt(el.dataset.idx, 10);
        executeCommand(idx);
      });
    });
  }

  function executeCommand(index) {
    const action = state.cmdFilteredItems[index];
    if (action && typeof action.onSelect === "function") {
      closeCommandPalette();
      action.onSelect();
    }
  }

  // Keyboard Navigation for Command Palette
  window.addEventListener("keydown", (e) => {
    // Cmd+K or Ctrl+K trigger
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
      e.preventDefault();
      if (state.cmdPaletteOpen) {
        closeCommandPalette();
      } else {
        openCommandPalette();
      }
      return;
    }

    if (!state.cmdPaletteOpen) return;

    if (e.key === "Escape") {
      e.preventDefault();
      closeCommandPalette();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      if (state.cmdSelectedIndex < state.cmdFilteredItems.length - 1) {
        state.cmdSelectedIndex++;
        renderCommandPaletteResults();
      }
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (state.cmdSelectedIndex > 0) {
        state.cmdSelectedIndex--;
        renderCommandPaletteResults();
      }
    } else if (e.key === "Enter") {
      e.preventDefault();
      executeCommand(state.cmdSelectedIndex);
    }
  });

  if (elements.cmdPaletteInput) {
    elements.cmdPaletteInput.addEventListener("input", () => {
      state.cmdSelectedIndex = 0;
      renderCommandPaletteResults();
    });
  }

  if (elements.headerSearchBtn) {
    elements.headerSearchBtn.addEventListener("click", () => openCommandPalette());
  }

  if (elements.commandPaletteModal) {
    elements.commandPaletteModal.addEventListener("click", (e) => {
      if (e.target === elements.commandPaletteModal) {
        closeCommandPalette();
      }
    });
  }

  // ==========================================
  // UTILITIES
  // ==========================================
  function escapeHTML(str) {
    if (!str) return "";
    return String(str).replace(/[&<>'"]/g, (tag) => {
      const map = {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        "'": "&#39;",
        '"': "&quot;",
      };
      return map[tag] || tag;
    });
  }

  function escapeRegExp(string) {
    return string.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function formatBytes(bytes) {
    if (!bytes || bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
  }

  // ==========================================
  // INITIALIZATION & BACKGROUND POLLING
  // ==========================================
  checkHealth();
  loadWorkflows();
  loadRuns();

  setInterval(() => {
    checkHealth();
    if (state.activeTab === "workflows" && !state.inspectingRunId) {
      loadWorkflows();
    } else if (state.activeTab === "runs") {
      loadRuns();
    }
  }, 5000);
});

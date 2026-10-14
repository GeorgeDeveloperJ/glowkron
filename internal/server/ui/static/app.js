// GlowKron Single-Page Application (Vanilla JS, Zero-CDN)
document.addEventListener("DOMContentLoaded", () => {
  "use strict";

  // Application State
  const state = {
    activeTab: "workflows",
    workflows: [],
    runs: [],
    runStepCache: {}, // runId -> steps array
    selectedWorkflow: null, // Currently selected workflow object
    inspectingRunId: null, // If set, DAG reflects this specific run's execution
    inspectingRun: null, // Full run object for inspected run
    activeStreamRunId: null, // Run ID currently hooked to SSE
    eventSource: null,
    reconnectAttempts: 0,
    maxReconnectAttempts: 5,
    reconnectTimer: null,
    stepPollTimer: null,
    autoScroll: true,
    userScrolledUp: false,
    statusFilter: "all",
    searchQuery: "",
    lastLogBytes: 0,
    logLineCount: 0,
  };

  // DOM Elements
  const elements = {
    // Nav & Shell
    sidebar: document.getElementById("sidebar"),
    sidebarToggleBtn: document.getElementById("sidebar-toggle-btn"),
    navItems: document.querySelectorAll(".nav-item"),
    viewSections: document.querySelectorAll(".view-section"),
    pageContextTitle: document.getElementById("page-context-title"),
    globalRefreshBtn: document.getElementById("btn-global-refresh"),
    daemonStatusBadge: document.getElementById("daemon-status-badge"),
    daemonBadgeLabel: document.getElementById("daemon-badge-label"),
    sidebarStatusDot: document.getElementById("sidebar-status-dot"),
    sidebarDaemonText: document.getElementById("sidebar-daemon-text"),
    sidebarWorkflowsDir: document.getElementById("sidebar-workflows-dir"),
    sidebarServerTime: document.getElementById("sidebar-server-time"),
    badgeWorkflowsCount: document.getElementById("badge-workflows-count"),
    badgeRunningCount: document.getElementById("badge-running-count"),
    terminalLiveIndicator: document.getElementById("terminal-live-indicator"),
    headerActiveRun: document.getElementById("header-active-run"),
    headerActiveRunText: document.getElementById("header-active-run-text"),
    headerActiveRunBtn: document.getElementById("header-active-run-btn"),
    toastContainer: document.getElementById("toast-container"),

    // Workflows View
    workflowCountBadge: document.getElementById("workflow-count-badge"),
    workflowTableBody: document.getElementById("workflow-table-body"),
    dagTitle: document.getElementById("dag-title"),
    dagSubtitle: document.getElementById("dag-subtitle"),
    dagRunBadge: document.getElementById("dag-run-badge"),
    dagResetTemplateBtn: document.getElementById("dag-reset-template-btn"),
    dagRefreshBtn: document.getElementById("dag-refresh-btn"),
    dagContainer: document.getElementById("dag-container"),

    // Runs View
    statTotalRuns: document.getElementById("stat-total-runs"),
    statRunningRuns: document.getElementById("stat-running-runs"),
    statCompletedRuns: document.getElementById("stat-completed-runs"),
    statFailedRuns: document.getElementById("stat-failed-runs"),
    filterPills: document.querySelectorAll(".filter-pill"),
    runsSearchInput: document.getElementById("runs-search-input"),
    runsSearchClear: document.getElementById("runs-search-clear"),
    runsTableBody: document.getElementById("runs-table-body"),

    // Terminal View
    terminalRunId: document.getElementById("terminal-run-id"),
    terminalConnStatus: document.getElementById("terminal-conn-status"),
    inspectDagBtn: document.getElementById("inspect-dag-btn"),
    copyTerminalBtn: document.getElementById("copy-terminal-btn"),
    clearTerminalBtn: document.getElementById("clear-terminal-btn"),
    autoScrollToggle: document.getElementById("auto-scroll-toggle"),
    autoScrollLabel: document.getElementById("auto-scroll-label"),
    terminalLogs: document.getElementById("terminal-logs"),
    terminalMetaLines: document.getElementById("terminal-meta-lines"),
    terminalMetaBytes: document.getElementById("terminal-meta-bytes"),
    terminalMetaStreamStatus: document.getElementById("terminal-meta-stream-status"),
    terminalScrollHint: document.getElementById("terminal-scroll-hint"),
  };

  // ==========================================
  // NAVIGATION & TAB SWITCHING
  // ==========================================

  function switchTab(tabId) {
    state.activeTab = tabId;

    // Update active nav button
    elements.navItems.forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === tabId);
    });

    // Update active view section
    elements.viewSections.forEach((section) => {
      section.classList.toggle("active", section.id === `view-${tabId}`);
    });

    // Update Topbar Breadcrumb Context
    const titles = {
      workflows: "Workflows & DAGs",
      runs: "Execution Runs",
      terminal: "Live Terminal Console",
    };
    elements.pageContextTitle.textContent = titles[tabId] || "Dashboard";

    // Close mobile drawer if opened
    if (elements.sidebar.classList.contains("open")) {
      elements.sidebar.classList.remove("open");
    }

    // Refresh context if needed
    if (tabId === "runs") {
      loadRuns();
    } else if (tabId === "workflows") {
      loadWorkflows();
    }
  }

  elements.navItems.forEach((btn) => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  // Mobile sidebar toggle
  elements.sidebarToggleBtn.addEventListener("click", () => {
    elements.sidebar.classList.toggle("open");
  });

  // Global refresh button
  elements.globalRefreshBtn.addEventListener("click", async () => {
    elements.globalRefreshBtn.disabled = true;
    const originalText = elements.globalRefreshBtn.innerHTML;
    elements.globalRefreshBtn.innerHTML = `<span>Refreshing...</span>`;
    await Promise.all([checkHealth(), loadWorkflows(), loadRuns()]);
    if (state.inspectingRunId) {
      await refreshInspectedRunSteps(state.inspectingRunId);
    }
    setTimeout(() => {
      elements.globalRefreshBtn.disabled = false;
      elements.globalRefreshBtn.innerHTML = originalText;
    }, 400);
  });

  // Header active run quick view jump
  elements.headerActiveRunBtn.addEventListener("click", () => {
    if (state.activeStreamRunId) {
      switchTab("terminal");
    }
  });

  // ==========================================
  // TOAST NOTIFICATIONS
  // ==========================================

  function showToast({ title, message, type = "info", duration = 4500, actions = [] }) {
    const toast = document.createElement("div");
    toast.className = `toast toast-${type}`;

    let actionsHtml = "";
    if (actions && actions.length > 0) {
      actionsHtml = `
        <div class="toast-actions">
          ${actions
            .map(
              (act, i) =>
                `<button class="btn btn-secondary btn-sm toast-act-btn" data-action-index="${i}">${escapeHTML(act.label)}</button>`
            )
            .join("")}
        </div>
      `;
    }

    toast.innerHTML = `
      <div class="toast-header">
        <span class="toast-title">
          <span class="status-indicator-dot"></span>
          ${escapeHTML(title)}
        </span>
        <button class="toast-close" aria-label="Close notification">&times;</button>
      </div>
      <div class="toast-msg">${escapeHTML(message)}</div>
      ${actionsHtml}
      <div class="toast-progress" style="animation-duration: ${duration}ms;"></div>
    `;

    // Handle actions
    toast.querySelectorAll(".toast-act-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.actionIndex, 10);
        if (actions[idx] && typeof actions[idx].onClick === "function") {
          actions[idx].onClick();
        }
        removeToast(toast);
      });
    });

    // Close button
    toast.querySelector(".toast-close").addEventListener("click", () => removeToast(toast));

    elements.toastContainer.appendChild(toast);

    const timer = setTimeout(() => {
      removeToast(toast);
    }, duration);

    function removeToast(el) {
      clearTimeout(timer);
      el.style.opacity = "0";
      el.style.transform = "translateX(50px)";
      setTimeout(() => el.remove(), 250);
    }
  }

  // ==========================================
  // HEALTH & SYSTEM METRICS
  // ==========================================

  async function checkHealth() {
    try {
      const res = await fetch("/api/health");
      if (res.ok) {
        const data = await res.json();
        elements.daemonStatusBadge.className = "badge badge-online";
        elements.daemonBadgeLabel.textContent = `Online • ${data.version || "v0.1.0"}`;
        elements.sidebarStatusDot.className = "status-dot-pulse";
        elements.sidebarDaemonText.textContent = "Daemon Online";

        if (data.workflows_dir) {
          elements.sidebarWorkflowsDir.textContent = data.workflows_dir;
          elements.sidebarWorkflowsDir.title = data.workflows_dir;
        }

        if (data.time) {
          const dt = new Date(data.time);
          elements.sidebarServerTime.textContent = dt.toLocaleTimeString();
        }
      } else {
        markOffline();
      }
    } catch {
      markOffline();
    }
  }

  function markOffline() {
    elements.daemonStatusBadge.className = "badge badge-offline";
    elements.daemonBadgeLabel.textContent = "Daemon Offline";
    elements.sidebarStatusDot.className = "status-dot-pulse offline";
    elements.sidebarDaemonText.textContent = "Daemon Offline";
  }

  // ==========================================
  // WORKFLOWS MANAGEMENT & VISUAL DAG
  // ==========================================

  async function loadWorkflows() {
    try {
      const res = await fetch("/api/jobs");
      if (res.ok) {
        state.workflows = await res.json();
        renderWorkflows();

        // Update count badges
        const count = state.workflows ? state.workflows.length : 0;
        elements.badgeWorkflowsCount.textContent = count;
        elements.workflowCountBadge.textContent = `${count} workflow${count === 1 ? "" : "s"}`;

        // Select first workflow if none selected yet
        if (state.workflows.length > 0 && !state.selectedWorkflow && !state.inspectingRunId) {
          selectWorkflow(state.workflows[0].id || state.workflows[0].name);
        }
      }
    } catch (err) {
      console.error("Failed to fetch workflows:", err);
    }
  }

  function renderWorkflows() {
    if (!state.workflows || state.workflows.length === 0) {
      elements.workflowTableBody.innerHTML = `
        <tr>
          <td colspan="6" class="table-loading">
            No workflows registered yet. Place workflow YAML files into your workflows directory.
          </td>
        </tr>
      `;
      return;
    }

    elements.workflowTableBody.innerHTML = state.workflows
      .map((wf) => {
        const isSelected =
          state.selectedWorkflow &&
          (state.selectedWorkflow.id === wf.id || state.selectedWorkflow.name === wf.name);
        const rowClass = isSelected ? "row-active" : "";

        return `
        <tr class="${rowClass}">
          <td>
            <strong>${escapeHTML(wf.name)}</strong>
            <div style="font-family:var(--font-mono);font-size:0.75rem;color:var(--text-muted);">${escapeHTML(wf.id)}</div>
          </td>
          <td>
            <span class="badge ${wf.enabled ? "badge-completed" : "badge-offline"}">
              ${wf.enabled ? "ENABLED" : "DISABLED"}
            </span>
          </td>
          <td>
            <code>${escapeHTML(wf.cron_expr || "(manual)")}</code>
          </td>
          <td>${wf.timeout_sec ? wf.timeout_sec + "s" : "-"}</td>
          <td>${escapeHTML(wf.description || "-")}</td>
          <td class="text-right">
            <button class="btn btn-primary btn-sm btn-trigger-workflow" data-id="${escapeHTML(wf.id || wf.name)}" data-name="${escapeHTML(wf.name)}">
              <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2">
                <polygon points="5 3 19 12 5 21 5 3"></polygon>
              </svg>
              Run
            </button>
            <button class="btn btn-secondary btn-sm btn-inspect-workflow" data-id="${escapeHTML(wf.id || wf.name)}">
              View DAG
            </button>
          </td>
        </tr>
      `;
      })
      .join("");

    // Bind Run buttons
    elements.workflowTableBody.querySelectorAll(".btn-trigger-workflow").forEach((btn) => {
      btn.addEventListener("click", () => triggerWorkflow(btn.dataset.id, btn.dataset.name, btn));
    });

    // Bind View DAG buttons
    elements.workflowTableBody.querySelectorAll(".btn-inspect-workflow").forEach((btn) => {
      btn.addEventListener("click", () => {
        state.inspectingRunId = null;
        state.inspectingRun = null;
        selectWorkflow(btn.dataset.id);
        const card = document.getElementById("dag-viewer-card");
        if (card) card.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    });
  }

  // Trigger ad-hoc workflow run
  async function triggerWorkflow(workflowId, workflowName, triggerBtn) {
    if (triggerBtn) {
      triggerBtn.disabled = true;
      triggerBtn.innerHTML = `<span>Dispatching...</span>`;
    }

    try {
      const res = await fetch(`/api/jobs/${encodeURIComponent(workflowId)}/run`, {
        method: "POST",
      });

      if (res.ok) {
        const runRecord = await res.json();
        const runId = runRecord.id;

        showToast({
          title: `Run Dispatched: ${workflowName || workflowId}`,
          message: `Execution Run ID: ${runId.substring(0, 16)}...`,
          type: "success",
          duration: 6000,
          actions: [
            {
              label: "Live Terminal",
              onClick: () => {
                switchTab("terminal");
                streamLogs(runId);
              },
            },
            {
              label: "Inspect DAG",
              onClick: () => {
                switchTab("workflows");
                inspectRunDAG(runId);
              },
            },
          ],
        });

        // Auto-navigate to Live Terminal and start streaming
        switchTab("terminal");
        streamLogs(runId);

        // Also prepare DAG in run mode
        inspectRunDAG(runId);

        // Refresh runs
        loadRuns();
      } else {
        const errText = await res.text();
        showToast({
          title: "Trigger Failed",
          message: `Server returned HTTP ${res.status}: ${errText}`,
          type: "error",
        });
      }
    } catch (err) {
      showToast({
        title: "Network Error",
        message: `Failed to dispatch run: ${err.message}`,
        type: "error",
      });
    } finally {
      if (triggerBtn) {
        triggerBtn.disabled = false;
        triggerBtn.innerHTML = `
          <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2">
            <polygon points="5 3 19 12 5 21 5 3"></polygon>
          </svg>
          Run
        `;
      }
    }
  }

  // Select workflow baseline template
  async function selectWorkflow(id) {
    try {
      const res = await fetch(`/api/jobs/${encodeURIComponent(id)}`);
      if (res.ok) {
        state.selectedWorkflow = await res.json();
        state.inspectingRunId = null;
        state.inspectingRun = null;
        renderWorkflows();
        renderDAG();
      }
    } catch (err) {
      console.error("Failed to fetch workflow detail:", err);
    }
  }

  // Inspect a specific run in the DAG viewer
  async function inspectRunDAG(runId) {
    state.inspectingRunId = runId;

    try {
      // 1. Fetch Run details
      const runRes = await fetch(`/api/runs/${encodeURIComponent(runId)}`);
      if (runRes.ok) {
        state.inspectingRun = await runRes.json();
      }

      // 2. Fetch associated workflow if not already selected
      if (
        state.inspectingRun &&
        (!state.selectedWorkflow || state.selectedWorkflow.id !== state.inspectingRun.job_id)
      ) {
        const wfRes = await fetch(`/api/jobs/${encodeURIComponent(state.inspectingRun.job_id)}`);
        if (wfRes.ok) {
          state.selectedWorkflow = await wfRes.json();
        }
      }

      // 3. Fetch step runs
      await refreshInspectedRunSteps(runId);

      // Render DAG with real-time status overlay
      renderDAG();
    } catch (err) {
      console.error("Failed to inspect run DAG:", err);
    }
  }

  // Refresh step runs for a specific run without full page reload
  async function refreshInspectedRunSteps(runId) {
    try {
      const stepRes = await fetch(`/api/runs/${encodeURIComponent(runId)}/steps`);
      if (stepRes.ok) {
        const steps = await stepRes.json();
        state.runStepCache[runId] = steps;

        // If currently viewing this run in DAG, update DOM nodes in place or re-render
        if (state.inspectingRunId === runId) {
          updateDAGNodesInPlace(steps);
        }
      }
    } catch (err) {
      console.error("Failed to fetch step runs:", err);
    }
  }

  // Update DAG node statuses without resetting scroll position or full re-render
  function updateDAGNodesInPlace(stepRuns) {
    if (!stepRuns || stepRuns.length === 0) return;

    stepRuns.forEach((sr) => {
      const nodeEl = document.querySelector(`.dag-node[data-step-id="${sr.step_id}"]`);
      if (!nodeEl) return;

      const currentStatus = sr.status || "PENDING";
      const statusClass = `status-${currentStatus.toLowerCase()}`;

      // Update node classes
      nodeEl.className = `dag-node ${statusClass}`;

      // Update badge
      const badgeEl = nodeEl.querySelector(".dag-node-status-badge");
      if (badgeEl) {
        badgeEl.className = `badge badge-${currentStatus.toLowerCase()} dag-node-status-badge`;
        badgeEl.textContent = currentStatus;
      }

      // Update duration/exit meta
      const metaEl = nodeEl.querySelector(".dag-node-runtime-meta");
      if (metaEl) {
        let metaText = "";
        if (sr.end_time && sr.start_time) {
          const dur = (new Date(sr.end_time) - new Date(sr.start_time)) / 1000;
          metaText = `${dur.toFixed(2)}s`;
        } else if (currentStatus === "RUNNING") {
          metaText = "running...";
        }
        if (sr.exit_code !== undefined && sr.exit_code !== null) {
          metaText += (metaText ? " • " : "") + `exit ${sr.exit_code}`;
        }
        metaEl.textContent = metaText;
      }
    });
  }

  // Render Visual DAG Viewer
  function renderDAG() {
    if (!state.selectedWorkflow) {
      elements.dagTitle.textContent = "Visual DAG Viewer";
      elements.dagSubtitle.textContent = "Select a workflow above to inspect its execution graph";
      elements.dagRunBadge.style.display = "none";
      elements.dagResetTemplateBtn.style.display = "none";
      elements.dagContainer.innerHTML = `
        <div class="dag-empty-state">
          <svg viewBox="0 0 24 24" width="48" height="48" fill="none" stroke="var(--text-muted)" stroke-width="1.5">
            <rect x="3" y="3" width="7" height="7"></rect>
            <rect x="14" y="3" width="7" height="7"></rect>
            <rect x="14" y="14" width="7" height="7"></rect>
            <rect x="3" y="14" width="7" height="7"></rect>
          </svg>
          <p>Select a workflow above or inspect a run to view its execution graph.</p>
        </div>
      `;
      return;
    }

    const steps = state.selectedWorkflow.steps || [];

    // Header adjustments based on template vs run inspection
    if (state.inspectingRunId) {
      elements.dagTitle.textContent = `Visual DAG: ${state.selectedWorkflow.name}`;
      elements.dagSubtitle.textContent = `Showing execution states for Run: ${state.inspectingRunId}`;
      elements.dagRunBadge.style.display = "inline-flex";

      const runStatus = state.inspectingRun ? state.inspectingRun.status : "RUNNING";
      elements.dagRunBadge.className = `badge badge-${runStatus.toLowerCase()}`;
      elements.dagRunBadge.textContent = runStatus;

      elements.dagResetTemplateBtn.style.display = "inline-flex";
    } else {
      elements.dagTitle.textContent = `Visual DAG: ${state.selectedWorkflow.name}`;
      elements.dagSubtitle.textContent = state.selectedWorkflow.description || "Baseline Workflow Template";
      elements.dagRunBadge.style.display = "none";
      elements.dagResetTemplateBtn.style.display = "none";
    }

    if (steps.length === 0) {
      elements.dagContainer.innerHTML = `
        <div class="dag-empty-state">
          <p>No steps defined in workflow "${escapeHTML(state.selectedWorkflow.name)}".</p>
        </div>
      `;
      return;
    }

    // Step run map if inspecting run
    const stepRunMap = {};
    if (state.inspectingRunId && state.runStepCache[state.inspectingRunId]) {
      state.runStepCache[state.inspectingRunId].forEach((sr) => {
        stepRunMap[sr.step_id] = sr;
      });
    }

    // Compute topological dependency stages
    const stages = computeTopologicalStages(steps);

    elements.dagContainer.innerHTML = stages
      .map((stage, stageIndex) => {
        return `
        <div class="dag-stage" data-stage="${stageIndex + 1}">
          <div class="dag-stage-header">
            <span class="dag-stage-title">Stage ${stageIndex + 1}</span>
            <span class="dag-stage-count">${stage.length} step${stage.length === 1 ? "" : "s"}</span>
          </div>

          <div class="dag-node-list">
            ${stage
              .map((step) => {
                const stepRun = stepRunMap[step.id];
                const status = stepRun ? stepRun.status : "PENDING";
                const statusClass = `status-${status.toLowerCase()}`;
                const badgeClass = `badge-${status.toLowerCase()}`;

                let runtimeMeta = "";
                if (stepRun) {
                  if (stepRun.end_time && stepRun.start_time) {
                    const dur = (new Date(stepRun.end_time) - new Date(stepRun.start_time)) / 1000;
                    runtimeMeta = `${dur.toFixed(2)}s`;
                  } else if (status === "RUNNING") {
                    runtimeMeta = "running...";
                  }
                  if (stepRun.exit_code !== undefined && stepRun.exit_code !== null) {
                    runtimeMeta += (runtimeMeta ? " • " : "") + `exit ${stepRun.exit_code}`;
                  }
                }

                const argsStr = (step.args || []).join(" ");
                const cmdDisplay = `${step.command || ""} ${argsStr}`.trim();

                const dependsOn = step.depends_on || [];
                const depsHtml =
                  dependsOn.length > 0
                    ? `
                    <div class="dag-node-deps">
                      <span style="font-size:0.68rem;color:var(--text-muted);">deps:</span>
                      ${dependsOn
                        .map((dep) => `<span class="dag-dep-tag">${escapeHTML(dep)}</span>`)
                        .join("")}
                    </div>
                  `
                    : "";

                return `
                  <div class="dag-node ${statusClass}" data-step-id="${escapeHTML(step.id)}">
                    <div class="dag-node-header">
                      <span class="dag-node-id">${escapeHTML(step.id)}</span>
                      <span class="badge ${badgeClass} dag-node-status-badge">${status}</span>
                    </div>

                    ${step.name && step.name !== step.id ? `<div style="font-size:0.75rem;color:var(--text-secondary);margin-bottom:0.35rem;">${escapeHTML(step.name)}</div>` : ""}

                    <div class="dag-node-cmd" title="${escapeHTML(cmdDisplay)}">${escapeHTML(cmdDisplay || "(no command)")}</div>

                    ${depsHtml}

                    <div class="dag-node-meta">
                      <span class="dag-node-runtime-meta">${escapeHTML(runtimeMeta)}</span>
                      ${step.timeout_sec ? `<span>timeout ${step.timeout_sec}s</span>` : ""}
                    </div>
                  </div>
                `;
              })
              .join("")}
          </div>
        </div>
      `;
      })
      .join("");

    // Click node to view step details
    elements.dagContainer.querySelectorAll(".dag-node").forEach((node) => {
      node.addEventListener("click", () => {
        const stepId = node.dataset.stepId;
        const stepRun = stepRunMap[stepId];
        if (stepRun && (stepRun.stdout_snippet || stepRun.stderr_snippet)) {
          showToast({
            title: `Step: ${stepId} [${stepRun.status}]`,
            message: stepRun.stderr_snippet
              ? `Stderr: ${stepRun.stderr_snippet}`
              : `Stdout: ${stepRun.stdout_snippet}`,
            type: stepRun.status === "FAILED" ? "error" : "info",
            duration: 6000,
          });
        }
      });
    });
  }

  // Reset DAG view to workflow template
  elements.dagResetTemplateBtn.addEventListener("click", () => {
    state.inspectingRunId = null;
    state.inspectingRun = null;
    renderDAG();
  });

  // DAG Refresh Button
  elements.dagRefreshBtn.addEventListener("click", async () => {
    if (state.inspectingRunId) {
      await refreshInspectedRunSteps(state.inspectingRunId);
    } else if (state.selectedWorkflow) {
      await selectWorkflow(state.selectedWorkflow.id || state.selectedWorkflow.name);
    }
  });

  // Topological DAG Partitioning into parallel stages
  function computeTopologicalStages(steps) {
    if (!steps || steps.length === 0) return [];

    const inDegree = {};
    const stepMap = {};
    const dependents = {};

    steps.forEach((s) => {
      stepMap[s.id] = s;
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

    // Wave 0: Root steps with zero dependencies
    let currentWave = steps.filter((s) => inDegree[s.id] === 0);

    // Fallback if all nodes have dependencies (circular or misconfigured)
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

      // Avoid infinite loop on cycles
      if (nextWave.length === 0 && visited.size < steps.length) {
        const remaining = steps.filter((s) => !visited.has(s.id));
        stages.push(remaining);
        break;
      }

      currentWave = nextWave;
    }

    return stages;
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
        renderRuns();
      }
    } catch (err) {
      console.error("Failed to fetch runs:", err);
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

    // Sidebar active badge
    if (running > 0) {
      elements.badgeRunningCount.style.display = "inline-block";
      elements.badgeRunningCount.textContent = running;
    } else {
      elements.badgeRunningCount.style.display = "none";
    }
  }

  function renderRuns() {
    const runs = state.runs || [];

    // Filter by status & search
    const filtered = runs.filter((r) => {
      const statusMatch =
        state.statusFilter === "all" ||
        (r.status || "").toLowerCase() === state.statusFilter.toLowerCase();

      if (!statusMatch) return false;

      if (!state.searchQuery) return true;
      const q = state.searchQuery.toLowerCase();
      const idMatch = (r.id || "").toLowerCase().includes(q);
      const jobMatch = (r.job_id || "").toLowerCase().includes(q);
      const triggerMatch = (r.triggered_by || "").toLowerCase().includes(q);

      return idMatch || jobMatch || triggerMatch;
    });

    if (filtered.length === 0) {
      elements.runsTableBody.innerHTML = `
        <tr>
          <td colspan="7" class="table-loading">
            No execution runs found matching your filter criteria.
          </td>
        </tr>
      `;
      return;
    }

    elements.runsTableBody.innerHTML = filtered
      .map((r) => {
        const isStreaming = state.activeStreamRunId === r.id;
        const isInspected = state.inspectingRunId === r.id;
        const rowClass = isInspected || isStreaming ? "row-active" : "";

        // Status badge
        const status = r.status || "PENDING";
        const badgeClass = `badge-${status.toLowerCase()}`;

        // Triggered by badge
        const trigger = r.triggered_by || "MANUAL";

        // Duration calculation
        let duration = "-";
        if (r.end_time && r.start_time) {
          const start = new Date(r.start_time).getTime();
          const end = new Date(r.end_time).getTime();
          const sec = ((end - start) / 1000).toFixed(2);
          duration = `${sec}s`;
        } else if (status === "RUNNING" && r.start_time) {
          const start = new Date(r.start_time).getTime();
          const elapsed = ((Date.now() - start) / 1000).toFixed(1);
          duration = `${elapsed}s (live)`;
        }

        const formattedStart = r.start_time
          ? new Date(r.start_time).toLocaleString()
          : "-";

        return `
        <tr class="${rowClass}" data-run-id="${escapeHTML(r.id)}">
          <td>
            <div style="display:flex;align-items:center;gap:0.4rem;">
              <code style="color:var(--primary);cursor:pointer;" class="btn-copy-id" data-copy="${escapeHTML(r.id)}" title="Click to copy full ID">
                ${escapeHTML(r.id.substring(0, 16))}...
              </code>
            </div>
          </td>
          <td><strong>${escapeHTML(r.job_id)}</strong></td>
          <td><span class="badge ${badgeClass}">${status}</span></td>
          <td><span class="badge badge-muted">${escapeHTML(trigger)}</span></td>
          <td style="white-space:nowrap;font-size:0.82rem;">${escapeHTML(formattedStart)}</td>
          <td style="font-family:var(--font-mono);font-size:0.82rem;">${duration}</td>
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

    // Bind row clicks and actions
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
        const card = document.getElementById("dag-viewer-card");
        if (card) card.scrollIntoView({ behavior: "smooth" });
      });
    });

    elements.runsTableBody.querySelectorAll(".btn-stream-run").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const runId = btn.dataset.id;
        switchTab("terminal");
        streamLogs(runId);
      });
    });

    // Row click to inspect
    elements.runsTableBody.querySelectorAll("tr").forEach((row) => {
      row.addEventListener("click", () => {
        const runId = row.dataset.runId;
        if (runId) {
          inspectRunDAG(runId);
          elements.runsTableBody.querySelectorAll("tr").forEach((r) => r.classList.remove("row-active"));
          row.classList.add("row-active");
        }
      });
    });
  }

  // Runs filters
  elements.filterPills.forEach((pill) => {
    pill.addEventListener("click", () => {
      elements.filterPills.forEach((p) => p.classList.remove("active"));
      pill.classList.add("active");
      state.statusFilter = pill.dataset.filter;
      renderRuns();
    });
  });

  // Runs search
  elements.runsSearchInput.addEventListener("input", (e) => {
    state.searchQuery = e.target.value.trim();
    elements.runsSearchClear.style.display = state.searchQuery ? "block" : "none";
    renderRuns();
  });

  elements.runsSearchClear.addEventListener("click", () => {
    state.searchQuery = "";
    elements.runsSearchInput.value = "";
    elements.runsSearchClear.style.display = "none";
    renderRuns();
  });

  // ==========================================
  // LIVE SSE TERMINAL LOG STREAMER
  // ==========================================

  function streamLogs(runId) {
    // Teardown any existing stream
    closeSSEStream();

    state.activeStreamRunId = runId;
    state.reconnectAttempts = 0;
    state.lastLogBytes = 0;
    state.logLineCount = 0;
    state.userScrolledUp = false;

    // Update UI headers
    elements.terminalRunId.textContent = `Run: ${runId}`;
    elements.terminalConnStatus.className = "terminal-conn-badge badge-reconnecting";
    elements.terminalConnStatus.textContent = "Connecting...";
    elements.terminalLiveIndicator.style.display = "inline-block";

    // Header active run indicator
    elements.headerActiveRun.style.display = "flex";
    elements.headerActiveRunText.textContent = `Streaming: ${runId.substring(0, 14)}...`;

    // Clear and set connecting text
    elements.terminalLogs.innerHTML = `<span class="log-line log-system">[Connecting to live log stream for run ${escapeHTML(runId)}...]</span>\n`;

    // Start background step poller for DAG live reflection
    startStepPoll(runId);

    // Open EventSource
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
      elements.terminalConnStatus.className = "terminal-conn-badge badge-connected";
      elements.terminalConnStatus.textContent = "Live";
      elements.terminalMetaStreamStatus.textContent = "Stream: Connected";
      state.reconnectAttempts = 0;
    };

    es.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.chunk) {
          appendLogChunk(payload.chunk);
        } else {
          appendLogChunk(event.data + "\n");
        }
      } catch {
        appendLogChunk(event.data + "\n");
      }
    };

    es.addEventListener("done", () => {
      appendLogChunk("\n[Stream finished: execution concluded]\n");
      elements.terminalConnStatus.className = "terminal-conn-badge badge-idle";
      elements.terminalConnStatus.textContent = "Completed";
      elements.terminalMetaStreamStatus.textContent = "Stream: Completed";
      closeSSEStream(false);
      // Final step sync & runs refresh
      refreshInspectedRunSteps(runId);
      loadRuns();
    });

    es.onerror = () => {
      handleSSEError(runId);
    };
  }

  function handleSSEError(runId) {
    // If user closed or switched run, ignore
    if (state.activeStreamRunId !== runId) return;

    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }

    // Check run status from API to determine if completed or truly disconnected
    fetch(`/api/runs/${encodeURIComponent(runId)}`)
      .then((r) => r.json())
      .then((run) => {
        if (run && (run.status === "COMPLETED" || run.status === "FAILED")) {
          // Finished: fetch full persisted log
          fetch(`/api/runs/${encodeURIComponent(runId)}/logs`)
            .then((res) => (res.ok ? res.text() : ""))
            .then((fullText) => {
              if (fullText) {
                elements.terminalLogs.textContent = "";
                appendLogChunk(fullText);
              }
            })
            .catch(() => {});

          elements.terminalConnStatus.className = "terminal-conn-badge badge-idle";
          elements.terminalConnStatus.textContent = run.status;
          elements.terminalMetaStreamStatus.textContent = "Stream: Ended";
          stopStepPoll();
          refreshInspectedRunSteps(runId);
          loadRuns();
          return;
        }

        // Run is still running: attempt auto-reconnect with backoff
        if (state.reconnectAttempts < state.maxReconnectAttempts) {
          state.reconnectAttempts++;
          const delay = Math.min(1000 * Math.pow(1.8, state.reconnectAttempts), 8000);
          elements.terminalConnStatus.className = "terminal-conn-badge badge-reconnecting";
          elements.terminalConnStatus.textContent = `Reconnecting (${state.reconnectAttempts})...`;
          elements.terminalMetaStreamStatus.textContent = `Stream: Reconnecting in ${(delay / 1000).toFixed(1)}s`;

          appendLogChunk(`\n[Stream dropped. Auto-reconnecting attempt ${state.reconnectAttempts}/${state.maxReconnectAttempts}...]\n`);

          state.reconnectTimer = setTimeout(() => {
            if (state.activeStreamRunId === runId) {
              connectSSE(runId);
            }
          }, delay);
        } else {
          elements.terminalConnStatus.className = "terminal-conn-badge badge-idle";
          elements.terminalConnStatus.textContent = "Disconnected";
          elements.terminalMetaStreamStatus.textContent = "Stream: Reconnect failed";
          appendLogChunk("\n[Reconnection failed. Terminal stream closed.]\n");
          stopStepPoll();
        }
      })
      .catch(() => {
        // Fallback fallback fetch logs
        fetch(`/api/runs/${encodeURIComponent(runId)}/logs`)
          .then((r) => (r.ok ? r.text() : ""))
          .then((t) => {
            if (t) elements.terminalLogs.textContent = t;
          })
          .catch(() => {});
        elements.terminalConnStatus.className = "terminal-conn-badge badge-idle";
        elements.terminalConnStatus.textContent = "Offline";
        stopStepPoll();
      });
  }

  function appendLogChunk(rawText) {
    if (!rawText) return;

    state.lastLogBytes += rawText.length;
    const lines = rawText.split("\n");
    state.logLineCount += lines.length - 1;

    // Syntax-highlight lines
    const fragment = document.createDocumentFragment();

    lines.forEach((line, idx) => {
      if (idx === lines.length - 1 && line === "") return;

      const span = document.createElement("span");
      span.className = "log-line";

      const lineLower = line.toLowerCase();
      if (lineLower.includes("[error]") || lineLower.includes("failed") || lineLower.includes("err:")) {
        span.className += " log-error";
      } else if (lineLower.includes("[warn]") || lineLower.includes("warning")) {
        span.className += " log-warn";
      } else if (lineLower.includes("[info]")) {
        span.className += " log-info";
      } else if (lineLower.includes("completed successfully") || lineLower.includes("success") || lineLower.includes("ok")) {
        span.className += " log-success";
      } else if (line.startsWith("$") || lineLower.includes("step:") || lineLower.includes("dispatching")) {
        span.className += " log-step";
      }

      span.textContent = line + "\n";
      fragment.appendChild(span);
    });

    elements.terminalLogs.appendChild(fragment);

    // Update terminal status bar
    elements.terminalMetaLines.textContent = `${state.logLineCount} lines`;
    elements.terminalMetaBytes.textContent = formatBytes(state.lastLogBytes);

    // Auto-scroll logic with scroll lock detection
    if (state.autoScroll && !state.userScrolledUp) {
      elements.terminalLogs.scrollTop = elements.terminalLogs.scrollHeight;
    }
  }

  // Scroll event on terminal: detect manual scroll-up for scroll lock
  elements.terminalLogs.addEventListener("scroll", () => {
    const el = elements.terminalLogs;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;

    if (distanceFromBottom > 40) {
      // User scrolled up
      state.userScrolledUp = true;
      elements.terminalScrollHint.className = "scroll-status-badge locked";
      elements.terminalScrollHint.textContent = "Scroll Locked";
    } else {
      // User is at bottom
      state.userScrolledUp = false;
      elements.terminalScrollHint.className = "scroll-status-badge";
      elements.terminalScrollHint.textContent = state.autoScroll ? "Auto-scrolling" : "Scroll: Paused";
    }
  });

  // Auto-scroll button toggle
  elements.autoScrollToggle.addEventListener("click", () => {
    state.autoScroll = !state.autoScroll;
    state.userScrolledUp = false;

    elements.autoScrollToggle.classList.toggle("active", state.autoScroll);
    elements.autoScrollLabel.textContent = state.autoScroll ? "Auto-Scroll: ON" : "Auto-Scroll: OFF";

    if (state.autoScroll) {
      elements.terminalLogs.scrollTop = elements.terminalLogs.scrollHeight;
      elements.terminalScrollHint.className = "scroll-status-badge";
      elements.terminalScrollHint.textContent = "Auto-scrolling";
    } else {
      elements.terminalScrollHint.className = "scroll-status-badge locked";
      elements.terminalScrollHint.textContent = "Auto-Scroll OFF";
    }
  });

  // Clear terminal
  elements.clearTerminalBtn.addEventListener("click", () => {
    elements.terminalLogs.textContent = "";
    state.logLineCount = 0;
    state.lastLogBytes = 0;
    elements.terminalMetaLines.textContent = "0 lines";
    elements.terminalMetaBytes.textContent = "0 B";
  });

  // Copy terminal logs
  elements.copyTerminalBtn.addEventListener("click", () => {
    const text = elements.terminalLogs.innerText || elements.terminalLogs.textContent;
    if (!text) return;
    navigator.clipboard.writeText(text).then(() => {
      showToast({ title: "Copied", message: "Terminal logs copied to clipboard", type: "info" });
    });
  });

  // Jump from terminal to inspect DAG
  elements.inspectDagBtn.addEventListener("click", () => {
    if (state.activeStreamRunId) {
      inspectRunDAG(state.activeStreamRunId);
      switchTab("workflows");
      const card = document.getElementById("dag-viewer-card");
      if (card) card.scrollIntoView({ behavior: "smooth" });
    } else {
      showToast({ title: "No Run", message: "No active run currently selected", type: "info" });
    }
  });

  // Periodic poll of step runs during active SSE stream to update DAG in real time
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

  // Polling loop for background health & runs updates
  setInterval(() => {
    checkHealth();
    if (state.activeTab === "workflows" && !state.inspectingRunId) {
      loadWorkflows();
    } else if (state.activeTab === "runs") {
      loadRuns();
    }
  }, 5000);
});

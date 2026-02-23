import { FormEvent, useEffect, useMemo, useState } from "react";
import type { components } from "@microgrid/shared";

type LoginResponse = components["schemas"]["LoginResponse"];
type AlertRecord = components["schemas"]["Alert"];
type NotificationRecord = components["schemas"]["Notification"];
type NotificationAckResponse = components["schemas"]["NotificationAckResponse"];
type StreamEvent = components["schemas"]["StreamEvent"];
type AlertListResponse = components["schemas"]["AlertListResponse"];
type NotificationListResponse = components["schemas"]["NotificationListResponse"];
type ControlCommandRequest = components["schemas"]["ControlCommandRequest"];
type ControlCommandRecord = components["schemas"]["ControlCommand"];
type ControlCommandAccepted = components["schemas"]["ControlCommandAccepted"];
type ControlCommandResponse = components["schemas"]["ControlCommandResponse"];
type UserRole = components["schemas"]["LoginResponse"]["role"];

type StreamStatus = "idle" | "connecting" | "connected" | "error";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:4000";

interface SessionState {
  accessToken: string;
  role: UserRole;
  expiresIn: number;
}

interface ApiErrorBody {
  message?: unknown;
}

interface CommandActionTemplate {
  id: string;
  label: string;
  description: string;
  commandType: string;
  payload: ControlCommandRequest["payload"];
}

const commandActionTemplates: CommandActionTemplate[] = [
  {
    id: "set-mode-eco",
    label: "Set Mode: Eco",
    description: "Reduce output to energy-saving mode for lower-demand periods.",
    commandType: "set_mode",
    payload: {
      mode: "eco"
    }
  },
  {
    id: "set-mode-boost",
    label: "Set Mode: Boost",
    description: "Increase output mode to handle peak demand windows.",
    commandType: "set_mode",
    payload: {
      mode: "boost"
    }
  },
  {
    id: "set-power-limit-80",
    label: "Set Power Limit: 80 kW",
    description: "Cap device output at 80 kW for safe operating range.",
    commandType: "set_power_limit",
    payload: {
      maxPowerKw: 80
    }
  },
  {
    id: "restart-inverter",
    label: "Restart Inverter",
    description: "Issue a controlled inverter restart for recovery action.",
    commandType: "restart_inverter",
    payload: {
      reason: "operator_requested_recovery"
    }
  }
];

const defaultCommandActionId = commandActionTemplates[0]?.id ?? "";

function createIdempotencyKey() {
  return `cmd-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}

function readApiErrorMessage(value: unknown) {
  if (!value || typeof value !== "object") {
    return null;
  }

  const body = value as ApiErrorBody;
  return typeof body.message === "string" ? body.message : null;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isControlCommandRecord(value: unknown): value is ControlCommandRecord {
  if (!isObject(value)) {
    return false;
  }

  return (
    typeof value.id === "string" &&
    typeof value.deviceId === "string" &&
    typeof value.commandType === "string" &&
    isObject(value.payload) &&
    typeof value.status === "string" &&
    typeof value.idempotencyKey === "string" &&
    (value.requestedBy === null || typeof value.requestedBy === "string") &&
    typeof value.createdAt === "string" &&
    typeof value.updatedAt === "string"
  );
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, init);
  const rawBody = await response.text();
  const parsedBody = rawBody.length > 0 ? (JSON.parse(rawBody) as unknown) : null;

  if (!response.ok) {
    throw new Error(readApiErrorMessage(parsedBody) ?? `Request failed (${response.status})`);
  }

  return parsedBody as T;
}

function isNotificationRecord(value: unknown): value is NotificationRecord {
  if (!isObject(value)) {
    return false;
  }

  return (
    typeof value.id === "string" &&
    typeof value.type === "string" &&
    typeof value.message === "string" &&
    typeof value.isRead === "boolean" &&
    isObject(value.payload) &&
    typeof value.createdAt === "string"
  );
}

function parseSseEventFrame(frame: string): { eventType: string; data: string | null } | null {
  const lines = frame.split("\n");
  let eventType = "message";
  const dataLines: string[] = [];

  for (const line of lines) {
    if (line.startsWith(":")) {
      continue;
    }

    if (line.startsWith("event:")) {
      eventType = line.slice("event:".length).trim();
      continue;
    }

    if (line.startsWith("data:")) {
      dataLines.push(line.slice("data:".length).trimStart());
    }
  }

  if (dataLines.length === 0) {
    return null;
  }

  return {
    eventType,
    data: dataLines.join("\n")
  };
}

function connectStream(
  accessToken: string,
  onEvent: (event: StreamEvent) => void,
  onOpen: () => void,
  onError: (errorMessage: string) => void
) {
  const abortController = new AbortController();

  void (async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/v1/stream`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${accessToken}`
        },
        signal: abortController.signal
      });

      if (!response.ok || !response.body) {
        const rawBody = await response.text().catch(() => "");
        const parsed = rawBody ? (JSON.parse(rawBody) as unknown) : null;
        throw new Error(readApiErrorMessage(parsed) ?? `Stream failed (${response.status})`);
      }

      onOpen();

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }

        buffer += decoder.decode(value, { stream: true });

        let separatorIndex = buffer.indexOf("\n\n");
        while (separatorIndex >= 0) {
          const frame = buffer.slice(0, separatorIndex);
          buffer = buffer.slice(separatorIndex + 2);

          const parsedFrame = parseSseEventFrame(frame);
          if (parsedFrame?.data) {
            try {
              const parsedEvent = JSON.parse(parsedFrame.data) as StreamEvent;
              onEvent(parsedEvent);
            } catch {
              // Ignore malformed event payloads.
            }
          }

          separatorIndex = buffer.indexOf("\n\n");
        }
      }

      if (!abortController.signal.aborted) {
        onError("Stream connection closed");
      }
    } catch (error) {
      if (abortController.signal.aborted) {
        return;
      }

      onError(error instanceof Error ? error.message : "Stream unavailable");
    }
  })();

  return () => {
    abortController.abort();
  };
}

function upsertControlCommand(current: ControlCommandRecord[], nextCommand: ControlCommandRecord) {
  const replaced = current.map((command) => (command.id === nextCommand.id ? nextCommand : command));
  const hasExisting = current.some((command) => command.id === nextCommand.id);
  const merged = hasExisting ? replaced : [nextCommand, ...current];

  return merged.sort((a, b) => Date.parse(b.createdAt) - Date.parse(a.createdAt));
}

function extractControlCommandFromStream(data: unknown): ControlCommandRecord | null {
  if (!isObject(data)) {
    return null;
  }

  const nested = (data as { command?: unknown }).command;
  if (isControlCommandRecord(nested)) {
    return nested;
  }

  if (isControlCommandRecord(data)) {
    return data;
  }

  return null;
}

function isPendingStatus(status: ControlCommandRecord["status"]) {
  return status === "queued" || status === "processing";
}

function getCommandActionTemplateById(actionId: string) {
  return commandActionTemplates.find((template) => template.id === actionId) ?? null;
}

function formatLocalDateTime(iso: string) {
  const parsed = Date.parse(iso);
  if (Number.isNaN(parsed)) {
    return iso;
  }

  return new Date(parsed).toLocaleString();
}

export function App() {
  const [username, setUsername] = useState("viewer");
  const [password, setPassword] = useState("viewer123");
  const [loginError, setLoginError] = useState<string | null>(null);
  const [isLoggingIn, setIsLoggingIn] = useState(false);

  const [session, setSession] = useState<SessionState | null>(null);

  const [alerts, setAlerts] = useState<AlertRecord[]>([]);
  const [alertsLoading, setAlertsLoading] = useState(false);
  const [alertsError, setAlertsError] = useState<string | null>(null);

  const [notifications, setNotifications] = useState<NotificationRecord[]>([]);
  const [notificationsLoading, setNotificationsLoading] = useState(false);
  const [notificationsError, setNotificationsError] = useState<string | null>(null);

  const [commands, setCommands] = useState<ControlCommandRecord[]>([]);
  const [commandError, setCommandError] = useState<string | null>(null);
  const [commandSuccess, setCommandSuccess] = useState<string | null>(null);
  const [isSubmittingCommand, setIsSubmittingCommand] = useState(false);
  const [refreshingCommandIds, setRefreshingCommandIds] = useState<string[]>([]);

  const [commandDeviceId, setCommandDeviceId] = useState("device-001");
  const [selectedCommandActionId, setSelectedCommandActionId] = useState(defaultCommandActionId);
  const [commandIdempotencyKey, setCommandIdempotencyKey] = useState(createIdempotencyKey());

  const [streamStatus, setStreamStatus] = useState<StreamStatus>("idle");
  const [streamError, setStreamError] = useState<string | null>(null);

  const [ackingIds, setAckingIds] = useState<string[]>([]);

  const canAckNotifications = session?.role === "admin" || session?.role === "operator";
  const canCreateCommands = session?.role === "admin" || session?.role === "operator";

  const selectedCommandAction = useMemo(
    () => getCommandActionTemplateById(selectedCommandActionId) ?? commandActionTemplates[0] ?? null,
    [selectedCommandActionId]
  );

  async function fetchControlCommand(accessToken: string, commandId: string) {
    const response = await requestJson<ControlCommandResponse>(`/api/v1/control/commands/${commandId}`, {
      headers: {
        Authorization: `Bearer ${accessToken}`
      }
    });

    return response.command;
  }

  async function refreshControlCommand(accessToken: string, commandId: string) {
    setRefreshingCommandIds((current) => (current.includes(commandId) ? current : [...current, commandId]));

    try {
      const command = await fetchControlCommand(accessToken, commandId);
      setCommands((current) => upsertControlCommand(current, command));
      setCommandError(null);
    } catch (error) {
      setCommandError(error instanceof Error ? error.message : "Failed to refresh command status");
    } finally {
      setRefreshingCommandIds((current) => current.filter((id) => id !== commandId));
    }
  }

  async function loadDashboardData(accessToken: string) {
    setAlertsLoading(true);
    setNotificationsLoading(true);
    setAlertsError(null);
    setNotificationsError(null);

    const [alertsResult, notificationsResult] = await Promise.allSettled([
      requestJson<AlertListResponse>("/api/v1/alerts?limit=50", {
        headers: {
          Authorization: `Bearer ${accessToken}`
        }
      }),
      requestJson<NotificationListResponse>("/api/v1/notifications?limit=50", {
        headers: {
          Authorization: `Bearer ${accessToken}`
        }
      })
    ]);

    if (alertsResult.status === "fulfilled") {
      setAlerts(alertsResult.value.items);
      setAlertsError(null);
    } else {
      setAlerts([]);
      setAlertsError(alertsResult.reason instanceof Error ? alertsResult.reason.message : "Failed to load alerts");
    }

    if (notificationsResult.status === "fulfilled") {
      setNotifications(notificationsResult.value.items);
      setNotificationsError(null);
    } else {
      setNotifications([]);
      setNotificationsError(
        notificationsResult.reason instanceof Error ? notificationsResult.reason.message : "Failed to load notifications"
      );
    }

    setAlertsLoading(false);
    setNotificationsLoading(false);
  }

  async function handleLoginSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsLoggingIn(true);
    setLoginError(null);

    try {
      const response = await requestJson<LoginResponse>("/api/v1/auth/login", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ username, password })
      });

      setSession({
        accessToken: response.accessToken,
        role: response.role,
        expiresIn: response.expiresIn
      });
    } catch (error) {
      setLoginError(error instanceof Error ? error.message : "Login failed");
    } finally {
      setIsLoggingIn(false);
    }
  }

  async function handleRefreshClick() {
    if (!session) {
      return;
    }

    await loadDashboardData(session.accessToken);

    await Promise.all(
      commands.map(async (command) => {
        await refreshControlCommand(session.accessToken, command.id);
      })
    );
  }

  async function handleAckNotification(notificationId: string) {
    if (!session || !canAckNotifications) {
      return;
    }

    setAckingIds((current) => (current.includes(notificationId) ? current : [...current, notificationId]));

    try {
      const result = await requestJson<NotificationAckResponse>(`/api/v1/notifications/${notificationId}/ack`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${session.accessToken}`
        }
      });

      setNotifications((current) =>
        current.map((notification) =>
          notification.id === result.id
            ? {
                ...notification,
                isRead: result.isRead
              }
            : notification
        )
      );
    } catch (error) {
      setNotificationsError(error instanceof Error ? error.message : "Failed to acknowledge notification");
    } finally {
      setAckingIds((current) => current.filter((id) => id !== notificationId));
    }
  }

  async function handleCreateControlCommand(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!session || !canCreateCommands) {
      return;
    }

    setCommandError(null);
    setCommandSuccess(null);

    if (!selectedCommandAction) {
      setCommandError("Select an action template before submitting.");
      return;
    }

    setIsSubmittingCommand(true);

    try {
      const createResult = await requestJson<ControlCommandAccepted>("/api/v1/control/commands", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${session.accessToken}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          deviceId: commandDeviceId,
          commandType: selectedCommandAction.commandType,
          payload: selectedCommandAction.payload,
          idempotencyKey: commandIdempotencyKey
        } satisfies ControlCommandRequest)
      });

      const command = await fetchControlCommand(session.accessToken, createResult.commandId);
      setCommands((current) => upsertControlCommand(current, command));
      setCommandSuccess(`Command ${createResult.commandId} queued (${selectedCommandAction.label}).`);
      setCommandIdempotencyKey(createIdempotencyKey());
    } catch (error) {
      setCommandError(error instanceof Error ? error.message : "Failed to create control command");
    } finally {
      setIsSubmittingCommand(false);
    }
  }

  function handleLogout() {
    setSession(null);
    setAlerts([]);
    setNotifications([]);
    setCommands([]);
    setAlertsError(null);
    setNotificationsError(null);
    setCommandError(null);
    setCommandSuccess(null);
    setStreamStatus("idle");
    setStreamError(null);
  }

  useEffect(() => {
    if (!session) {
      return;
    }

    void loadDashboardData(session.accessToken);
  }, [session]);

  useEffect(() => {
    if (!session) {
      setStreamStatus("idle");
      setStreamError(null);
      return;
    }

    setStreamStatus("connecting");
    setStreamError(null);

    const disconnect = connectStream(
      session.accessToken,
      (event) => {
        if (event.eventType === "notification.created" && isNotificationRecord(event.data)) {
          const liveNotification = event.data;
          setNotifications((current) => {
            if (current.some((notification) => notification.id === liveNotification.id)) {
              return current;
            }

            return [liveNotification, ...current];
          });
        }

        if (event.eventType === "control.updated") {
          const command = extractControlCommandFromStream(event.data);
          if (command) {
            setCommands((current) => upsertControlCommand(current, command));
          }
        }

        setStreamStatus("connected");
        setStreamError(null);
      },
      () => {
        setStreamStatus("connected");
      },
      (errorMessage) => {
        setStreamStatus("error");
        setStreamError(errorMessage);
      }
    );

    return () => {
      disconnect();
    };
  }, [session]);

  useEffect(() => {
    if (!session) {
      return;
    }

    const pendingCommandIds = commands.filter((command) => isPendingStatus(command.status)).map((command) => command.id);
    if (pendingCommandIds.length === 0) {
      return;
    }

    const interval = setInterval(() => {
      void Promise.all(
        pendingCommandIds.map(async (commandId) => {
          try {
            const command = await fetchControlCommand(session.accessToken, commandId);
            setCommands((current) => upsertControlCommand(current, command));
          } catch {
            // Best effort polling; SSE is primary update path.
          }
        })
      );
    }, 5000);

    return () => {
      clearInterval(interval);
    };
  }, [session, commands]);

  const streamLabel = useMemo(() => {
    if (streamStatus === "connected") {
      return "Live stream connected";
    }
    if (streamStatus === "connecting") {
      return "Connecting live stream";
    }
    if (streamStatus === "error") {
      return "Live stream disconnected";
    }

    return "Live stream idle";
  }, [streamStatus]);

  const dashboardSnapshot = useMemo(() => {
    let openAlerts = 0;
    let criticalOpenAlerts = 0;
    let highOrCriticalOpenAlerts = 0;

    for (const alert of alerts) {
      if (alert.status !== "OPEN") {
        continue;
      }

      openAlerts += 1;

      if (alert.severity === "CRITICAL") {
        criticalOpenAlerts += 1;
      }

      if (alert.severity === "HIGH" || alert.severity === "CRITICAL") {
        highOrCriticalOpenAlerts += 1;
      }
    }

    const unreadNotifications = notifications.filter((notification) => !notification.isRead).length;
    const pendingCommands = commands.filter((command) => isPendingStatus(command.status)).length;
    const failedCommands = commands.filter((command) => command.status === "failed").length;

    let latestNotificationAt: string | null = null;
    let latestNotificationMs = -1;
    for (const notification of notifications) {
      const parsed = Date.parse(notification.createdAt);
      if (Number.isNaN(parsed) || parsed <= latestNotificationMs) {
        continue;
      }

      latestNotificationMs = parsed;
      latestNotificationAt = notification.createdAt;
    }

    let statusTone: "stable" | "monitoring" | "attention" | "critical" = "stable";
    let statusLabel = "Stable";
    let statusReason = "No open critical risks or failed commands in the latest window.";

    if (criticalOpenAlerts > 0) {
      statusTone = "critical";
      statusLabel = "Critical Risk";
      statusReason = "Critical threshold breaches are active and need immediate response.";
    } else if (highOrCriticalOpenAlerts > 0 || failedCommands > 0) {
      statusTone = "attention";
      statusLabel = "Attention Needed";
      statusReason = "High-severity risk or failed command execution was detected.";
    } else if (openAlerts > 0 || unreadNotifications > 0 || pendingCommands > 0) {
      statusTone = "monitoring";
      statusLabel = "Monitoring";
      statusReason = "Non-critical activity is present; continue operator review.";
    }

    return {
      openAlerts,
      criticalOpenAlerts,
      unreadNotifications,
      pendingCommands,
      failedCommands,
      latestNotificationAt,
      statusTone,
      statusLabel,
      statusReason
    };
  }, [alerts, notifications, commands]);

  return (
    <main className="page">
      <header className="page-header">
        <h1>Microgrid Dashboard</h1>
        <p>Alerts, notifications, and control command lifecycle updates.</p>
      </header>

      {!session ? (
        <section className="panel">
          <h2>Login</h2>
          <p className="hint">Use seeded credentials: admin/admin123, operator/operator123, viewer/viewer123.</p>
          <form className="form" onSubmit={handleLoginSubmit}>
            <label>
              Username
              <input value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" />
            </label>
            <label>
              Password
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                autoComplete="current-password"
              />
            </label>
            <button type="submit" disabled={isLoggingIn}>
              {isLoggingIn ? "Signing in..." : "Sign in"}
            </button>
          </form>
          {loginError ? <p className="error">{loginError}</p> : null}
        </section>
      ) : (
        <>
          <section className="panel">
            <h2>Operational Purpose</h2>
            <p className="hint">This dashboard is for operator decisions, not just data display.</p>
            <ul className="purpose-list">
              <li>
                <strong>Protect power quality:</strong> alerts show threshold breaches that can impact grid stability.
              </li>
              <li>
                <strong>Prioritize action queue:</strong> notifications represent items operators should acknowledge/review.
              </li>
              <li>
                <strong>Validate remote control:</strong> command lifecycle confirms whether requested actions succeeded or failed.
              </li>
              <li>
                <strong>Verify reliability at scale:</strong> load traffic should still result in actionable, readable operations data.
              </li>
            </ul>
          </section>

          <section className="panel">
            <h2>Operational Snapshot</h2>
            <p className="hint">Business-level state from current alerts, notifications, and command outcomes.</p>
            <div className="kpi-grid">
              <article className={`kpi-card kpi-${dashboardSnapshot.statusTone}`}>
                <span className="kpi-label">Current Risk</span>
                <strong className="kpi-value">{dashboardSnapshot.statusLabel}</strong>
                <span className="kpi-note">{dashboardSnapshot.statusReason}</span>
              </article>
              <article className="kpi-card">
                <span className="kpi-label">Open Alerts</span>
                <strong className="kpi-value">{dashboardSnapshot.openAlerts}</strong>
                <span className="kpi-note">Threshold violations currently unresolved.</span>
              </article>
              <article className="kpi-card">
                <span className="kpi-label">Critical Open</span>
                <strong className="kpi-value">{dashboardSnapshot.criticalOpenAlerts}</strong>
                <span className="kpi-note">Highest-risk alerts requiring immediate review.</span>
              </article>
              <article className="kpi-card">
                <span className="kpi-label">Unread Notifications</span>
                <strong className="kpi-value">{dashboardSnapshot.unreadNotifications}</strong>
                <span className="kpi-note">Items that still need operator acknowledgement.</span>
              </article>
              <article className="kpi-card">
                <span className="kpi-label">Pending Commands</span>
                <strong className="kpi-value">{dashboardSnapshot.pendingCommands}</strong>
                <span className="kpi-note">Queued or processing control actions.</span>
              </article>
              <article className="kpi-card">
                <span className="kpi-label">Failed Commands</span>
                <strong className="kpi-value">{dashboardSnapshot.failedCommands}</strong>
                <span className="kpi-note">Control actions that did not complete successfully.</span>
              </article>
            </div>
            <p className="muted">
              Latest notification event:{" "}
              {dashboardSnapshot.latestNotificationAt
                ? formatLocalDateTime(dashboardSnapshot.latestNotificationAt)
                : "none received yet"}
            </p>
          </section>

          <section className="panel toolbar">
            <div>
              <strong>Role:</strong> {session.role}
            </div>
            <div>
              <strong>Token TTL:</strong> {session.expiresIn}s
            </div>
            <div className={`stream-status ${streamStatus}`}>
              <span>{streamLabel}</span>
              {streamError ? <small>{streamError}</small> : null}
            </div>
            <div className="actions">
              <button type="button" onClick={handleRefreshClick}>
                Refresh
              </button>
              <button type="button" className="ghost" onClick={handleLogout}>
                Logout
              </button>
            </div>
          </section>

          <section className="panel">
            <h2>Control Commands</h2>
            <p className="hint">Use this to issue actions and verify execution lifecycle for each device command.</p>
            {canCreateCommands ? (
              <form className="form" onSubmit={handleCreateControlCommand}>
                <label>
                  Device ID
                  <input value={commandDeviceId} onChange={(event) => setCommandDeviceId(event.target.value)} />
                </label>
                <label>
                  Send Action
                  <select
                    value={selectedCommandActionId}
                    onChange={(event) => setSelectedCommandActionId(event.target.value)}
                  >
                    {commandActionTemplates.map((template) => (
                      <option key={template.id} value={template.id}>
                        {template.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  Idempotency Key
                  <input
                    value={commandIdempotencyKey}
                    onChange={(event) => setCommandIdempotencyKey(event.target.value)}
                  />
                </label>
                <div className="wide template-summary">
                  <div>
                    <strong>Command Type:</strong> {selectedCommandAction?.commandType ?? "N/A"}
                  </div>
                  <div className="muted">{selectedCommandAction?.description ?? "No action template selected."}</div>
                  <pre className="payload-preview">
                    {selectedCommandAction ? JSON.stringify(selectedCommandAction.payload, null, 2) : "{}"}
                  </pre>
                </div>
                <button type="submit" disabled={isSubmittingCommand}>
                  {isSubmittingCommand ? "Submitting..." : "Queue Command"}
                </button>
              </form>
            ) : (
              <p className="hint">Viewer role cannot create control commands.</p>
            )}

            {commandError ? <p className="error">{commandError}</p> : null}
            {commandSuccess ? <p className="hint">{commandSuccess}</p> : null}

            {commands.length === 0 ? (
              <p className="empty">No tracked commands yet.</p>
            ) : (
              <ul className="list">
                {commands.map((command) => {
                  const isRefreshing = refreshingCommandIds.includes(command.id);
                  return (
                    <li key={command.id} className="row">
                      <div>
                        <strong>{command.commandType}</strong> on {command.deviceId}
                      </div>
                      <div className="muted">Command ID: {command.id}</div>
                      <div className="muted">Idempotency: {command.idempotencyKey}</div>
                      <div className={`status status-${command.status}`}>Status: {command.status}</div>
                      <div className="muted">
                        Requested by {command.requestedBy ?? "unknown"} · {formatLocalDateTime(command.updatedAt)}
                      </div>
                      {session ? (
                        <button
                          type="button"
                          disabled={isRefreshing}
                          onClick={() => void refreshControlCommand(session.accessToken, command.id)}
                        >
                          {isRefreshing ? "Refreshing..." : "Refresh status"}
                        </button>
                      ) : null}
                    </li>
                  );
                })}
              </ul>
            )}
          </section>

          <section className="panel">
            <h2>Alerts (Risk Signals)</h2>
            <p className="hint">Each alert indicates a rule breach that may affect service quality or stability.</p>
            {alertsLoading ? <p className="hint">Loading alerts...</p> : null}
            {alertsError ? <p className="error">{alertsError}</p> : null}
            {!alertsLoading && !alertsError && alerts.length === 0 ? <p className="empty">No alerts yet.</p> : null}

            {!alertsLoading && alerts.length > 0 ? (
              <ul className="list">
                {alerts.map((alert) => (
                  <li key={alert.id} className={`row severity-${alert.severity.toLowerCase()}`}>
                    <div>
                      <strong>{alert.severity}</strong> · {alert.status}
                    </div>
                    <div className="muted">Device {alert.deviceId}</div>
                    <div>{alert.message}</div>
                    <div className="muted">{formatLocalDateTime(alert.createdAt)}</div>
                  </li>
                ))}
              </ul>
            ) : null}
          </section>

          <section className="panel">
            <h2>Notifications (Operator Action Queue)</h2>
            <p className="hint">
              Notifications are workflow items generated by alerts. Unread entries represent pending operator review.
            </p>
            {notificationsLoading ? <p className="hint">Loading notifications...</p> : null}
            {notificationsError ? <p className="error">{notificationsError}</p> : null}
            {!notificationsLoading && !notificationsError && notifications.length === 0 ? (
              <p className="empty">No notifications yet.</p>
            ) : null}

            {!notificationsLoading && notifications.length > 0 ? (
              <ul className="list">
                {notifications.map((notification) => {
                  const isAcking = ackingIds.includes(notification.id);
                  return (
                    <li key={notification.id} className={`row ${notification.isRead ? "read" : "unread"}`}>
                      <div>
                        <strong>{notification.type}</strong> · {notification.isRead ? "read" : "unread"}
                      </div>
                      <div>{notification.message}</div>
                      <div className="muted">{formatLocalDateTime(notification.createdAt)}</div>
                      {canAckNotifications ? (
                        <button
                          type="button"
                          disabled={notification.isRead || isAcking}
                          onClick={() => void handleAckNotification(notification.id)}
                        >
                          {isAcking ? "Acking..." : "Acknowledge"}
                        </button>
                      ) : (
                        <div className="muted">Viewer role cannot acknowledge notifications.</div>
                      )}
                    </li>
                  );
                })}
              </ul>
            ) : null}
          </section>
        </>
      )}
    </main>
  );
}

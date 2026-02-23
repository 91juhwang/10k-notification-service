import { FormEvent, useEffect, useMemo, useState } from "react";
import type { components } from "@microgrid/shared";

type LoginResponse = components["schemas"]["LoginResponse"];
type AlertRecord = components["schemas"]["Alert"];
type NotificationRecord = components["schemas"]["Notification"];
type NotificationAckResponse = components["schemas"]["NotificationAckResponse"];
type StreamEvent = components["schemas"]["StreamEvent"];
type AlertListResponse = components["schemas"]["AlertListResponse"];
type NotificationListResponse = components["schemas"]["NotificationListResponse"];
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

function readApiErrorMessage(value: unknown) {
  if (!value || typeof value !== "object") {
    return null;
  }

  const body = value as ApiErrorBody;
  return typeof body.message === "string" ? body.message : null;
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
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const payload = value as {
    id?: unknown;
    type?: unknown;
    message?: unknown;
    isRead?: unknown;
    payload?: unknown;
    createdAt?: unknown;
  };

  return (
    typeof payload.id === "string" &&
    typeof payload.type === "string" &&
    typeof payload.message === "string" &&
    typeof payload.isRead === "boolean" &&
    typeof payload.payload === "object" &&
    payload.payload !== null &&
    typeof payload.createdAt === "string"
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

  const [streamStatus, setStreamStatus] = useState<StreamStatus>("idle");
  const [streamError, setStreamError] = useState<string | null>(null);

  const [ackingIds, setAckingIds] = useState<string[]>([]);

  const canAckNotifications = session?.role === "admin" || session?.role === "operator";

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

  function handleLogout() {
    setSession(null);
    setAlerts([]);
    setNotifications([]);
    setAlertsError(null);
    setNotificationsError(null);
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
        if (event.eventType !== "notification.created" || !isNotificationRecord(event.data)) {
          return;
        }
        const liveNotification = event.data;

        setStreamStatus("connected");
        setStreamError(null);

        setNotifications((current) => {
          if (current.some((notification) => notification.id === liveNotification.id)) {
            return current;
          }

          return [liveNotification, ...current];
        });
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

  return (
    <main className="page">
      <header className="page-header">
        <h1>Microgrid Dashboard</h1>
        <p>Alerts and notifications with authenticated streaming updates.</p>
      </header>

      {!session ? (
        <section className="panel">
          <h2>Login</h2>
          <p className="hint">Use seeded credentials from env: admin/admin123, operator/operator123, viewer/viewer123.</p>
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
            <h2>Alerts</h2>
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
                    <div className="muted">{new Date(alert.createdAt).toLocaleString()}</div>
                  </li>
                ))}
              </ul>
            ) : null}
          </section>

          <section className="panel">
            <h2>Notifications</h2>
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
                      <div className="muted">{new Date(notification.createdAt).toLocaleString()}</div>
                      {canAckNotifications ? (
                        <button
                          type="button"
                          disabled={notification.isRead || isAcking}
                          onClick={() => handleAckNotification(notification.id)}
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

import "server-only";

import type { Attempt, Notification, NotificationPage } from "./types";

/**
 * Server-side client for the notification API.
 *
 * The `server-only` import above is the security boundary, not a style choice.
 * The API key carries WRITE access — anyone holding it can send notifications
 * as this client — so it must never reach the browser. Importing this module
 * from a Client Component is a build error rather than a leaked credential in
 * a JavaScript bundle.
 *
 * For the same reason the key is read from `API_KEY`, deliberately without the
 * `NEXT_PUBLIC_` prefix that would inline it into the client bundle.
 */

const BASE_URL = process.env.API_BASE_URL ?? "http://localhost:8080";
const API_KEY = process.env.API_KEY ?? "";

/** How long to wait on the API before giving up and showing an error. */
const REQUEST_TIMEOUT_MS = 10_000;

/** An API call that failed, carrying enough detail to render something useful. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }

  /** Whether this is a configuration problem the operator must fix. */
  get isAuthFailure(): boolean {
    return this.status === 401 || this.status === 403;
  }
}

/** Raised when the API cannot be reached at all. */
export class ApiUnreachableError extends Error {
  constructor(readonly cause: unknown) {
    super(`Could not reach the API at ${BASE_URL}`);
    this.name = "ApiUnreachableError";
  }
}

async function request<T>(path: string): Promise<T> {
  if (!API_KEY) {
    throw new ApiError(
      401,
      "API_KEY is not set. Add it to web/.env.local — see .env.local.example.",
    );
  }

  let response: Response;
  try {
    response = await fetch(`${BASE_URL}${path}`, {
      headers: {
        Authorization: `Bearer ${API_KEY}`,
        Accept: "application/json",
      },
      // A dashboard must never show stale delivery state, and fetch is not
      // cached by default in this version — this makes the intent explicit
      // rather than dependent on a default.
      cache: "no-store",
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
  } catch (cause) {
    throw new ApiUnreachableError(cause);
  }

  if (!response.ok) {
    throw new ApiError(response.status, await errorMessage(response));
  }

  return (await response.json()) as T;
}

/** Extracts the API's error message, falling back to the status text. */
async function errorMessage(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { error?: string };
    if (body.error) return body.error;
  } catch {
    // Body was not JSON; fall through to the status line.
  }
  return `${response.status} ${response.statusText}`;
}

export interface ListOptions {
  status?: string;
  channel?: string;
  cursor?: string;
  limit?: number;
}

/** Lists notifications, newest first. */
export async function listNotifications(
  options: ListOptions = {},
): Promise<NotificationPage> {
  const params = new URLSearchParams();
  if (options.status) params.set("status", options.status);
  if (options.channel) params.set("channel", options.channel);
  if (options.cursor) params.set("cursor", options.cursor);
  params.set("limit", String(options.limit ?? 25));

  return request<NotificationPage>(`/v1/notifications?${params}`);
}

/** Fetches one notification. */
export async function getNotification(id: string): Promise<Notification> {
  return request<Notification>(`/v1/notifications/${encodeURIComponent(id)}`);
}

/** Fetches a notification's delivery history, newest attempt first. */
export async function listAttempts(id: string): Promise<Attempt[]> {
  const body = await request<{ data: Attempt[] }>(
    `/v1/notifications/${encodeURIComponent(id)}/attempts`,
  );
  return body.data;
}

/** The API base URL, for display in operator-facing error states. */
export function apiBaseUrl(): string {
  return BASE_URL;
}

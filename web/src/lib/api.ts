import "server-only";

import type {
  Attempt,
  Channel,
  Notification,
  NotificationPage,
  Status,
} from "./types";

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

interface RequestOptions {
  method?: string;
  body?: unknown;
  /** Extra headers, used for Idempotency-Key on writes. */
  headers?: Record<string, string>;
}

/** A status code and decoded body, for callers that interpret both. */
interface RawResponse {
  status: number;
  body: unknown;
}

/**
 * Sends a request and returns the status alongside the decoded body, without
 * throwing on a non-2xx.
 *
 * Writes use this rather than `request` because a rejected notification is not
 * an exceptional condition — 400 and 409 are answers, and they need to reach
 * the form field that caused them.
 */
async function requestRaw(
  path: string,
  options: RequestOptions = {},
): Promise<RawResponse> {
  if (!API_KEY) {
    throw new ApiError(
      401,
      "API_KEY is not set. Add it to web/.env.local — see .env.local.example.",
    );
  }

  const headers: Record<string, string> = {
    Authorization: `Bearer ${API_KEY}`,
    Accept: "application/json",
    ...options.headers,
  };
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }

  let response: Response;
  try {
    response = await fetch(`${BASE_URL}${path}`, {
      method: options.method ?? "GET",
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      // A dashboard must never show stale delivery state, and fetch is not
      // cached by default in this version — this makes the intent explicit
      // rather than dependent on a default.
      cache: "no-store",
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
  } catch (cause) {
    throw new ApiUnreachableError(cause);
  }

  return { status: response.status, body: await json(response) };
}

/** Reads a JSON body, tolerating a non-JSON one. */
async function json(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

/**
 * Sends a read request, throwing on anything that is not a success.
 *
 * Reads have no useful non-2xx outcome for the person looking at the page, so
 * failures become an error state rather than something each caller unpacks.
 */
async function request<T>(path: string): Promise<T> {
  const { status, body } = await requestRaw(path);

  if (status < 200 || status >= 300) {
    const message = (body as { error?: string } | null)?.error;
    throw new ApiError(status, message ?? `Request failed with ${status}`);
  }

  return body as T;
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

/** A notification to submit. Mirrors the API's accepted body. */
export interface CreateInput {
  channel: Channel;
  recipient: string;
  payload: unknown;
  /** RFC3339. Omitted means "send now" — the API defaults it. */
  scheduled_at?: string;
  max_attempts?: number;
}

/** What the API decided about a submitted notification. */
export type CreateResult =
  | { ok: true; id: string; status: Status }
  | { ok: false; error: string; field?: string };

/**
 * Submits a notification.
 *
 * The idempotency key is the caller's, not this function's, deliberately: the
 * key is what makes a retry safe, and only the caller knows whether a second
 * submission is the same intent (a double-click) or a genuinely new one.
 *
 * 400 and 409 are returned as values rather than thrown. They are the two
 * outcomes a person can act on — a bad field, or a key reused with different
 * content — and both belong next to the form input that caused them, not on an
 * error page.
 */
export async function createNotification(
  input: CreateInput,
  idempotencyKey: string,
): Promise<CreateResult> {
  const { status, body } = await requestRaw("/v1/notifications", {
    method: "POST",
    body: input,
    headers: { "Idempotency-Key": idempotencyKey },
  });

  const decoded = (body ?? {}) as {
    id?: string;
    status?: Status;
    error?: string;
    field?: string;
  };

  // 202 is the only success this endpoint returns; delivery is asynchronous, so
  // it means "accepted", never "delivered".
  if (status === 202 && decoded.id) {
    return { ok: true, id: decoded.id, status: decoded.status ?? "pending" };
  }

  if (status === 400 || status === 409) {
    return {
      ok: false,
      error: decoded.error ?? "The API rejected this notification.",
      field: decoded.field,
    };
  }

  // Anything else — 401, 429, 500, an unreachable provider — is not something
  // the person filling in the form can fix by editing a field, so it surfaces
  // as an error rather than field-level feedback.
  throw new ApiError(status, decoded.error ?? `Request failed with ${status}`);
}

/** The API base URL, for display in operator-facing error states. */
export function apiBaseUrl(): string {
  return BASE_URL;
}

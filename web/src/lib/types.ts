/**
 * Types mirroring the notification API's read surface.
 *
 * These are hand-written rather than generated: the API is small and stable,
 * and a hand-written type is the place to record what a field actually means.
 */

/** A notification's lifecycle state. */
export type Status =
  | "pending"
  | "queued"
  | "delivering"
  | "delivered"
  | "failed"
  | "dead_lettered";

/** The delivery channels the system supports. */
export type Channel = "email" | "sms" | "push";

export const STATUSES: Status[] = [
  "pending",
  "queued",
  "delivering",
  "delivered",
  "failed",
  "dead_lettered",
];

export const CHANNELS: Channel[] = ["email", "sms", "push"];

/** One accepted notification and its delivery state. */
export interface Notification {
  id: string;
  channel: Channel;
  recipient: string;
  payload: unknown;
  status: Status;
  /**
   * Failed delivery attempts. Note this counts failures, so a notification
   * delivered on the first try reports 0.
   */
  attempts: number;
  max_attempts: number;
  idempotency_key: string;
  /** Earliest time this may be delivered; retries advance it. */
  scheduled_at: string;
  created_at: string;
  updated_at: string;
}

/** One call to a provider and what came back. */
export interface Attempt {
  id: string;
  attempt_number: number;
  outcome: "succeeded" | "failed";
  /** The provider's error, absent on success. */
  error?: string;
  started_at: string;
  finished_at: string;
  duration_ms: number;
}

/** A page of notifications. `next_cursor` is null on the last page. */
export interface NotificationPage {
  data: Notification[];
  next_cursor: string | null;
}

/** Terminal states never change again. */
export function isTerminal(status: Status): boolean {
  return status === "delivered" || status === "dead_lettered";
}

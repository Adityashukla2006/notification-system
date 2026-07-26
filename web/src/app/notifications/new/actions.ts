"use server";

import { redirect } from "next/navigation";

import {
  ApiError,
  ApiUnreachableError,
  createNotification,
  type CreateInput,
} from "@/lib/api";
import { CHANNELS, type Channel } from "@/lib/types";

/**
 * The Server Action behind the create form.
 *
 * This exists so the browser never holds the API key. The form posts here, to
 * the Next server, which is the only side that can read `API_KEY` and call the
 * Go API. Moving the submission into the browser would mean shipping a
 * write-capable credential to every visitor.
 *
 * Validation is duplicated from the API on purpose. The API remains the
 * authority — it is what a non-browser client hits — but re-checking here turns
 * a fixable mistake into inline feedback instead of a round trip, and catches
 * malformed JSON before it becomes an opaque 400.
 */

/** What the form renders after a submission. */
export interface FormState {
  error?: string;
  /** Names the offending input so the error can sit next to it. */
  field?: string;
}

/** Offsets for the relative timer, in seconds. */
const PRESET_DELAYS: Record<string, number> = {
  now: 0,
  "30s": 30,
  "1m": 60,
  "5m": 5 * 60,
  "15m": 15 * 60,
  "1h": 60 * 60,
};

/** Guards the max_attempts input. The API only requires > 0; this also caps it,
 *  because a form is an easy place to typo a number that ties up a worker. */
const MAX_ATTEMPTS_LIMIT = 10;

export async function createNotificationAction(
  _previous: FormState,
  formData: FormData,
): Promise<FormState> {
  const channel = String(formData.get("channel") ?? "");
  if (!isChannel(channel)) {
    return { error: "Choose a channel.", field: "channel" };
  }

  const recipient = String(formData.get("recipient") ?? "").trim();
  if (!recipient) {
    return { error: "A recipient is required.", field: "recipient" };
  }

  const payload = parsePayload(String(formData.get("payload") ?? ""));
  if ("error" in payload) {
    return { error: payload.error, field: "payload" };
  }

  const maxAttempts = parseMaxAttempts(String(formData.get("max_attempts") ?? ""));
  if ("error" in maxAttempts) {
    return { error: maxAttempts.error, field: "max_attempts" };
  }

  const scheduledAt = resolveSchedule(formData);
  if ("error" in scheduledAt) {
    return { error: scheduledAt.error, field: "schedule" };
  }

  // A blank key means "this is a new intent", so one is minted per submission.
  // Supplying a key by hand is what lets you submit twice and watch the API
  // replay the first notification instead of creating a second.
  const idempotencyKey =
    String(formData.get("idempotency_key") ?? "").trim() || crypto.randomUUID();

  const input: CreateInput = {
    channel,
    recipient,
    payload: payload.value,
    ...(maxAttempts.value === undefined ? {} : { max_attempts: maxAttempts.value }),
    ...(scheduledAt.value === undefined ? {} : { scheduled_at: scheduledAt.value }),
  };

  let result;
  try {
    result = await createNotification(input, idempotencyKey);
  } catch (error) {
    return { error: submissionErrorMessage(error) };
  }

  if (!result.ok) {
    return { error: result.error, field: result.field };
  }

  // Land on the detail page: the point of scheduling something is watching it
  // move, and that is the view where the lifecycle is visible.
  redirect(`/notifications/${result.id}`);
}

/** Turns a thrown API failure into something worth reading above the form. */
function submissionErrorMessage(error: unknown): string {
  if (error instanceof ApiUnreachableError) {
    return "Could not reach the API. Is the server running?";
  }
  if (error instanceof ApiError) {
    if (error.isAuthFailure) {
      return "The dashboard's API key was rejected. Check API_KEY in web/.env.local.";
    }
    if (error.status === 429) {
      return "Rate limited by the API. Wait a moment and submit again.";
    }
    return error.message;
  }
  return "Something went wrong submitting this notification.";
}

function isChannel(value: string): value is Channel {
  return (CHANNELS as string[]).includes(value);
}

/** Parses the payload textarea, enforcing the API's "JSON object" rule early. */
function parsePayload(raw: string): { value: unknown } | { error: string } {
  const trimmed = raw.trim();
  if (!trimmed) return { error: "A payload is required." };

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return { error: "The payload is not valid JSON." };
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { error: "The payload must be a JSON object, not an array or a value." };
  }
  return { value: parsed };
}

/** Parses max_attempts, treating blank as "let the API apply its default". */
function parseMaxAttempts(
  raw: string,
): { value: number | undefined } | { error: string } {
  const trimmed = raw.trim();
  if (!trimmed) return { value: undefined };

  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return { error: "Max attempts must be a whole number of at least 1." };
  }
  if (parsed > MAX_ATTEMPTS_LIMIT) {
    return { error: `Max attempts is capped at ${MAX_ATTEMPTS_LIMIT} here.` };
  }
  return { value: parsed };
}

/**
 * Resolves the two timer modes into a single RFC3339 instant.
 *
 * Relative delays are computed here, at submit time, rather than when the form
 * rendered — otherwise "in 1 minute" would mean one minute after the page
 * loaded, which is wrong for anyone who paused to fill in the payload.
 *
 * Absolute times arrive already converted to UTC by the browser, because a
 * `datetime-local` value carries no zone: interpreting "14:30" here would
 * silently use the server's timezone instead of the person's.
 */
function resolveSchedule(
  formData: FormData,
): { value: string | undefined } | { error: string } {
  const mode = String(formData.get("schedule_mode") ?? "now");

  if (mode === "at") {
    const iso = String(formData.get("scheduled_at") ?? "").trim();
    if (!iso) return { error: "Pick a date and time, or choose a delay." };

    const at = new Date(iso);
    if (Number.isNaN(at.getTime())) {
      return { error: "That date and time could not be read." };
    }
    return { value: at.toISOString() };
  }

  const seconds = PRESET_DELAYS[mode];
  if (seconds === undefined) {
    return { error: "Choose when this should be sent." };
  }
  // "now" omits the field entirely and lets the API stamp its own default,
  // which keeps the ready-queue path identical to a plain API call.
  if (seconds === 0) return { value: undefined };

  return { value: new Date(Date.now() + seconds * 1000).toISOString() };
}

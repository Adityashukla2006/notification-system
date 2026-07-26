"use client";

import { useActionState, useId, useState } from "react";

import {
  createNotificationAction,
  type FormState,
} from "@/app/notifications/new/actions";
import { CHANNELS, type Channel } from "@/lib/types";

/**
 * The manual send form.
 *
 * A Client Component for three reasons: inline validation errors need
 * `useActionState`, the payload template should follow the channel you pick,
 * and only the browser can convert a `datetime-local` value into a real
 * instant using the viewer's own timezone.
 *
 * It never imports `@/lib/api` — that module is `server-only`, and reaching for
 * it here would be a build error rather than a leaked API key.
 */

/** Starting payloads per channel, matching what each provider actually reads. */
const PAYLOAD_TEMPLATES: Record<Channel, string> = {
  email: '{\n  "subject": "Test notification",\n  "body": "Hello from the dashboard.",\n  "html": false\n}',
  sms: '{\n  "body": "Hello from the dashboard."\n}',
  push: '{\n  "title": "Test notification",\n  "body": "Hello from the dashboard."\n}',
};

/** The relative timer options, in the order they read as a scale. */
const DELAYS = [
  { value: "now", label: "Now" },
  { value: "30s", label: "+30s" },
  { value: "1m", label: "+1m" },
  { value: "5m", label: "+5m" },
  { value: "15m", label: "+15m" },
  { value: "1h", label: "+1h" },
  { value: "at", label: "At…" },
];

const INITIAL_STATE: FormState = {};

export function CreateNotificationForm() {
  const [state, formAction, pending] = useActionState(
    createNotificationAction,
    INITIAL_STATE,
  );

  // Every input is controlled, including ones with no dynamic behaviour of
  // their own. React resets an uncontrolled field once a form action settles,
  // which would empty the form on a validation error — the one moment the
  // person needs their input back to fix it.
  const [channel, setChannel] = useState<Channel>("email");
  const [recipient, setRecipient] = useState("");
  const [payload, setPayload] = useState(PAYLOAD_TEMPLATES.email);
  const [maxAttempts, setMaxAttempts] = useState("");
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [mode, setMode] = useState("now");
  const [localTime, setLocalTime] = useState("");

  const ids = {
    channel: useId(),
    recipient: useId(),
    payload: useId(),
    maxAttempts: useId(),
    idempotencyKey: useId(),
    scheduledAt: useId(),
  };

  /**
   * Switching channel swaps the template, but only while it is still a
   * template — silently discarding a payload someone typed would be worse than
   * leaving a mismatched one in place.
   */
  function changeChannel(next: Channel) {
    const untouched = Object.values(PAYLOAD_TEMPLATES).includes(payload);
    setChannel(next);
    if (untouched) setPayload(PAYLOAD_TEMPLATES[next]);
  }

  // The browser resolves the local value to a real instant, including DST.
  // The server receives an unambiguous UTC timestamp.
  const scheduledAtIso = isoFromLocal(localTime);

  return (
    <form action={formAction} className="space-y-6">
      {state.error && (
        <p
          role="alert"
          className="rounded-md border border-danger-fg/30 bg-danger-bg px-3 py-2 text-sm text-danger-fg"
        >
          {state.error}
        </p>
      )}

      <section className="space-y-4 rounded-lg border border-border-subtle bg-surface p-4">
        <Field
          label="Channel"
          htmlFor={ids.channel}
          invalid={state.field === "channel"}
        >
          <select
            id={ids.channel}
            name="channel"
            value={channel}
            onChange={(e) => changeChannel(e.target.value as Channel)}
            className={inputClass(state.field === "channel")}
          >
            {CHANNELS.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </Field>

        <Field
          label="Recipient"
          htmlFor={ids.recipient}
          hint="An address the provider will accept. Use an invalid one to watch retries and dead-lettering."
          invalid={state.field === "recipient"}
        >
          <input
            id={ids.recipient}
            name="recipient"
            required
            autoComplete="off"
            value={recipient}
            onChange={(e) => setRecipient(e.target.value)}
            placeholder={channel === "email" ? "you@example.com" : "+15550100"}
            className={inputClass(state.field === "recipient")}
          />
        </Field>

        <Field
          label="Payload"
          htmlFor={ids.payload}
          hint="A JSON object. This is stored as jsonb and handed to the provider verbatim."
          invalid={state.field === "payload"}
        >
          <textarea
            id={ids.payload}
            name="payload"
            rows={7}
            spellCheck={false}
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
            className={`${inputClass(state.field === "payload")} font-mono text-xs leading-relaxed`}
          />
        </Field>
      </section>

      <section className="space-y-3 rounded-lg border border-border-subtle bg-surface p-4">
        <div>
          <h2 className="text-sm font-medium">Send at</h2>
          <p className="mt-1 text-sm text-muted">
            Anything in the future is parked in a Redis sorted set instead of
            the ready queue, and promoted onto the queue when it comes due.
          </p>
        </div>

        <input type="hidden" name="schedule_mode" value={mode} />
        <div className="flex flex-wrap gap-2">
          {DELAYS.map((d) => (
            <button
              key={d.value}
              type="button"
              onClick={() => setMode(d.value)}
              aria-pressed={mode === d.value}
              className={`rounded-md border px-3 py-1.5 text-sm transition-colors ${
                mode === d.value
                  ? "border-accent bg-accent/10 text-accent"
                  : "border-border-subtle text-muted hover:text-foreground"
              }`}
            >
              {d.label}
            </button>
          ))}
        </div>

        {mode === "at" && (
          <Field
            label="Date and time"
            htmlFor={ids.scheduledAt}
            hint="Your local time. It is converted to UTC before it reaches the API."
            invalid={state.field === "schedule"}
          >
            <input
              id={ids.scheduledAt}
              type="datetime-local"
              value={localTime}
              onChange={(e) => setLocalTime(e.target.value)}
              className={inputClass(state.field === "schedule")}
            />
            <input type="hidden" name="scheduled_at" value={scheduledAtIso} />
          </Field>
        )}

        {mode !== "at" && mode !== "now" && (
          <p className="text-sm text-muted">
            The delay is measured from when you submit, not from now.
          </p>
        )}
      </section>

      <details className="rounded-lg border border-border-subtle bg-surface">
        <summary className="cursor-pointer px-4 py-3 text-sm font-medium">
          Delivery options
        </summary>
        <div className="space-y-4 border-t border-border-subtle p-4">
          <Field
            label="Max attempts"
            htmlFor={ids.maxAttempts}
            hint="Blank uses the server default. Set it to 2 with a bad recipient to reach a dead letter quickly."
            invalid={state.field === "max_attempts"}
          >
            <input
              id={ids.maxAttempts}
              name="max_attempts"
              type="number"
              min={1}
              max={10}
              value={maxAttempts}
              onChange={(e) => setMaxAttempts(e.target.value)}
              placeholder="default"
              className={inputClass(state.field === "max_attempts")}
            />
          </Field>

          <Field
            label="Idempotency key"
            htmlFor={ids.idempotencyKey}
            hint="Blank generates a fresh one. Reuse a key to watch the API return the original notification instead of creating a second."
            invalid={state.field === "idempotency_key"}
          >
            <input
              id={ids.idempotencyKey}
              name="idempotency_key"
              autoComplete="off"
              value={idempotencyKey}
              onChange={(e) => setIdempotencyKey(e.target.value)}
              placeholder="generated per submission"
              className={`${inputClass(state.field === "idempotency_key")} font-mono text-xs`}
            />
          </Field>
        </div>
      </details>

      <div className="flex items-center gap-3">
        <button
          type="submit"
          disabled={pending}
          className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-background disabled:opacity-60"
        >
          {pending ? "Submitting…" : "Send notification"}
        </button>
        <p className="text-sm text-muted">
          The API answers 202 Accepted — never &ldquo;delivered&rdquo;.
        </p>
      </div>
    </form>
  );
}

/** Converts a `datetime-local` value to RFC3339, or "" when unset/unparseable. */
function isoFromLocal(value: string): string {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString();
}

function inputClass(invalid: boolean): string {
  return `w-full rounded-md border bg-surface px-3 py-2 text-sm text-foreground focus:outline-none ${
    invalid
      ? "border-danger-fg focus:border-danger-fg"
      : "border-border-subtle focus:border-accent"
  }`;
}

function Field({
  label,
  htmlFor,
  hint,
  invalid,
  children,
}: {
  label: string;
  htmlFor: string;
  hint?: string;
  invalid?: boolean;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1.5">
      <label htmlFor={htmlFor} className="block text-sm font-medium">
        {label}
      </label>
      {children}
      {hint && (
        <p className={`text-xs ${invalid ? "text-danger-fg" : "text-muted"}`}>
          {hint}
        </p>
      )}
    </div>
  );
}

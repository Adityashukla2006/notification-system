import { Countdown } from "@/components/Countdown";
import { absoluteTime, duration } from "@/lib/format";
import type { Attempt, Notification } from "@/lib/types";

/**
 * The lifecycle of one notification, as a single vertical trace.
 *
 * The list and the attempt table each answer part of "what happened", but
 * neither shows the shape of it: that a future-dated notification sat in a
 * sorted set before it was ever queued, that the gap between two attempts is
 * backoff rather than the system being slow, or that a dead letter is where
 * retries ran out rather than a separate failure. This reconstructs that
 * sequence from the two things the API exposes — the notification's own
 * timestamps and its attempts.
 *
 * It is a reconstruction, not an event log. The API records state, not
 * transitions, so the ordering here is inferred. A production system usually
 * emits real lifecycle events for exactly this reason; inferring is fine when
 * the state machine is small enough to be unambiguous.
 */

type Tone = "neutral" | "info" | "ok" | "warn" | "danger";

interface Step {
  key: string;
  label: string;
  detail: string;
  at?: string;
  tone: Tone;
  /** Rendered under the step, for the live countdown on a pending send. */
  extra?: React.ReactNode;
}

const TONE_DOT: Record<Tone, string> = {
  neutral: "bg-neutral-fg",
  info: "bg-info-fg",
  ok: "bg-ok-fg",
  warn: "bg-warn-fg",
  danger: "bg-danger-fg",
};

export function Timeline({
  notification,
  attempts,
}: {
  notification: Notification;
  attempts: Attempt[];
}) {
  const steps = buildSteps(notification, attempts);

  return (
    <section className="rounded-lg border border-border-subtle bg-surface">
      <h2 className="flex items-baseline justify-between border-b border-border-subtle px-4 py-3">
        <span className="text-sm font-medium">Lifecycle</span>
        <span className="text-xs text-muted">accepted → queued → delivered</span>
      </h2>

      <ol className="px-4 py-4">
        {steps.map((step, index) => (
          <li key={step.key} className="flex gap-3">
            {/* The rail: a dot per step, joined by a line except after the
                last one, so the trace reads as one continuous sequence. */}
            <div className="flex flex-col items-center">
              <span
                aria-hidden
                className={`mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full ${TONE_DOT[step.tone]}`}
              />
              {index < steps.length - 1 && (
                <span
                  aria-hidden
                  className="w-px flex-1 bg-border-subtle"
                  style={{ minHeight: "1.5rem" }}
                />
              )}
            </div>

            <div className="flex-1 pb-4">
              <div className="flex flex-wrap items-baseline justify-between gap-x-3">
                <span className="text-sm font-medium">{step.label}</span>
                {step.at && (
                  <span className="text-xs text-muted tabular">
                    {absoluteTime(step.at)}
                  </span>
                )}
              </div>
              <p className="mt-0.5 text-sm text-muted">{step.detail}</p>
              {step.extra}
            </div>
          </li>
        ))}
      </ol>
    </section>
  );
}

/** Reconstructs the sequence of steps from stored state. */
function buildSteps(n: Notification, attempts: Attempt[]): Step[] {
  const steps: Step[] = [
    {
      key: "accepted",
      label: "Accepted",
      detail:
        "Validated and written to Postgres inside one transaction, then 202 returned. Postgres is the source of truth from here on.",
      at: n.created_at,
      tone: "neutral",
    },
  ];

  // Attempts arrive newest first; a trace has to run forwards.
  const ordered = [...attempts].sort(
    (a, b) => a.attempt_number - b.attempt_number,
  );

  // scheduled_at carries two different meanings over a notification's life: the
  // time the client asked for, and — after a failure — the time the next retry
  // is due. A retry overwrites the original, so a future scheduled_at is only
  // evidence of a deliberate schedule while it still sits before the first
  // attempt. Without this check, every retried notification would claim to have
  // been scheduled, at a timestamp later than the attempt it supposedly preceded.
  const first = ordered[0];
  const wasScheduled =
    new Date(n.scheduled_at) > new Date(n.created_at) &&
    (!first || new Date(n.scheduled_at) <= new Date(first.started_at));

  // Still waiting on the send the client actually asked for.
  const awaitingFirstSend = wasScheduled && new Date(n.scheduled_at) > new Date();

  if (wasScheduled) {
    steps.push({
      key: "scheduled",
      label: awaitingFirstSend ? "Waiting to be sent" : "Was scheduled",
      detail: awaitingFirstSend
        ? "Parked in a Redis sorted set, scored by its send time. It is not on the ready queue, so no worker can claim it yet."
        : "Held in the sorted set until it came due, then promoted onto the ready queue.",
      at: n.scheduled_at,
      tone: awaitingFirstSend ? "warn" : "neutral",
      extra: awaitingFirstSend ? (
        <div className="mt-2">
          <Countdown to={n.scheduled_at} />
        </div>
      ) : undefined,
    });
  }

  if (!awaitingFirstSend) {
    steps.push({
      key: "queued",
      label: "Queued",
      detail:
        "Pushed onto the Redis list a worker blocks on. If Redis were wiped now, this could be rebuilt from Postgres.",
      tone: "info",
    });
  }

  ordered.forEach((attempt, index) => {
    const failed = attempt.outcome === "failed";
    const previous = ordered[index - 1];

    // The gap between the previous failure and this attempt is backoff, and
    // saying so is the difference between "the system is slow" and "the system
    // is doing exactly what it was told".
    let detail = failed
      ? `The provider rejected this in ${duration(attempt.duration_ms)}.`
      : `The provider accepted this in ${duration(attempt.duration_ms)}.`;

    if (previous) {
      const waited =
        new Date(attempt.started_at).getTime() -
        new Date(previous.finished_at).getTime();
      if (waited > 0) {
        detail += ` Waited ${duration(waited)} after the previous failure — exponential backoff with jitter.`;
      }
    }

    steps.push({
      key: attempt.id,
      label: `Attempt ${attempt.attempt_number} — ${failed ? "failed" : "succeeded"}`,
      detail,
      at: attempt.started_at,
      tone: failed ? "danger" : "ok",
    });
  });

  steps.push(terminalStep(n, ordered.length));
  return steps;
}

/** The final step, describing where the notification actually stands. */
function terminalStep(n: Notification, attemptCount: number): Step {
  switch (n.status) {
    case "delivered":
      return {
        key: "delivered",
        label: "Delivered",
        detail:
          "A provider accepted it. At-least-once delivery means this could in principle have been sent twice; the idempotency key is what stops a retried request from becoming a second notification.",
        at: n.updated_at,
        tone: "ok",
      };

    case "dead_lettered":
      return {
        key: "dead_lettered",
        label: "Dead-lettered",
        detail: `Retries ran out after ${n.max_attempts} attempts. It is off the queue and will not be tried again — it is kept so a human can decide what to do, rather than being dropped.`,
        at: n.updated_at,
        tone: "danger",
      };

    case "failed":
      return {
        key: "retrying",
        label: "Waiting to retry",
        detail: `${n.attempts} of ${n.max_attempts} attempts used. It is back in the sorted set with a backoff delay, and will return to the queue when that expires.`,
        at: n.scheduled_at,
        tone: "warn",
        extra:
          new Date(n.scheduled_at) > new Date() ? (
            <div className="mt-2">
              <Countdown to={n.scheduled_at} />
            </div>
          ) : undefined,
      };

    case "delivering":
      return {
        key: "delivering",
        label: "Delivering",
        detail:
          "A worker has claimed this and is calling the provider now. If that worker dies mid-flight, the claim expires and another worker reclaims it.",
        tone: "info",
      };

    default:
      return {
        key: "waiting",
        label: attemptCount === 0 ? "No attempt yet" : "Waiting",
        detail:
          "Nothing has picked this up. It is either not due, or waiting for a free worker.",
        tone: "neutral",
      };
  }
}

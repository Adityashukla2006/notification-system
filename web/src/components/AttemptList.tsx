import { absoluteTime, duration, relativeTime } from "@/lib/format";
import type { Attempt } from "@/lib/types";

/**
 * Delivery history, newest attempt first.
 *
 * This is the view that answers the only question worth asking when something
 * did not arrive: what was tried, when, and what came back. The provider's own
 * error text is shown verbatim rather than summarised — the exact wording is
 * usually what identifies the problem.
 */
export function AttemptList({ attempts }: { attempts: Attempt[] }) {
  return (
    <section className="rounded-lg border border-border-subtle bg-surface">
      <h2 className="flex items-baseline justify-between border-b border-border-subtle px-4 py-3">
        <span className="text-sm font-medium">Delivery attempts</span>
        <span className="text-xs text-muted tabular">
          {attempts.length} recorded
        </span>
      </h2>

      {attempts.length === 0 ? (
        <p className="px-4 py-8 text-center text-sm text-muted">
          No attempts yet. The worker has not picked this up — it may be
          scheduled for later, or waiting on a retry.
        </p>
      ) : (
        <ol>
          {attempts.map((attempt) => (
            <AttemptRow key={attempt.id} attempt={attempt} />
          ))}
        </ol>
      )}
    </section>
  );
}

function AttemptRow({ attempt }: { attempt: Attempt }) {
  const failed = attempt.outcome === "failed";

  return (
    <li className="border-b border-border-subtle px-4 py-3 last:border-0">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <span
          className={`inline-flex h-6 w-6 items-center justify-center rounded-full text-xs font-medium tabular ${
            failed ? "bg-danger-bg text-danger-fg" : "bg-ok-bg text-ok-fg"
          }`}
          title={`Attempt ${attempt.attempt_number}`}
        >
          {attempt.attempt_number}
        </span>

        <span
          className={`text-sm font-medium ${failed ? "text-danger-fg" : "text-ok-fg"}`}
        >
          {failed ? "Failed" : "Succeeded"}
        </span>

        <span
          className="text-sm text-muted"
          title={absoluteTime(attempt.started_at)}
        >
          {relativeTime(attempt.started_at)}
        </span>

        <span className="text-sm text-muted tabular">
          took {duration(attempt.duration_ms)}
        </span>
      </div>

      {attempt.error && (
        <p className="mt-2 rounded-md bg-danger-bg px-3 py-2 font-mono text-xs leading-relaxed break-words text-danger-fg">
          {attempt.error}
        </p>
      )}
    </li>
  );
}

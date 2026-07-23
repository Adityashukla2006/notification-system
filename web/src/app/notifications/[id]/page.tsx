import Link from "next/link";
import { notFound } from "next/navigation";

import { ApiErrorState } from "@/components/ApiErrorState";
import { AttemptList } from "@/components/AttemptList";
import { StatusBadge } from "@/components/StatusBadge";
import { ApiError, getNotification, listAttempts } from "@/lib/api";
import {
  absoluteTime,
  formatPayload,
  relativeTime,
  shortId,
} from "@/lib/format";
import type { Attempt, Notification } from "@/lib/types";

type Params = Promise<{ id: string }>;

export async function generateMetadata({ params }: { params: Params }) {
  const { id } = await params;
  return { title: `Notification ${shortId(id)}` };
}

export default async function NotificationDetailPage({
  params,
}: {
  params: Params;
}) {
  const { id } = await params;

  let notification: Notification;
  try {
    notification = await getNotification(id);
  } catch (error) {
    // A notification belonging to another client also returns 404, by design —
    // distinguishing "not yours" from "does not exist" would let someone probe
    // for real ids.
    if (error instanceof ApiError && error.status === 404) notFound();
    return <ApiErrorState error={error} />;
  }

  // Attempts are fetched after the notification, because the notification
  // lookup is what establishes the caller may see this data at all.
  let attempts: Attempt[];
  try {
    attempts = await listAttempts(id);
  } catch (error) {
    return <ApiErrorState error={error} />;
  }

  return (
    <div className="space-y-6">
      <div>
        <Link
          href="/notifications"
          className="text-sm text-muted hover:text-foreground"
        >
          ← All notifications
        </Link>
      </div>

      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="space-y-1">
          <h1 className="font-mono text-lg font-semibold tracking-tight">
            {notification.id}
          </h1>
          <p className="text-sm text-muted">
            Created {relativeTime(notification.created_at)} ·{" "}
            {absoluteTime(notification.created_at)}
          </p>
        </div>
        <StatusBadge status={notification.status} />
      </div>

      <section className="rounded-lg border border-border-subtle bg-surface">
        <h2 className="border-b border-border-subtle px-4 py-3 text-sm font-medium">
          Details
        </h2>
        <dl className="grid grid-cols-1 gap-px bg-border-subtle sm:grid-cols-2">
          <Field label="Channel">{notification.channel}</Field>
          <Field label="Recipient">
            <span className="break-all">{notification.recipient}</span>
          </Field>
          <Field label="Attempts">
            <span className="tabular">
              {notification.attempts} of {notification.max_attempts}
            </span>
            {notification.attempts === 0 && attempts.length > 0 && (
              <span className="ml-2 text-xs text-muted">
                (counter tracks failures)
              </span>
            )}
          </Field>
          <Field label="Scheduled for">
            <span title={absoluteTime(notification.scheduled_at)}>
              {absoluteTime(notification.scheduled_at)}
            </span>
          </Field>
          <Field label="Idempotency key">
            <span className="font-mono text-xs break-all">
              {notification.idempotency_key}
            </span>
          </Field>
          <Field label="Last updated">
            <span title={absoluteTime(notification.updated_at)}>
              {relativeTime(notification.updated_at)}
            </span>
          </Field>
        </dl>
      </section>

      <section className="rounded-lg border border-border-subtle bg-surface">
        <h2 className="border-b border-border-subtle px-4 py-3 text-sm font-medium">
          Payload
        </h2>
        <pre className="overflow-x-auto px-4 py-3 font-mono text-xs leading-relaxed">
          {formatPayload(notification.payload)}
        </pre>
      </section>

      <AttemptList attempts={attempts} />
    </div>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="bg-surface px-4 py-3">
      <dt className="text-xs tracking-wide text-muted uppercase">{label}</dt>
      <dd className="mt-1 text-sm">{children}</dd>
    </div>
  );
}

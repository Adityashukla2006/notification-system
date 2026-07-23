import Link from "next/link";
import { Suspense } from "react";

import { ApiErrorState } from "@/components/ApiErrorState";
import { Filters } from "@/components/Filters";
import { Pagination, parseTrail } from "@/components/Pagination";
import { StatusBadge } from "@/components/StatusBadge";
import { listNotifications } from "@/lib/api";
import { relativeTime, absoluteTime, shortId } from "@/lib/format";
import type { Notification, NotificationPage } from "@/lib/types";

export const metadata = {
  title: "Notifications",
};

type SearchParams = Promise<{ [key: string]: string | string[] | undefined }>;

/** Reads a single-valued search parameter, ignoring repeated keys. */
function one(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export default async function NotificationsPage({
  searchParams,
}: {
  searchParams: SearchParams;
}) {
  const params = await searchParams;
  const status = one(params.status);
  const channel = one(params.channel);
  const cursor = one(params.cursor);
  const trail = parseTrail(one(params.trail));

  // Caught here rather than left to the error boundary: in production Next
  // sanitises the message before it reaches the client, so only this side can
  // tell an unreachable API from a rejected key and say which it was.
  let page: NotificationPage;
  try {
    page = await listNotifications({ status, channel, cursor, limit: 25 });
  } catch (error) {
    return <ApiErrorState error={error} />;
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">Notifications</h1>
          <p className="mt-1 text-sm text-muted">
            Newest first. Select one to see its delivery history.
          </p>
        </div>
        <Suspense fallback={null}>
          <Filters />
        </Suspense>
      </div>

      <div className="overflow-hidden rounded-lg border border-border-subtle bg-surface">
        {page.data.length === 0 ? (
          <EmptyState filtered={Boolean(status || channel)} />
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border-subtle bg-surface-muted text-left text-xs tracking-wide text-muted uppercase">
                    <Th>Status</Th>
                    <Th>Channel</Th>
                    <Th>Recipient</Th>
                    <Th>Attempts</Th>
                    <Th>Created</Th>
                    <Th>Id</Th>
                  </tr>
                </thead>
                <tbody>
                  {page.data.map((n) => (
                    <Row key={n.id} notification={n} />
                  ))}
                </tbody>
              </table>
            </div>

            <Pagination
              params={{ status, channel }}
              cursor={cursor}
              trail={trail}
              nextCursor={page.next_cursor}
              count={page.data.length}
            />
          </>
        )}
      </div>
    </div>
  );
}

function Th({ children }: { children: React.ReactNode }) {
  return <th className="px-4 py-2.5 font-medium">{children}</th>;
}

function Row({ notification }: { notification: Notification }) {
  const n = notification;
  // The whole row is a link target, but only the id cell carries the anchor —
  // nesting interactive elements inside a row-wide link breaks keyboard use.
  return (
    <tr className="border-b border-border-subtle last:border-0 hover:bg-surface-muted">
      <td className="px-4 py-2.5">
        <StatusBadge status={n.status} />
      </td>
      <td className="px-4 py-2.5 text-muted">{n.channel}</td>
      <td className="max-w-xs truncate px-4 py-2.5" title={n.recipient}>
        {n.recipient}
      </td>
      <td className="px-4 py-2.5 tabular text-muted">
        {n.attempts}
        <span className="opacity-50"> / {n.max_attempts}</span>
      </td>
      <td
        className="px-4 py-2.5 whitespace-nowrap text-muted"
        title={absoluteTime(n.created_at)}
      >
        {relativeTime(n.created_at)}
      </td>
      <td className="px-4 py-2.5">
        <Link
          href={`/notifications/${n.id}`}
          className="font-mono text-xs text-accent hover:underline"
        >
          {shortId(n.id)}
        </Link>
      </td>
    </tr>
  );
}

function EmptyState({ filtered }: { filtered: boolean }) {
  return (
    <div className="px-6 py-16 text-center">
      <p className="text-sm font-medium">No notifications</p>
      <p className="mx-auto mt-1 max-w-md text-sm text-muted">
        {filtered
          ? "Nothing matches these filters. Try clearing them."
          : "Nothing has been accepted yet. POST to /v1/notifications and it will appear here."}
      </p>
    </div>
  );
}

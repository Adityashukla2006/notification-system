import Link from "next/link";

/**
 * Cursor pagination controls.
 *
 * A cursor only points forward — it says "everything older than this id" — so
 * going back needs the cursors already visited. They are kept in a `trail`
 * parameter: Next pushes the current cursor onto it, Previous pops it. Keeping
 * the trail in the URL (rather than component state) means a deep page can be
 * shared or reloaded and still knows its way back.
 *
 * These are plain links, so pagination works before any JavaScript loads.
 */
export function Pagination({
  params,
  cursor,
  trail,
  nextCursor,
  count,
}: {
  params: Record<string, string | undefined>;
  cursor?: string;
  trail: string[];
  nextCursor: string | null;
  count: number;
}) {
  const page = trail.length + (cursor ? 1 : 0) + 1;

  const nextHref = nextCursor
    ? buildHref(params, {
        cursor: nextCursor,
        trail: cursor ? [...trail, cursor] : trail,
      })
    : null;

  const previousTrail = trail.slice(0, -1);
  const previousCursor = trail[trail.length - 1];
  const previousHref = cursor
    ? buildHref(params, { cursor: previousCursor, trail: previousTrail })
    : null;

  return (
    <div className="flex items-center justify-between border-t border-border-subtle px-4 py-3 text-sm">
      <span className="text-muted tabular">
        Page {page} · {count} {count === 1 ? "notification" : "notifications"}
      </span>

      <div className="flex items-center gap-2">
        <PageLink href={previousHref}>← Previous</PageLink>
        <PageLink href={nextHref}>Next →</PageLink>
      </div>
    </div>
  );
}

function PageLink({
  href,
  children,
}: {
  href: string | null;
  children: React.ReactNode;
}) {
  if (!href) {
    return (
      <span className="cursor-not-allowed rounded-md border border-border-subtle px-3 py-1.5 text-muted opacity-50">
        {children}
      </span>
    );
  }
  return (
    <Link
      href={href}
      className="rounded-md border border-border-subtle bg-surface px-3 py-1.5 text-foreground transition-colors hover:border-accent hover:text-accent"
    >
      {children}
    </Link>
  );
}

/** Builds a URL preserving filters while replacing the pagination position. */
function buildHref(
  params: Record<string, string | undefined>,
  position: { cursor?: string; trail: string[] },
): string {
  const next = new URLSearchParams();
  if (params.status) next.set("status", params.status);
  if (params.channel) next.set("channel", params.channel);
  if (position.cursor) next.set("cursor", position.cursor);
  if (position.trail.length) next.set("trail", position.trail.join(","));

  return next.size ? `/notifications?${next}` : "/notifications";
}

/** Parses the trail parameter into the list of cursors already visited. */
export function parseTrail(raw?: string): string[] {
  if (!raw) return [];
  return raw.split(",").filter(Boolean);
}

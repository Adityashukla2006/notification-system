/**
 * Skeleton shown while the list loads.
 *
 * It mirrors the real table's shape so the layout does not jump when data
 * arrives — a shifting page is harder to read than a still one.
 */
export default function Loading() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <div className="h-6 w-40 animate-pulse rounded bg-surface-muted" />
        <div className="h-4 w-64 animate-pulse rounded bg-surface-muted" />
      </div>

      <div className="overflow-hidden rounded-lg border border-border-subtle bg-surface">
        <div className="h-10 border-b border-border-subtle bg-surface-muted" />
        {Array.from({ length: 8 }).map((_, i) => (
          <div
            key={i}
            className="flex items-center gap-4 border-b border-border-subtle px-4 py-3 last:border-0"
          >
            <div className="h-5 w-20 animate-pulse rounded-full bg-surface-muted" />
            <div className="h-4 w-16 animate-pulse rounded bg-surface-muted" />
            <div className="h-4 flex-1 animate-pulse rounded bg-surface-muted" />
            <div className="h-4 w-12 animate-pulse rounded bg-surface-muted" />
          </div>
        ))}
      </div>
    </div>
  );
}

"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useTransition } from "react";

import { CHANNELS, STATUSES } from "@/lib/types";
import { statusLabel } from "@/lib/format";

/**
 * Status and channel filters.
 *
 * Filter state lives in the URL rather than component state, so a filtered
 * view can be bookmarked, shared with a colleague, and survives a reload —
 * which is exactly what someone does when they find something broken.
 *
 * Changing a filter resets pagination: a cursor names a position in the
 * previous result set, so carrying it into a different filter would land on a
 * meaningless page.
 */
export function Filters() {
  const router = useRouter();
  const params = useSearchParams();
  const [isPending, startTransition] = useTransition();

  const status = params.get("status") ?? "";
  const channel = params.get("channel") ?? "";

  function apply(key: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) {
      next.set(key, value);
    } else {
      next.delete(key);
    }
    next.delete("cursor");
    next.delete("trail");

    startTransition(() => {
      router.push(next.size ? `/notifications?${next}` : "/notifications");
    });
  }

  const hasFilters = Boolean(status || channel);

  return (
    <div
      className={`flex flex-wrap items-center gap-3 ${isPending ? "opacity-60" : ""}`}
    >
      <Select
        label="Status"
        value={status}
        onChange={(v) => apply("status", v)}
        options={STATUSES.map((s) => ({ value: s, label: statusLabel(s) }))}
      />
      <Select
        label="Channel"
        value={channel}
        onChange={(v) => apply("channel", v)}
        options={CHANNELS.map((c) => ({ value: c, label: c }))}
      />

      {hasFilters && (
        <button
          type="button"
          onClick={() => startTransition(() => router.push("/notifications"))}
          className="text-sm text-muted underline underline-offset-4 hover:text-foreground"
        >
          Clear
        </button>
      )}
    </div>
  );
}

function Select({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  options: { value: string; label: string }[];
}) {
  return (
    <label className="flex items-center gap-2 text-sm">
      <span className="text-muted">{label}</span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="rounded-md border border-border-subtle bg-surface px-2 py-1.5 text-sm text-foreground focus:border-accent focus:outline-none"
      >
        <option value="">All</option>
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </label>
  );
}

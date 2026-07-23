/** Formatting helpers shared across the dashboard. */

/**
 * Renders a timestamp as a short relative age ("3m ago").
 *
 * Operators reading a delivery dashboard care about "how long ago" far more
 * than the wall-clock time, and a relative age is readable at a glance in a
 * dense table.
 */
export function relativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "—";

  const seconds = Math.round((Date.now() - then) / 1000);
  const future = seconds < 0;
  const abs = Math.abs(seconds);

  const [value, unit] = pickUnit(abs);
  if (value === 0) return "just now";

  return future ? `in ${value}${unit}` : `${value}${unit} ago`;
}

function pickUnit(seconds: number): [number, string] {
  if (seconds < 60) return [seconds, "s"];
  if (seconds < 3600) return [Math.floor(seconds / 60), "m"];
  if (seconds < 86_400) return [Math.floor(seconds / 3600), "h"];
  return [Math.floor(seconds / 86_400), "d"];
}

/** Renders a full timestamp for tooltips and detail views. */
export function absoluteTime(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  });
}

/** Renders a millisecond duration compactly. */
export function duration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)}s`;
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
}

/** Shortens a UUID for dense display, keeping it recognisable. */
export function shortId(id: string): string {
  return id.slice(0, 8);
}

/** Pretty-prints a payload, tolerating anything the API returns. */
export function formatPayload(payload: unknown): string {
  try {
    return JSON.stringify(payload, null, 2);
  } catch {
    return String(payload);
  }
}

/** Turns a status into human-facing words. */
export function statusLabel(status: string): string {
  return status.replace(/_/g, " ");
}

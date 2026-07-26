"use client";

import { useEffect, useState } from "react";

/**
 * A ticking countdown to a scheduled send.
 *
 * This is the only element on the page that must not be rendered on the
 * server: the remaining time is different by the moment the HTML arrives, and
 * a server-rendered value would hydrate to a mismatch. It starts blank and
 * fills in after mount, which is why the first paint shows a dash.
 */
export function Countdown({ to }: { to: string }) {
  const target = new Date(to).getTime();
  const [remaining, setRemaining] = useState<number | null>(null);

  useEffect(() => {
    if (Number.isNaN(target)) return;

    function tick() {
      setRemaining(Math.max(0, target - Date.now()));
    }

    tick();
    const timer = setInterval(tick, 1000);
    return () => clearInterval(timer);
  }, [target]);

  const done = remaining !== null && remaining <= 0;

  return (
    <span
      className={`inline-flex items-center gap-2 rounded-md px-2.5 py-1 text-sm tabular ${
        done ? "bg-info-bg text-info-fg" : "bg-warn-bg text-warn-fg"
      }`}
      // Announced once it changes meaningfully rather than every second.
      aria-live="off"
    >
      {done ? "Due now — waiting for a worker" : `Sends in ${format(remaining)}`}
    </span>
  );
}

/** Formats milliseconds as a clock-style countdown. */
function format(ms: number | null): string {
  if (ms === null) return "—";

  const total = Math.ceil(ms / 1000);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;

  const pad = (n: number) => String(n).padStart(2, "0");
  return hours > 0
    ? `${hours}:${pad(minutes)}:${pad(seconds)}`
    : `${minutes}:${pad(seconds)}`;
}

"use client";

import { useRouter } from "next/navigation";
import { useCallback, useEffect, useState, useTransition } from "react";

/**
 * Polls the server so the page tracks delivery as it happens.
 *
 * Polling rather than a stream: the API is a plain request/response service
 * with no push channel, and adding SSE or websockets to watch a handful of
 * rows would be far more machinery than the problem needs. At FAANG scale the
 * answer is usually the opposite — a change-data-capture stream feeding a
 * push channel — because polling cost grows with viewers times rows.
 *
 * `router.refresh()` re-runs the Server Component and reconciles the result
 * into the existing tree, so the table updates without a navigation, without
 * losing scroll position, and without clearing form state.
 *
 * Refreshing stops while the tab is hidden. A background tab is nobody's live
 * view, and it would otherwise poll the API forever.
 */
export function AutoRefresh({
  intervalMs = 3000,
  /** When true, polling starts switched off. */
  defaultPaused = false,
}: {
  intervalMs?: number;
  defaultPaused?: boolean;
}) {
  const router = useRouter();
  const [paused, setPaused] = useState(defaultPaused);
  const [isPending, startTransition] = useTransition();
  const [lastRefresh, setLastRefresh] = useState<number | null>(null);

  const refresh = useCallback(() => {
    startTransition(() => {
      router.refresh();
      setLastRefresh(Date.now());
    });
  }, [router]);

  useEffect(() => {
    if (paused) return;

    let timer: ReturnType<typeof setInterval> | undefined;

    function start() {
      if (timer === undefined) timer = setInterval(refresh, intervalMs);
    }
    function stop() {
      clearInterval(timer);
      timer = undefined;
    }
    function onVisibilityChange() {
      if (document.hidden) {
        stop();
      } else {
        // Catch up immediately: whatever happened while the tab was hidden is
        // exactly what the viewer came back to see.
        refresh();
        start();
      }
    }

    if (!document.hidden) start();
    document.addEventListener("visibilitychange", onVisibilityChange);

    return () => {
      stop();
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [paused, intervalMs, refresh]);

  return (
    <div className="flex items-center gap-3 text-sm">
      <button
        type="button"
        onClick={() => setPaused((p) => !p)}
        aria-pressed={!paused}
        className="inline-flex items-center gap-2 rounded-md border border-border-subtle px-2.5 py-1.5 text-muted hover:text-foreground"
      >
        <span
          aria-hidden
          className={`h-2 w-2 rounded-full ${
            paused
              ? "bg-muted"
              : `bg-ok-fg ${isPending ? "opacity-40" : "animate-pulse"}`
          }`}
        />
        {paused ? "Paused" : "Live"}
      </button>

      <button
        type="button"
        onClick={refresh}
        className="text-muted underline underline-offset-4 hover:text-foreground"
      >
        Refresh now
      </button>

      {lastRefresh !== null && (
        <span className="text-xs text-muted tabular">
          updated{" "}
          {new Date(lastRefresh).toLocaleTimeString(undefined, {
            hour12: false,
          })}
        </span>
      )}
    </div>
  );
}

"use client";

import { useEffect } from "react";

/**
 * Error boundary for the dashboard.
 *
 * The two failures that actually happen in practice — the API is not running,
 * and the API key is missing or wrong — are both operator mistakes with
 * specific fixes, so they get specific instructions rather than a generic
 * "something went wrong".
 *
 * Error messages are sanitised by Next before reaching the client in
 * production, so this matches on the shape of the message rather than
 * expecting full detail.
 */
export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error(error);
  }, [error]);

  const message = error.message ?? "";
  const unreachable = /could not reach the api/i.test(message);
  const auth = /api_key|unauthorized|401/i.test(message);

  return (
    <div className="mx-auto max-w-lg rounded-lg border border-border-subtle bg-surface p-8">
      <h1 className="text-base font-semibold">
        {unreachable
          ? "The API is not reachable"
          : auth
            ? "The dashboard is not authorised"
            : "Something went wrong"}
      </h1>

      <p className="mt-2 text-sm text-muted">
        {unreachable ? (
          <>
            Start the API and a worker, then try again:
            <code className="mt-2 block rounded bg-surface-muted px-3 py-2 font-mono text-xs">
              cd api && go run ./cmd/server
            </code>
          </>
        ) : auth ? (
          <>
            Set <code className="font-mono">API_KEY</code> in{" "}
            <code className="font-mono">web/.env.local</code>. Mint one with:
            <code className="mt-2 block rounded bg-surface-muted px-3 py-2 font-mono text-xs">
              go run ./cmd/keygen -client-name local -name dashboard
            </code>
          </>
        ) : (
          message || "An unexpected error occurred."
        )}
      </p>

      <button
        type="button"
        onClick={reset}
        className="mt-6 rounded-md border border-border-subtle bg-surface px-3 py-1.5 text-sm transition-colors hover:border-accent hover:text-accent"
      >
        Try again
      </button>
    </div>
  );
}

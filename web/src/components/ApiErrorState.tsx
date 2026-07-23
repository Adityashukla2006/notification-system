import { ApiError, ApiUnreachableError, apiBaseUrl } from "@/lib/api";

/**
 * Renders an API failure with the command that fixes it.
 *
 * This is rendered by the Server Component that made the call, not by the
 * error boundary, and that distinction matters: Next sanitises error messages
 * before they cross to the client in production, so a boundary only ever sees
 * an opaque digest. Here the real error is still in hand, so the two failures
 * that actually happen — the API is not running, and the key is missing or
 * wrong — can be named and fixed rather than reported as "something went
 * wrong".
 */
export function ApiErrorState({ error }: { error: unknown }) {
  const { title, body, command } = describe(error);

  return (
    <div className="mx-auto max-w-lg rounded-lg border border-border-subtle bg-surface p-8">
      <h1 className="text-base font-semibold">{title}</h1>
      <p className="mt-2 text-sm text-muted">{body}</p>
      {command && (
        <code className="mt-4 block overflow-x-auto rounded bg-surface-muted px-3 py-2 font-mono text-xs whitespace-pre">
          {command}
        </code>
      )}
    </div>
  );
}

function describe(error: unknown): {
  title: string;
  body: string;
  command?: string;
} {
  if (error instanceof ApiUnreachableError) {
    return {
      title: "The API is not reachable",
      body: `Nothing is answering at ${apiBaseUrl()}. Start the API and a worker, then reload.`,
      command: "cd api && go run ./cmd/server\ncd api && go run ./cmd/worker",
    };
  }

  if (error instanceof ApiError && error.isAuthFailure) {
    return {
      title: "The dashboard is not authorised",
      body: "The API rejected this key. Set API_KEY in web/.env.local to a valid key, then reload.",
      command:
        "cd api && go run ./cmd/keygen -client-name local -name dashboard",
    };
  }

  if (error instanceof ApiError) {
    return {
      title: `The API returned ${error.status}`,
      body: error.message,
    };
  }

  return {
    title: "Something went wrong",
    body: error instanceof Error ? error.message : "An unexpected error occurred.",
  };
}

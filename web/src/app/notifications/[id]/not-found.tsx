import Link from "next/link";

export default function NotFound() {
  return (
    <div className="mx-auto max-w-lg rounded-lg border border-border-subtle bg-surface p-8 text-center">
      <h1 className="text-base font-semibold">Notification not found</h1>
      <p className="mt-2 text-sm text-muted">
        No notification with that id belongs to this client. It may have been
        deleted, or the id may be mistyped.
      </p>
      <Link
        href="/notifications"
        className="mt-6 inline-block rounded-md border border-border-subtle px-3 py-1.5 text-sm transition-colors hover:border-accent hover:text-accent"
      >
        Back to all notifications
      </Link>
    </div>
  );
}

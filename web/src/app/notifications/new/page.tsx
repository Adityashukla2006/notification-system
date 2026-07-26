import Link from "next/link";

import { CreateNotificationForm } from "@/components/CreateNotificationForm";

export const metadata = {
  title: "New notification",
};

export default function NewNotificationPage() {
  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <div>
        <Link
          href="/notifications"
          className="text-sm text-muted hover:text-foreground"
        >
          ← All notifications
        </Link>
      </div>

      <div>
        <h1 className="text-xl font-semibold tracking-tight">
          New notification
        </h1>
        <p className="mt-1 text-sm text-muted">
          This submits through the same API a real client would call — the
          dashboard has no shortcut into the database or the queue.
        </p>
      </div>

      <CreateNotificationForm />
    </div>
  );
}

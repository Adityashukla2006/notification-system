import type { Status } from "@/lib/types";
import { statusLabel } from "@/lib/format";

/**
 * Colour carries meaning here, so it follows the lifecycle rather than
 * aesthetics: green only for a genuine success, red for the two states that
 * need a human, amber for in-flight work, and grey for "nothing has happened
 * yet". Text always accompanies it — colour alone is unreadable for anyone
 * with a colour vision deficiency.
 */
const STYLES: Record<Status, string> = {
  delivered: "bg-ok-bg text-ok-fg",
  dead_lettered: "bg-danger-bg text-danger-fg",
  failed: "bg-danger-bg text-danger-fg",
  delivering: "bg-info-bg text-info-fg",
  queued: "bg-warn-bg text-warn-fg",
  pending: "bg-neutral-bg text-neutral-fg",
};

export function StatusBadge({ status }: { status: Status }) {
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium whitespace-nowrap ${
        STYLES[status] ?? "bg-neutral-bg text-neutral-fg"
      }`}
    >
      {statusLabel(status)}
    </span>
  );
}

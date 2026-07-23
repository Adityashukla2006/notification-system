import { redirect } from "next/navigation";

/** The dashboard has one entry point; the root simply forwards to it. */
export default function Home() {
  redirect("/notifications");
}

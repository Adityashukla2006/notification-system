import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import Link from "next/link";

import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Notifications",
  description: "Delivery dashboard for the notification system",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body className="flex min-h-full flex-col font-sans">
        <header className="border-b border-border-subtle bg-surface">
          <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4">
            <Link href="/notifications" className="flex items-baseline gap-3">
              <span className="text-lg font-semibold tracking-tight">
                Notifications
              </span>
              <span className="text-sm text-muted">delivery dashboard</span>
            </Link>
            {/* The dashboard is read-only by design: it observes the system,
                it never sends. Sending is the API's job. */}
            <span className="rounded-full bg-surface-muted px-2.5 py-1 text-xs text-muted">
              read-only
            </span>
          </div>
        </header>

        <main className="mx-auto w-full max-w-6xl flex-1 px-6 py-8">
          {children}
        </main>
      </body>
    </html>
  );
}

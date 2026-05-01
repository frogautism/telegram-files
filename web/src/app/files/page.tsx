"use client";
import Files from "@/components/files";
import ThemeToggleButton from "@/components/theme-toggle-button";
import Link from "next/link";

export default function LibraryPage() {
  return (
    <div className="app-shell px-4 py-4 md:px-6 md:py-6">
      <header className="sticky top-0 z-30 -mx-4 mb-6 border-b border-border/80 bg-background/85 px-4 backdrop-blur-md md:-mx-6 md:px-6">
        <div className="flex h-14 items-center justify-between gap-3">
          <Link href="/" className="inline-flex items-center gap-2.5">
            <span
              aria-hidden="true"
              className="relative inline-flex h-8 w-8 items-center justify-center overflow-hidden rounded-md border border-border bg-card shadow-card"
            >
              <span className="absolute inset-0 bg-[radial-gradient(120%_120%_at_0%_0%,hsl(var(--brand)/0.18),transparent_55%)]" />
              <span className="relative font-display text-[18px] leading-none tracking-tight text-foreground">
                T
              </span>
              <span className="absolute -bottom-0.5 right-1 h-1 w-1 rounded-full bg-brand" />
            </span>
            <div className="flex items-baseline gap-2">
              <span className="font-display text-xl leading-none tracking-tight">
                Library
              </span>
              <span className="hidden text-[10px] uppercase tracking-[0.18em] text-muted-foreground sm:inline">
                · all accounts
              </span>
            </div>
          </Link>

          <ThemeToggleButton />
        </div>
      </header>
      <Files accountId="-1" chatId="-1" />
    </div>
  );
}

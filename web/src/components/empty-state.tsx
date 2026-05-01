import {
  AlertTriangle,
  ArrowRight,
  ArrowUpRight,
  Check,
  Download,
  HardDrive,
  ImageIcon,
  MessageSquare,
  Sparkles,
  UserPlus,
} from "lucide-react";
import { AccountList } from "./account-list";
import { type TelegramAccount } from "@/lib/types";
import { AccountDialog } from "@/components/account-dialog";
import React from "react";
import { Button } from "@/components/ui/button";
import useSWR from "swr";
import prettyBytes from "pretty-bytes";
import { useRouter } from "next/navigation";
import { Skeleton } from "@/components/ui/skeleton";
import ThemeToggleButton from "@/components/theme-toggle-button";
import Link from "next/link";

interface EmptyStateProps {
  isLoadingAccount?: boolean;
  hasAccounts: boolean;
  accounts?: TelegramAccount[];
  message?: string;
  onSelectAccount?: (accountId: string) => void;
}

export function EmptyState({
  isLoadingAccount,
  hasAccounts,
  accounts = [],
  message,
  onSelectAccount,
}: EmptyStateProps) {
  if (message) {
    return (
      <div className="flex w-full max-w-md flex-col items-center gap-5 rounded-xl border border-border bg-card p-10 text-center shadow-card">
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
          <MessageSquare className="h-5 w-5" strokeWidth={1.5} />
        </div>
        <div className="space-y-1.5">
          <h2 className="font-display text-2xl leading-tight tracking-tight">
            {message}
          </h2>
          <p className="text-sm text-muted-foreground">
            Pick a chat from the sidebar to start browsing media.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="relative min-h-screen overflow-hidden bg-background">
      <BackgroundOrnament />

      <div className="app-shell relative px-5 py-6 md:px-8 md:py-8">
        <div className="mb-12 flex items-center justify-between md:mb-16">
          <Link href="/" className="inline-flex items-center gap-2.5">
            <span
              aria-hidden="true"
              className="relative inline-flex h-9 w-9 items-center justify-center overflow-hidden rounded-md border border-border bg-card shadow-card"
            >
              <span className="absolute inset-0 bg-[radial-gradient(120%_120%_at_0%_0%,hsl(var(--brand)/0.18),transparent_55%)]" />
              <span className="relative font-display text-[20px] leading-none tracking-tight">
                T
              </span>
              <span className="absolute -bottom-0.5 right-1 h-1 w-1 rounded-full bg-brand" />
            </span>
            <div className="flex items-baseline gap-2">
              <span className="font-display text-xl leading-none tracking-tight">
                TeleFiles
              </span>
              <span className="hidden text-[10px] uppercase tracking-[0.18em] text-muted-foreground sm:inline">
                · vault
              </span>
            </div>
          </Link>
          <div className="flex items-center gap-2">
            <span className="hidden text-[11px] text-muted-foreground sm:inline">
              v0.3 · open beta
            </span>
            <ThemeToggleButton />
          </div>
        </div>

        <section className="grid gap-10 lg:grid-cols-[1.2fr_1fr] lg:items-center lg:gap-16">
          <div className="space-y-8">
            <div className="inline-flex items-center gap-2 rounded-full border border-border bg-card px-3 py-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground shadow-card">
              <span className="h-1.5 w-1.5 rounded-full bg-brand" />
              A quiet vault for the things you save
            </div>

            <h1 className="font-display text-balance text-5xl leading-[1.05] tracking-tight md:text-6xl lg:text-7xl">
              Every photo, video,
              <br />
              <span className="italic text-muted-foreground">
                and file you saved —
              </span>
              <br />
              one beautiful library.
            </h1>

            <p className="max-w-lg text-balance text-base leading-relaxed text-muted-foreground md:text-lg">
              TeleFiles is a content-first, keyboard-fast home for your Telegram
              media. Connect an account, pick a chat, and every download lives
              in a calm, browsable archive — yours, on your machine.
            </p>

            <div className="flex flex-wrap items-center gap-3">
              <AccountDialog isAdd={true}>
                <Button size="lg" variant="default" className="gap-2">
                  <UserPlus className="h-4 w-4" />
                  Connect Telegram
                  <ArrowRight className="h-3.5 w-3.5 opacity-60" />
                </Button>
              </AccountDialog>
              {hasAccounts && (
                <span className="text-sm text-muted-foreground">
                  or pick an existing account below
                </span>
              )}
            </div>

            <FeatureRow />
          </div>

          <LibraryPreview isLoading={!!isLoadingAccount} />
        </section>

        {hasAccounts && accounts.length > 0 && onSelectAccount && (
          <section className="mt-20 space-y-5">
            <div className="flex items-end justify-between">
              <div className="space-y-1">
                <h2 className="font-display text-2xl tracking-tight md:text-3xl">
                  Your accounts
                </h2>
                <p className="text-sm text-muted-foreground">
                  Tap to enter the vault.
                </p>
              </div>
              <AccountDialog isAdd={true}>
                <Button variant="ghost" size="sm" className="gap-1.5">
                  <UserPlus className="h-3.5 w-3.5" />
                  Add account
                </Button>
              </AccountDialog>
            </div>
            <AccountList accounts={accounts} onSelectAccount={onSelectAccount} />
          </section>
        )}

        <footer className="mt-24 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-6 text-xs text-muted-foreground">
          <span>Self-hosted · no telemetry · your media stays local.</span>
          <span className="font-mono">TeleFiles · 0.3.0</span>
        </footer>
      </div>
    </div>
  );
}

function BackgroundOrnament() {
  return (
    <>
      <div
        aria-hidden
        className="pointer-events-none absolute -top-40 right-[-10%] h-[520px] w-[520px] rounded-full opacity-50 blur-3xl"
        style={{
          background:
            "radial-gradient(closest-side, hsl(var(--brand) / 0.18), transparent 70%)",
        }}
      />
      <div
        aria-hidden
        className="pointer-events-none absolute left-[-15%] top-[30%] h-[420px] w-[420px] rounded-full opacity-40 blur-3xl"
        style={{
          background:
            "radial-gradient(closest-side, hsl(var(--chart-4) / 0.12), transparent 70%)",
        }}
      />
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 opacity-[0.035] mix-blend-multiply dark:opacity-[0.06] dark:mix-blend-normal"
        style={{
          backgroundImage:
            "radial-gradient(hsl(var(--foreground)) 0.5px, transparent 0.5px)",
          backgroundSize: "20px 20px",
        }}
      />
    </>
  );
}

function FeatureRow() {
  const items = [
    {
      icon: Sparkles,
      label: "Image-first browsing",
      hint: "Calm grid, density toggle, keyboard nav.",
    },
    {
      icon: Download,
      label: "Live + offline",
      hint: "Search Telegram or your own archive.",
    },
    {
      icon: HardDrive,
      label: "Yours, locally",
      hint: "Files stay on disk. No middleman.",
    },
  ];
  return (
    <div className="grid gap-3 sm:grid-cols-3">
      {items.map(({ icon: Icon, label, hint }) => (
        <div
          key={label}
          className="rounded-lg border border-border bg-card/60 p-3 backdrop-blur-sm transition-colors hover:border-border-strong"
        >
          <Icon
            className="h-4 w-4 text-brand"
            strokeWidth={2}
            aria-hidden="true"
          />
          <p className="mt-2 text-sm font-medium leading-none">{label}</p>
          <p className="mt-1 text-[12px] leading-snug text-muted-foreground">
            {hint}
          </p>
        </div>
      ))}
    </div>
  );
}

interface FileCount {
  downloading: number;
  completed: number;
  downloadedSize: number;
}

function LibraryPreview({ isLoading }: { isLoading: boolean }) {
  const router = useRouter();
  const { data, error, isLoading: isLoadingCount } = useSWR<FileCount, Error>(
    `/files/count`,
  );

  return (
    <div className="relative">
      <MediaCollage />

      <div className="relative -mt-6 ml-auto w-full max-w-sm rounded-xl border border-border bg-card p-5 shadow-pop md:-mt-10 md:p-6">
        <div className="flex items-center gap-2">
          <span className="font-display text-sm uppercase tracking-[0.16em] text-muted-foreground">
            Library
          </span>
          <span className="ml-auto inline-flex items-center gap-1.5 rounded-full bg-success-soft px-2 py-0.5 text-[10px] font-medium text-success-soft-foreground">
            <span className="h-1.5 w-1.5 rounded-full bg-success" />
            Ready
          </span>
        </div>

        <h3 className="mt-1 font-display text-2xl leading-tight tracking-tight">
          What you&rsquo;ve saved so far
        </h3>

        {error ? (
          <div className="mt-4 flex items-start gap-2 rounded-md bg-destructive-soft p-3 text-sm text-destructive-soft-foreground">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <div className="flex-1">
              <p className="font-medium">We couldn&rsquo;t reach the library.</p>
              <p className="mt-1 text-[12px] opacity-80">
                Check your backend connection and try again.
              </p>
            </div>
          </div>
        ) : isLoadingCount || !data ? (
          <div className="mt-5 grid grid-cols-3 gap-2">
            <Skeleton className="h-16 rounded-md" />
            <Skeleton className="h-16 rounded-md" />
            <Skeleton className="h-16 rounded-md" />
          </div>
        ) : (
          <div className="mt-5 grid grid-cols-3 gap-2">
            <Stat label="Saved" value={String(data.completed)} icon={Check} />
            <Stat
              label="Active"
              value={String(data.downloading)}
              icon={Download}
              accent={data.downloading > 0}
            />
            <Stat
              label="On disk"
              value={prettyBytes(data.downloadedSize)}
              icon={HardDrive}
              compact
            />
          </div>
        )}

        <Button
          variant="outline"
          className="mt-5 w-full justify-between"
          onClick={() => router.push("/files")}
        >
          Open library
          <ArrowUpRight className="h-4 w-4" />
        </Button>
        {isLoading && (
          <div className="mt-3 inline-flex items-center gap-2 text-[12px] text-muted-foreground">
            <span className="h-1 w-1 animate-pulse rounded-full bg-brand" />
            Syncing accounts…
          </div>
        )}
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  icon: Icon,
  accent,
  compact,
}: {
  label: string;
  value: string;
  icon: typeof Check;
  accent?: boolean;
  compact?: boolean;
}) {
  return (
    <div className="rounded-md border border-border bg-surface p-3">
      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-[0.14em] text-muted-foreground">
        <Icon
          className={
            accent ? "h-3 w-3 text-brand" : "h-3 w-3 text-muted-foreground"
          }
          strokeWidth={2}
        />
        {label}
      </div>
      <p
        className={
          compact
            ? "mt-2 truncate font-mono text-sm font-medium tabular-nums"
            : "mt-2 font-display text-2xl leading-none tabular-nums"
        }
      >
        {value}
      </p>
    </div>
  );
}

function MediaCollage() {
  const tiles = [
    { row: "row-span-2", col: "col-span-2" },
    { row: "", col: "" },
    { row: "", col: "" },
    { row: "", col: "col-span-2" },
    { row: "", col: "" },
  ];

  return (
    <div className="relative">
      <div className="grid grid-cols-3 gap-2 [grid-auto-rows:6rem]">
        {tiles.map((t, i) => (
          <div
            key={i}
            className={`relative overflow-hidden rounded-md border border-border bg-muted ${t.row} ${t.col}`}
            style={{
              background: `linear-gradient(${i * 67}deg, hsl(var(--muted)), hsl(var(--secondary)))`,
            }}
          >
            <ImageIcon
              className="absolute inset-0 m-auto h-6 w-6 text-muted-foreground/40"
              strokeWidth={1.25}
            />
          </div>
        ))}
      </div>
      <div className="pointer-events-none absolute inset-x-0 bottom-0 h-24 bg-gradient-to-t from-background to-transparent" />
    </div>
  );
}

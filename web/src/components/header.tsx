"use client";

import {
  ChevronsLeftRightEllipsisIcon,
  Download,
  UnplugIcon,
} from "lucide-react";
import { TooltipWrapper } from "./ui/tooltip";
import { useWebsocket } from "@/hooks/use-websocket";
import { useTelegramAccount } from "@/hooks/use-telegram-account";
import { SettingsDialog } from "@/components/settings-dialog";
import prettyBytes from "pretty-bytes";
import ChatSelect from "@/components/chat-select";
import Link from "next/link";
import AutomationDialog from "@/components/automation-dialog";
import ThemeToggleButton from "@/components/theme-toggle-button";
import AccountSelect from "@/components/account-select";
import { useSearchParams } from "next/navigation";
import { useSettings } from "@/hooks/use-settings";
import { cn } from "@/lib/utils";

export function Header() {
  const useTelegramAccountProps = useTelegramAccount();
  const { connectionStatus, accountDownloadSpeed } = useWebsocket();
  const { settings } = useSettings();
  const searchParams = useSearchParams();
  const messageThreadId = searchParams.get("messageThreadId");

  return (
    <header className="sticky top-0 z-30 -mx-4 mb-6 border-b border-border/80 bg-background/85 px-4 backdrop-blur-md md:-mx-6 md:px-6">
      <div className="flex h-14 items-center justify-between gap-3">
        <Link
          href="/"
          className="group inline-flex items-center gap-2.5"
          aria-label="TeleFiles home"
        >
          <BrandMark />
          <div className="flex items-baseline gap-2">
            <span className="font-display text-xl leading-none tracking-tight text-foreground">
              TeleFiles
            </span>
            <span className="hidden text-[10px] uppercase tracking-[0.18em] text-muted-foreground sm:inline">
              · vault
            </span>
          </div>
        </Link>

        <div className="flex items-center gap-1.5">
          {accountDownloadSpeed !== 0 && (
            <TooltipWrapper content="Active download speed">
              <div className="hidden h-8 items-center gap-1.5 rounded-full bg-info-soft px-2.5 text-[11px] font-medium text-info-soft-foreground sm:inline-flex">
                <Download className="h-3 w-3" strokeWidth={2.25} />
                <span className="font-mono tabular-nums">
                  {prettyBytes(accountDownloadSpeed, {
                    bits: settings?.speedUnits === "bits",
                  })}
                  /s
                </span>
              </div>
            </TooltipWrapper>
          )}

          {connectionStatus && (
            <TooltipWrapper
              content={`WebSocket — ${connectionStatus.toLowerCase()}`}
            >
              <div
                className={cn(
                  "hidden h-8 items-center gap-1.5 rounded-full px-2.5 text-[11px] font-medium sm:inline-flex",
                  connectionStatus === "Open"
                    ? "bg-success-soft text-success-soft-foreground"
                    : "bg-muted text-muted-foreground",
                )}
              >
                {connectionStatus === "Open" ? (
                  <ChevronsLeftRightEllipsisIcon
                    className="h-3 w-3"
                    strokeWidth={2.25}
                  />
                ) : (
                  <UnplugIcon className="h-3 w-3" strokeWidth={2.25} />
                )}
                {connectionStatus}
              </div>
            </TooltipWrapper>
          )}

          <ThemeToggleButton />
          <SettingsDialog />
        </div>
      </div>

      <div className="flex flex-col gap-2 pb-3 lg:flex-row lg:items-center lg:gap-3">
        <div className="flex flex-1 flex-col gap-2 sm:flex-row sm:items-center">
          <div className="w-full sm:w-[240px]">
            <AccountSelect {...useTelegramAccountProps} />
          </div>
          <div className="w-full flex-1 sm:max-w-[480px]">
            <ChatSelect disabled={!useTelegramAccountProps.accountId} />
          </div>
        </div>

        <div className="flex items-center justify-between gap-2 lg:justify-end">
          <span className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground lg:hidden">
            {messageThreadId ? "Thread" : "Browse"}
          </span>
          {!messageThreadId && <AutomationDialog />}
        </div>
      </div>
    </header>
  );
}

function BrandMark() {
  return (
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
  );
}

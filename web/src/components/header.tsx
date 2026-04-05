"use client";

import { Card, CardContent } from "@/components/ui/card";
import {
  ChevronsLeftRightEllipsisIcon,
  Download,
  UnplugIcon,
} from "lucide-react";
import { TooltipWrapper } from "./ui/tooltip";
import { Badge } from "@/components/ui/badge";
import { useWebsocket } from "@/hooks/use-websocket";
import { useTelegramAccount } from "@/hooks/use-telegram-account";
import { SettingsDialog } from "@/components/settings-dialog";
import prettyBytes from "pretty-bytes";
import ChatSelect from "@/components/chat-select";
import Link from "next/link";
import TelegramIcon from "@/components/telegram-icon";
import AutomationDialog from "@/components/automation-dialog";
import ThemeToggleButton from "@/components/theme-toggle-button";
import AccountSelect from "@/components/account-select";
import { useSearchParams } from "next/navigation";
import { useSettings } from "@/hooks/use-settings";

export function Header() {
  const useTelegramAccountProps = useTelegramAccount();
  const { connectionStatus, accountDownloadSpeed } = useWebsocket();
  const { settings } = useSettings();
  const searchParams = useSearchParams();
  const messageThreadId = searchParams.get("messageThreadId");

  return (
    <Card className="sticky top-4 z-20 mb-6 bg-card/95 backdrop-blur">
      <CardContent className="p-4 md:p-5">
        <div className="flex flex-col gap-4">
          <div className="flex items-start justify-between gap-4">
            <Link
              href="/"
              className="inline-flex items-center gap-3 rounded-[20px] bg-muted px-3 py-2"
            >
              <div className="flex h-11 w-11 items-center justify-center rounded-full bg-primary text-primary-foreground">
                <TelegramIcon className="h-5 w-5" />
              </div>
              <div>
                <p className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">
                  Telegram downloader
                </p>
                <h1 className="text-xl font-semibold text-foreground">
                  TeleFiles
                </h1>
              </div>
            </Link>

            <div className="flex flex-wrap items-center justify-end gap-2">
              {accountDownloadSpeed !== 0 && (
                <TooltipWrapper content="Current account download speed">
                  <Badge variant="outline" className="gap-2 px-3 py-2 text-xs">
                    <Download className="h-3.5 w-3.5" />
                    {`${prettyBytes(accountDownloadSpeed, { bits: settings?.speedUnits === "bits" })}/s`}
                  </Badge>
                </TooltipWrapper>
              )}

              {connectionStatus && (
                <TooltipWrapper content="WebSocket connection status">
                  <Badge
                    variant={
                      connectionStatus === "Open" ? "default" : "secondary"
                    }
                    className="gap-2 px-3 py-2 text-xs"
                  >
                    {connectionStatus === "Open" ? (
                      <ChevronsLeftRightEllipsisIcon className="h-3.5 w-3.5" />
                    ) : (
                      <UnplugIcon className="h-3.5 w-3.5" />
                    )}
                    {connectionStatus}
                  </Badge>
                </TooltipWrapper>
              )}

              <ThemeToggleButton />
              <SettingsDialog />
            </div>
          </div>

          <div className="grid gap-3 xl:grid-cols-[240px_minmax(320px,420px)_auto]">
            <AccountSelect {...useTelegramAccountProps} />
            <ChatSelect disabled={!useTelegramAccountProps.accountId} />
            <div className="flex items-center justify-between gap-3 rounded-[20px] bg-muted px-4 py-3 text-sm text-muted-foreground">
              <span>{messageThreadId ? "Thread board" : "Browse by chat"}</span>
              {!messageThreadId && <AutomationDialog />}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

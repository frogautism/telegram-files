"use client";

import { Card, CardContent } from "@/components/ui/card";
import {
  ChevronsLeftRightEllipsisIcon,
  Download,
  Ellipsis,
  GalleryHorizontal,
  List,
  UnplugIcon,
} from "lucide-react";
import { useWebsocket } from "@/hooks/use-websocket";
import { useTelegramAccount } from "@/hooks/use-telegram-account";
import prettyBytes from "pretty-bytes";
import Link from "next/link";
import { Drawer as DrawerPrimitive } from "vaul";
import TelegramIcon from "@/components/telegram-icon";
import { Button } from "@/components/ui/button";
import {
  Drawer,
  DrawerOverlay,
  DrawerPortal,
  DrawerTitle,
  DrawerTrigger,
} from "@/components/ui/drawer";
import React, { type CSSProperties } from "react";
import AccountSelect from "@/components/account-select";
import ChatSelect from "@/components/chat-select";
import { cn } from "@/lib/utils";
import AutomationDialog from "@/components/automation-dialog";
import { Badge } from "@/components/ui/badge";
import ThemeToggleButton from "@/components/theme-toggle-button";
import { SettingsDialog } from "@/components/settings-dialog";
import { Label } from "../ui/label";
import { Toggle } from "@/components/ui/toggle";
import { useLocalStorage } from "@/hooks/use-local-storage";
import { useTelegramChat } from "@/hooks/use-telegram-chat";
import { useSettings } from "@/hooks/use-settings";

export function MobileHeader() {
  const { accountDownloadSpeed } = useWebsocket();
  const { settings } = useSettings();

  return (
    <Card className="sticky top-3 z-30 mb-4 bg-card/95 backdrop-blur">
      <CardContent className="p-4">
        <div className="flex w-full items-center justify-between">
          <Link href="/" className="inline-flex items-center gap-2 rounded-full bg-muted px-3 py-2">
            <div className="flex h-9 w-9 items-center justify-center rounded-full bg-primary text-primary-foreground">
              <TelegramIcon className="h-4 w-4" />
            </div>
            <span className="text-sm font-semibold">TeleFiles</span>
          </Link>

          {accountDownloadSpeed !== 0 ? (
            <div className="flex items-center gap-2 rounded-full bg-muted px-3 py-2 text-xs text-muted-foreground">
              <span className="text-nowrap">
                {`${prettyBytes(accountDownloadSpeed, { bits: settings?.speedUnits === "bits" })}/s`}
              </span>
              <Download className="h-4 w-4 flex-shrink-0" />
            </div>
          ) : (
            <h3 className="text-sm font-medium text-muted-foreground">
              Telegram downloader
            </h3>
          )}

          <MenuDrawer />
        </div>
      </CardContent>
    </Card>
  );
}

function MenuDrawer() {
  const useTelegramAccountProps = useTelegramAccount();
  const { chat } = useTelegramChat();
  const { connectionStatus } = useWebsocket();
  const [layout, setLayout] = useLocalStorage<"detailed" | "gallery">(
    "telegramFileLayout",
    "gallery",
  );

  return (
    <Drawer
      direction="left"
      shouldScaleBackground={true}
      preventScrollRestoration={true}
    >
      <DrawerTrigger asChild>
        <Button size="xs" variant="ghost">
          <Ellipsis className="h-4 w-4" />
        </Button>
      </DrawerTrigger>
      <DrawerPortal>
        <DrawerOverlay className="bg-black/30 dark:bg-black/50" />
        <DrawerPrimitive.Content
          className={cn(
            "fixed bottom-2 left-2 top-2 z-50 flex w-4/5 outline-none",
          )}
          style={{ "--initial-transform": "calc(100% + 8px)" } as CSSProperties}
          aria-describedby={undefined}
        >
          <div className="flex h-full w-full grow flex-col rounded-[28px] bg-background p-4">
            <DrawerTitle className="mb-6 text-center text-lg font-semibold">
              TeleFiles
            </DrawerTitle>
            <div className="flex h-full flex-col justify-between">
              <div className="flex flex-1 flex-col gap-4">
                <AccountSelect {...useTelegramAccountProps} />
                <ChatSelect disabled={!useTelegramAccountProps.accountId} />
              </div>
              <div className="flex flex-col gap-4">
                <div className="flex flex-col">
                  <Label className="text-xs font-semibold text-muted-foreground">
                    Auto Download
                  </Label>
                  <div className="py-2">
                    {chat ? (
                      <AutomationDialog />
                    ) : (
                      <Button
                        variant="outline"
                        className="w-full"
                        disabled={true}
                      >
                        No chat selected
                      </Button>
                    )}
                  </div>
                </div>
                <div className="flex flex-col">
                  <Label className="text-xs font-semibold text-muted-foreground">
                    Layout
                  </Label>
                  <div className="py-2">
                    <Toggle
                      className="w-full border border-input bg-card"
                      pressed={layout === "gallery"}
                      onPressedChange={(pressed) => {
                        setLayout(pressed ? "gallery" : "detailed");
                      }}
                    >
                      {layout === "detailed" ? (
                        <>
                          <List className="h-4 w-4" />
                          <span className="">Detailed Layout</span>
                        </>
                      ) : (
                        <>
                          <GalleryHorizontal className="h-4 w-4" />
                          <span>Pin Layout</span>
                        </>
                      )}
                    </Toggle>
                  </div>
                </div>
              </div>
              <div className="flex items-center justify-between gap-2 py-4">
                <Badge
                  variant={
                    connectionStatus === "Open" ? "default" : "secondary"
                  }
                  className="gap-2 px-3 py-2"
                >
                  {connectionStatus === "Open" ? (
                    <ChevronsLeftRightEllipsisIcon className="h-4 w-4" />
                  ) : (
                    <UnplugIcon className="h-4 w-4" />
                  )}
                  {connectionStatus}
                </Badge>

                <ThemeToggleButton />
                <SettingsDialog />
              </div>
            </div>
          </div>
        </DrawerPrimitive.Content>
      </DrawerPortal>
    </Drawer>
  );
}

"use client";

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
    <div className="sticky top-0 z-30 -mx-4 mb-3 border-b border-border/80 bg-background/85 px-4 backdrop-blur-md">
      <div className="flex h-12 w-full items-center justify-between">
        <Link href="/" className="inline-flex items-center gap-2">
          <span className="relative inline-flex h-7 w-7 items-center justify-center overflow-hidden rounded-md border border-border bg-card shadow-card">
            <span className="absolute inset-0 bg-[radial-gradient(120%_120%_at_0%_0%,hsl(var(--brand)/0.18),transparent_55%)]" />
            <span className="relative font-display text-[15px] leading-none tracking-tight">
              T
            </span>
            <span className="absolute -bottom-0.5 right-1 h-1 w-1 rounded-full bg-brand" />
          </span>
          <span className="font-display text-base leading-none tracking-tight">
            TeleFiles
          </span>
        </Link>

        <div className="flex items-center gap-2">
          {accountDownloadSpeed !== 0 && (
            <span className="inline-flex h-7 items-center gap-1.5 rounded-full bg-info-soft px-2 text-[11px] font-medium text-info-soft-foreground">
              <Download className="h-3 w-3" strokeWidth={2.25} />
              <span className="font-mono tabular-nums">
                {prettyBytes(accountDownloadSpeed, {
                  bits: settings?.speedUnits === "bits",
                })}
                /s
              </span>
            </span>
          )}

          <MenuDrawer />
        </div>
      </div>
    </div>
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
        <DrawerOverlay />
        <DrawerPrimitive.Content
          className={cn(
            "fixed bottom-0 left-0 top-0 z-50 flex w-4/5 outline-none",
          )}
          style={{ "--initial-transform": "calc(100% + 8px)" } as CSSProperties}
          aria-describedby={undefined}
        >
          <div className="flex h-full w-full grow flex-col border-r border-border bg-background p-4">
            <DrawerTitle className="mb-6 font-display text-2xl tracking-tight">
              TeleFiles
            </DrawerTitle>
            <div className="flex h-full flex-col justify-between">
              <div className="flex flex-1 flex-col gap-3">
                <AccountSelect {...useTelegramAccountProps} />
                <ChatSelect disabled={!useTelegramAccountProps.accountId} />
              </div>
              <div className="flex flex-col gap-3">
                <div className="flex flex-col gap-1">
                  <Label className="text-xs font-bold text-muted-foreground">
                    Automation
                  </Label>
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
                <div className="flex flex-col gap-1">
                  <Label className="text-xs font-bold text-muted-foreground">
                    Layout
                  </Label>
                  <Toggle
                    className="w-full rounded-md border border-input"
                    pressed={layout === "gallery"}
                    onPressedChange={(pressed) => {
                      setLayout(pressed ? "gallery" : "detailed");
                    }}
                  >
                    {layout === "detailed" ? (
                      <>
                        <List className="h-4 w-4" />
                        <span>Detailed</span>
                      </>
                    ) : (
                      <>
                        <GalleryHorizontal className="h-4 w-4" />
                        <span>Gallery</span>
                      </>
                    )}
                  </Toggle>
                </div>
              </div>
              <div className="mt-3 flex items-center justify-between gap-2 border-t border-border pt-3">
                <Badge
                  variant={
                    connectionStatus === "Open" ? "default" : "secondary"
                  }
                  className="gap-1.5 text-xs"
                >
                  {connectionStatus === "Open" ? (
                    <ChevronsLeftRightEllipsisIcon className="h-3 w-3" />
                  ) : (
                    <UnplugIcon className="h-3 w-3" />
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

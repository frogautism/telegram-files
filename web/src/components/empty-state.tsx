import {
  AlertTriangle,
  ArrowRight,
  Check,
  Download,
  HardDrive,
  LoaderPinwheel,
  MessageSquare,
  UserPlus,
} from "lucide-react";
import { AccountList } from "./account-list";
import { type TelegramAccount } from "@/lib/types";
import TelegramIcon from "@/components/telegram-icon";
import { AccountDialog } from "@/components/account-dialog";
import React from "react";
import { Button } from "@/components/ui/button";
import useSWR from "swr";
import prettyBytes from "pretty-bytes";
import { Card, CardContent } from "./ui/card";
import { useRouter } from "next/navigation";
import { Badge } from "@/components/ui/badge";

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
      <Card className="w-full max-w-2xl">
        <CardContent className="flex flex-col items-center gap-4 p-8 text-center md:p-10">
          <div className="flex h-16 w-16 items-center justify-center rounded-full bg-muted text-foreground">
            <MessageSquare className="h-7 w-7" />
          </div>
          <div className="space-y-2">
            <h2 className="text-3xl font-semibold">{message}</h2>
            <p className="text-sm text-muted-foreground">
              Pick a chat and the board fills in.
            </p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="app-shell px-4 py-6 md:px-6 md:py-8">
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.1fr)_420px]">
        <Card className="overflow-hidden">
          <CardContent className="p-6 md:p-10">
            <div className="grid gap-8 lg:grid-cols-[minmax(0,1fr)_260px] lg:items-start">
              <div className="space-y-6">
                <Badge variant="outline" className="px-3 py-2 uppercase tracking-[0.12em]">
                  Telegram downloader
                </Badge>

                <div className="space-y-4">
                  <div className="flex h-16 w-16 items-center justify-center rounded-full bg-primary text-primary-foreground">
                    <TelegramIcon className="h-7 w-7" />
                  </div>
                  <h1 className="max-w-3xl text-4xl font-semibold leading-[0.95] sm:text-5xl md:text-[4.5rem]">
                    Save Telegram media to a board built for browsing.
                  </h1>
                  <p className="max-w-xl text-base text-muted-foreground">
                    Add an account, pick a chat, download what matters.
                  </p>
                </div>

                <AccountDialog isAdd={true}>
                  <Button size="lg">
                    <UserPlus className="h-4 w-4" />
                    Add account
                  </Button>
                </AccountDialog>
              </div>

              <div className="grid gap-3">
                <MetricTile
                  icon={hasAccounts ? Check : AlertTriangle}
                  label="Accounts"
                  value={String(accounts.length)}
                />
                <MetricTile
                  icon={Download}
                  label="Downloader"
                  value={isLoadingAccount ? "Syncing" : "Ready"}
                />
                <MetricTile
                  icon={HardDrive}
                  label="Mode"
                  value="Board"
                />
              </div>
            </div>
          </CardContent>
        </Card>

        <AllFiles />
      </div>

      {isLoadingAccount && (
        <div className="absolute inset-0 flex items-center justify-center">
          <LoaderPinwheel
            className="h-8 w-8 animate-spin"
            style={{ strokeWidth: "0.8px" }}
          />
        </div>
      )}

      {hasAccounts && accounts.length > 0 && onSelectAccount && (
        <div className="mt-8 space-y-4">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-2xl font-semibold">Accounts</h2>
              <p className="text-sm text-muted-foreground">
                Choose a workspace to start downloading.
              </p>
            </div>
          </div>
          <AccountList accounts={accounts} onSelectAccount={onSelectAccount} />
        </div>
      )}
    </div>
  );
}

function MetricTile({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Check;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-[24px] bg-muted p-4">
      <div className="flex items-center gap-3 text-sm text-muted-foreground">
        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-card">
          <Icon className="h-4 w-4" />
        </div>
        {label}
      </div>
      <p className="mt-4 text-2xl font-semibold text-foreground">{value}</p>
    </div>
  );
}

interface FileCount {
  downloading: number;
  completed: number;
  downloadedSize: number;
}

function AllFiles() {
  const router = useRouter();
  const { data, error, isLoading } = useSWR<FileCount, Error>(`/files/count`);

  if (error) {
    return (
      <Card>
        <CardContent className="flex h-full min-h-[280px] items-center justify-center gap-3 p-6 text-destructive">
          <AlertTriangle className="h-5 w-5" />
          Failed to load library
        </CardContent>
      </Card>
    );
  }

  if (isLoading || !data) {
    return (
      <Card>
        <CardContent className="flex h-full min-h-[280px] items-center justify-center gap-3 p-6 text-muted-foreground">
          <LoaderPinwheel
            className="h-5 w-5 animate-spin"
            style={{ strokeWidth: "0.8px" }}
          />
          Loading library
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardContent className="flex h-full flex-col gap-6 p-6 md:p-8">
        <div className="space-y-2">
          <p className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">
            Downloaded library
          </p>
          <h2 className="text-3xl font-semibold leading-tight">
            Everything you have already saved.
          </h2>
        </div>

        <div className="grid gap-3 sm:grid-cols-3 xl:grid-cols-1">
          <StatTile icon={Check} label="Downloaded" value={String(data.completed)} />
          <StatTile
            icon={Download}
            label="Downloading"
            value={String(data.downloading)}
          />
          <StatTile
            icon={HardDrive}
            label="Storage"
            value={prettyBytes(data.downloadedSize)}
          />
        </div>

        <Button
          variant="secondary"
          className="w-full"
          onClick={() => router.push("/files")}
        >
          Open library
          <ArrowRight className="h-4 w-4" />
        </Button>
      </CardContent>
    </Card>
  );
}

function StatTile({
  icon: Icon,
  label,
  value,
}: {
  icon: typeof Check;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-[22px] bg-muted p-4">
      <div className="flex items-center gap-3 text-sm text-muted-foreground">
        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-card">
          <Icon className="h-4 w-4" />
        </div>
        {label}
      </div>
      <p className="mt-4 text-2xl font-semibold text-foreground">{value}</p>
    </div>
  );
}

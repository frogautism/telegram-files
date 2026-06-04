"use client";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { toast } from "@/hooks/use-toast";
import { DELETE, PATCH, POST } from "@/lib/api";
import type { DouyinSource } from "@/lib/types";
import { cn } from "@/lib/utils";
import { formatDistanceToNow } from "date-fns";
import {
  Check,
  History,
  Loader2,
  Pencil,
  RefreshCw,
  Trash2,
  X,
} from "lucide-react";
import { useState } from "react";
import { type KeyedMutator } from "swr";

const DEFAULT_AUTO_REFRESH_INTERVAL = 1800;

type RefreshResult = {
  sourceId: string;
  discovered: number;
  new: number;
  existing: number;
  failed: number;
  jobId: string;
};

type DeleteResult = {
  deleted: boolean;
  removedFiles: number;
};

function statusVariant(status: string) {
  switch (status) {
    case "error":
      return "destructive" as const;
    case "downloading":
    case "discovering":
      return "info" as const;
    default:
      return "neutral" as const;
  }
}

function sourceLabel(source: DouyinSource) {
  return (
    source.displayName ||
    source.authorName ||
    source.title ||
    source.urlType ||
    source.id
  );
}

function SourceRow({
  source,
  mutate,
}: {
  source: DouyinSource;
  mutate: KeyedMutator<DouyinSource[]>;
}) {
  const [busy, setBusy] = useState<null | "latest" | "backfill" | "auto">(null);
  const [renaming, setRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState(sourceLabel(source));
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const handleRefresh = async (backfill: boolean) => {
    setBusy(backfill ? "backfill" : "latest");
    try {
      const result = (await POST(
        `/douyin/sources/${encodeURIComponent(source.id)}/refresh`,
        { backfill },
      )) as RefreshResult;
      await mutate();
      toast({
        variant: "success",
        title: backfill ? "Backfill started" : "Refreshed latest",
        description: `${result.new ?? 0} new · ${result.discovered ?? 0} discovered`,
      });
    } catch (error) {
      toast({
        variant: "error",
        title: "Refresh failed",
        description: error instanceof Error ? error.message : "Request failed.",
      });
    } finally {
      setBusy(null);
    }
  };

  const handleRename = async () => {
    const trimmed = renameValue.trim();
    if (!trimmed || trimmed === sourceLabel(source)) {
      setRenaming(false);
      return;
    }
    try {
      await PATCH(`/douyin/sources/${encodeURIComponent(source.id)}`, {
        displayName: trimmed,
      });
      await mutate();
      setRenaming(false);
      toast({ variant: "success", title: "Renamed source" });
    } catch (error) {
      toast({
        variant: "error",
        title: "Rename failed",
        description: error instanceof Error ? error.message : "Request failed.",
      });
    }
  };

  const handleToggleAutoRefresh = async (enabled: boolean) => {
    setBusy("auto");
    try {
      await PATCH(`/douyin/sources/${encodeURIComponent(source.id)}`, {
        autoRefresh: {
          enabled,
          intervalSeconds:
            source.autoRefresh?.intervalSeconds || DEFAULT_AUTO_REFRESH_INTERVAL,
        },
      });
      await mutate();
    } catch (error) {
      toast({
        variant: "error",
        title: "Auto-refresh update failed",
        description: error instanceof Error ? error.message : "Request failed.",
      });
    } finally {
      setBusy(null);
    }
  };

  const handleDelete = async () => {
    setDeleting(true);
    try {
      const result = (await DELETE(
        `/douyin/sources/${encodeURIComponent(source.id)}?deleteFiles=${deleteFiles}`,
      )) as DeleteResult;
      await mutate();
      setConfirmDelete(false);
      toast({
        variant: "success",
        title: "Source deleted",
        description: deleteFiles
          ? `Removed ${result.removedFiles ?? 0} files`
          : undefined,
      });
    } catch (error) {
      toast({
        variant: "error",
        title: "Delete failed",
        description: error instanceof Error ? error.message : "Request failed.",
      });
    } finally {
      setDeleting(false);
    }
  };

  const lastRefresh = source.lastRefreshCompletedAt || source.lastRefreshStartedAt;

  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1 space-y-1">
          {renaming ? (
            <div className="flex items-center gap-1.5">
              <Input
                value={renameValue}
                onChange={(event) => setRenameValue(event.target.value)}
                className="h-7 text-sm"
                autoFocus
                onKeyDown={(event) => {
                  if (event.key === "Enter") void handleRename();
                  if (event.key === "Escape") setRenaming(false);
                }}
              />
              <Button
                variant="ghost"
                size="icon-xs"
                aria-label="Save name"
                onClick={() => void handleRename()}
              >
                <Check className="h-3.5 w-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-xs"
                aria-label="Cancel rename"
                onClick={() => setRenaming(false)}
              >
                <X className="h-3.5 w-3.5" />
              </Button>
            </div>
          ) : (
            <div className="flex items-center gap-1.5">
              <span className="truncate text-sm font-medium">
                {sourceLabel(source)}
              </span>
              <Button
                variant="ghost"
                size="icon-xs"
                aria-label="Rename source"
                onClick={() => {
                  setRenameValue(sourceLabel(source));
                  setRenaming(true);
                }}
              >
                <Pencil className="h-3 w-3" />
              </Button>
            </div>
          )}
          <div className="flex flex-wrap items-center gap-1.5">
            <Badge variant={statusVariant(source.status)} size="sm">
              {source.status}
            </Badge>
            {source.urlType && (
              <Badge variant="outline" size="sm">
                {source.urlType}
              </Badge>
            )}
            {lastRefresh ? (
              <span className="text-[11px] text-muted-foreground">
                {formatDistanceToNow(new Date(lastRefresh), {
                  addSuffix: true,
                })}
              </span>
            ) : null}
          </div>
        </div>

        <div className="flex shrink-0 items-center gap-1">
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="Refresh latest"
            title="Refresh latest"
            onClick={() => void handleRefresh(false)}
            disabled={busy !== null}
          >
            <RefreshCw
              className={cn("h-4 w-4", busy === "latest" && "animate-spin")}
            />
          </Button>
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="Backfill older"
            title="Backfill older"
            onClick={() => void handleRefresh(true)}
            disabled={busy !== null}
          >
            <History
              className={cn("h-4 w-4", busy === "backfill" && "animate-spin")}
            />
          </Button>
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="Delete source"
            title="Delete source"
            onClick={() => setConfirmDelete(true)}
            disabled={busy !== null}
          >
            <Trash2 className="h-4 w-4 text-destructive" />
          </Button>
        </div>
      </div>

      <div className="mt-2.5 grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] sm:grid-cols-4">
        <Stat label="Files" value={source.totalFiles ?? 0} />
        <Stat label="Completed" value={source.completedDownloads ?? 0} />
        <Stat label="Failed" value={source.failedDownloads ?? 0} />
        <Stat label="New found" value={source.lastDiscoveredCount ?? 0} />
      </div>

      <div className="mt-2.5 flex items-center justify-between gap-2">
        <label className="inline-flex items-center gap-2 text-xs text-muted-foreground">
          <Switch
            checked={source.autoRefresh?.enabled ?? false}
            disabled={busy === "auto"}
            onCheckedChange={(checked) => void handleToggleAutoRefresh(checked)}
          />
          Auto-refresh
          {source.autoRefresh?.enabled &&
          source.autoRefresh?.intervalSeconds ? (
            <span className="text-[11px]">
              every {Math.round(source.autoRefresh.intervalSeconds / 60)}m
            </span>
          ) : null}
        </label>
      </div>

      {(source.refreshError || source.lastError) && (
        <p className="mt-2 text-[11px] text-destructive">
          {source.refreshError || source.lastError}
        </p>
      )}

      <Dialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <DialogContent className="max-w-sm">
          <DialogHeader>
            <DialogTitle>Delete source</DialogTitle>
            <DialogDescription>
              Remove &quot;{sourceLabel(source)}&quot; from your library. This
              cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              checked={deleteFiles}
              onCheckedChange={(checked) => setDeleteFiles(checked === true)}
            />
            Also delete downloaded files
          </label>
          <DialogFooter>
            <DialogClose asChild>
              <Button variant="outline" size="sm" disabled={deleting}>
                Cancel
              </Button>
            </DialogClose>
            <Button
              variant="destructive"
              size="sm"
              onClick={() => void handleDelete()}
              disabled={deleting}
            >
              {deleting ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Trash2 className="h-4 w-4" />
              )}
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div className="flex items-baseline justify-between gap-1 sm:flex-col sm:items-start sm:justify-start sm:gap-0">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-mono font-medium tabular-nums">{value}</span>
    </div>
  );
}

export default function DouyinSourceManager({
  open,
  onOpenChange,
  sources,
  mutate,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sources: DouyinSource[];
  mutate: KeyedMutator<DouyinSource[]>;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex max-h-[85vh] max-w-2xl flex-col">
        <DialogHeader>
          <DialogTitle>Manage Douyin sources</DialogTitle>
          <DialogDescription>
            Refresh, rename, configure auto-refresh, or remove sources.
          </DialogDescription>
        </DialogHeader>
        <div className="-mx-1 flex-1 space-y-2 overflow-y-auto px-1 py-1">
          {sources.length === 0 ? (
            <p className="py-8 text-center text-sm text-muted-foreground">
              No sources yet. Add one from the header.
            </p>
          ) : (
            sources.map((source) => (
              <SourceRow key={source.id} source={source} mutate={mutate} />
            ))
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}

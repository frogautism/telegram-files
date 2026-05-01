"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Button } from "@/components/ui/button";
import {
  ChevronLeft,
  Download,
  Film,
  FileText,
  Grid3x3,
  LayoutGrid,
  Loader2,
  Music2,
  RefreshCw,
  Rows3,
  WandSparkles,
} from "lucide-react";
import { useFiles } from "@/hooks/use-files";
import FileNotFount from "@/components/file-not-found";
import type { TelegramFile } from "@/lib/types";
import FileViewer from "@/components/file-viewer";
import FileFilters from "./file-filters";
import FileBatchControl from "@/components/file-batch-control";
import FileImage from "@/components/file-image";
import FileStatus from "@/components/file-status";
import FileControl from "@/components/file-control";
import { Progress } from "@/components/ui/progress";
import { useFileSpeed } from "@/hooks/use-file-speed";
import { useSettings } from "@/hooks/use-settings";
import prettyBytes from "pretty-bytes";
import { cn } from "@/lib/utils";
import SpoiledWrapper from "@/components/spoiled-wrapper";
import FileCaptionText from "@/components/file-caption-text";
import { groupFilesByMessage, type FileGroup } from "@/lib/file-groups";
import { formatDistanceToNow } from "date-fns";
import useSWRMutation from "swr/mutation";
import { POST } from "@/lib/api";
import { toast } from "@/hooks/use-toast";
import { MediaGridSkeleton } from "@/components/ui/skeleton";
import { TooltipWrapper } from "@/components/ui/tooltip";

interface FileTableProps {
  accountId: string;
  chatId: string;
  messageThreadId?: number;
  link?: string;
}

type Density = "compact" | "comfortable" | "detail";

const DENSITY_KEY = "telefiles:grid-density";

export function FileTable({
  accountId,
  chatId,
  messageThreadId,
  link,
}: FileTableProps) {
  const [selectedFiles, setSelectedFiles] = useState<Set<number>>(new Set());
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  const useFilesProps = useFiles(accountId, chatId, messageThreadId, link);
  const {
    filters,
    updateField,
    handleFilterChange,
    clearFilters,
    isLoading,
    reload,
    size,
    files,
    hasMore,
    handleLoadMore,
  } = useFilesProps;
  const [currentViewFile, setCurrentViewFile] = useState<
    TelegramFile | undefined
  >();
  const [viewerOpen, setViewerOpen] = useState(false);
  const [isReloading, setIsReloading] = useState(false);
  const [density, setDensity] = useState<Density>("comfortable");
  const fileGroups = useMemo(() => groupFilesByMessage(files), [files]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const stored = window.localStorage.getItem(DENSITY_KEY);
    if (stored === "compact" || stored === "comfortable" || stored === "detail") {
      setDensity(stored);
    }
  }, []);

  const updateDensity = (next: Density) => {
    setDensity(next);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(DENSITY_KEY, next);
    }
  };

  useEffect(() => {
    if (files.length === 0 || !currentViewFile) return;
    const index = files.findIndex((file) => file.id === currentViewFile.id);
    if (index === -1) {
      setCurrentViewFile(undefined);
      return;
    }
    const file = files[index]!;
    if (currentViewFile.next === undefined && file.next !== undefined) {
      setCurrentViewFile(file);
    }
  }, [currentViewFile, files]);

  useEffect(() => {
    if (!hasMore || isLoading) return;
    const node = loadMoreRef.current;
    if (!node) return;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry?.isIntersecting) void handleLoadMore();
      },
      { rootMargin: "600px 0px" },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, [handleLoadMore, hasMore, isLoading, files.length]);

  const activeFilterCount = Object.entries(filters).filter(([key, value]) => {
    if (["offline", "sort", "order", "dateType", "sizeUnit"].includes(key)) {
      return false;
    }
    if (key === "type") return value !== "media";
    if (typeof value === "string") return value !== "";
    if (typeof value === "boolean") return value;
    if (Array.isArray(value)) return value.length > 0;
    return false;
  }).length;

  const toggleSelectAll = () => {
    if (files.length === 0) return;
    if (selectedFiles.size === files.length) {
      setSelectedFiles(new Set());
      return;
    }
    setSelectedFiles(new Set(files.map((file) => file.id)));
  };

  const handleSelectFile = (fileId: number) => {
    const nextSelected = new Set(selectedFiles);
    if (nextSelected.has(fileId)) nextSelected.delete(fileId);
    else nextSelected.add(fileId);
    setSelectedFiles(nextSelected);
  };

  const handleTagClick = (tag: string) => {
    void handleFilterChange({ ...filters, search: tag });
  };

  const handleReload = async () => {
    setIsReloading(true);
    try {
      await reload();
    } catch {
      toast({ variant: "error", description: "Failed to refresh files." });
    } finally {
      setIsReloading(false);
    }
  };

  const gridClass =
    density === "compact"
      ? "grid-media-compact"
      : density === "detail"
        ? "grid-media-detail"
        : "grid-media-comfortable";

  const titleLabel = link
    ? "Linked board"
    : messageThreadId
      ? "Thread"
      : filters.type === "media"
        ? "Media"
        : capitalize(filters.type);

  return (
    <>
      {currentViewFile && (
        <FileViewer
          open={viewerOpen}
          onOpenChange={setViewerOpen}
          file={currentViewFile}
          onFileChange={setCurrentViewFile}
          {...useFilesProps}
        />
      )}

      <div className="space-y-5">
        <div className="flex flex-col gap-3">
          <div className="flex items-end justify-between gap-3">
            <div className="space-y-1">
              {messageThreadId && (
                <button
                  className="inline-flex items-center gap-1 text-[11px] uppercase tracking-[0.16em] text-muted-foreground transition-colors hover:text-foreground"
                  onClick={() => window.history.back()}
                >
                  <ChevronLeft className="h-3 w-3" />
                  back
                </button>
              )}
              <h1 className="font-display text-3xl leading-none tracking-tight md:text-4xl">
                {titleLabel}
              </h1>
              <p className="text-sm text-muted-foreground">
                {link ? (
                  <span className="inline-flex items-center gap-1.5">
                    <WandSparkles className="h-3.5 w-3.5" />
                    {link}
                  </span>
                ) : (
                  <>
                    <span className="font-mono tabular-nums">
                      {files.length}
                    </span>{" "}
                    {files.length === 1 ? "item" : "items"}
                    {activeFilterCount > 0 && (
                      <>
                        {" "}
                        ·{" "}
                        <span className="text-foreground">
                          {activeFilterCount} filter
                          {activeFilterCount === 1 ? "" : "s"}
                        </span>
                      </>
                    )}
                  </>
                )}
              </p>
            </div>

            <div className="flex items-center gap-1.5">
              <DensityToggle density={density} onChange={updateDensity} />
              <TooltipWrapper content="Refresh">
                <Button
                  variant="ghost"
                  size="icon-sm"
                  onClick={() => void handleReload()}
                  disabled={isReloading}
                  aria-label="Refresh"
                >
                  <RefreshCw
                    className={cn("h-4 w-4", isReloading && "animate-spin")}
                  />
                </Button>
              </TooltipWrapper>
            </div>
          </div>

          {!link && (
            <FileFilters
              telegramId={accountId}
              chatId={chatId}
              filters={filters}
              onFiltersChange={handleFilterChange}
              clearFilters={clearFilters}
            />
          )}

          {selectedFiles.size > 0 && files.length > 0 && (
            <div className="flex items-center justify-between rounded-md border border-border bg-card px-3 py-1.5 text-xs text-muted-foreground shadow-card">
              <span>
                <span className="font-mono tabular-nums text-foreground">
                  {selectedFiles.size}
                </span>{" "}
                selected
              </span>
              <button
                onClick={toggleSelectAll}
                className="text-foreground underline-offset-2 hover:underline"
              >
                {selectedFiles.size === files.length
                  ? "Clear selection"
                  : "Select all visible"}
              </button>
            </div>
          )}
        </div>

        <FileBatchControl
          files={files}
          selectedFiles={selectedFiles}
          setSelectedFiles={setSelectedFiles}
          updateField={updateField}
        />

        <div>
          {size === 1 && isLoading ? (
            <MediaGridSkeleton count={18} />
          ) : files.length === 0 ? (
            <FileNotFount />
          ) : (
            <>
              <div className={gridClass}>
                {fileGroups.map((group) => (
                  <FileGroupSlot
                    key={group.key}
                    group={group}
                    selectedFiles={selectedFiles}
                    onCheckedChange={handleSelectFile}
                    onFileClick={(file) => {
                      setCurrentViewFile(file);
                      setViewerOpen(true);
                    }}
                    onTagClick={handleTagClick}
                    density={density}
                  />
                ))}
              </div>

              <div ref={loadMoreRef} className="flex justify-center pt-8">
                {hasMore ? (
                  <div className="inline-flex items-center gap-2 text-xs uppercase tracking-[0.18em] text-muted-foreground">
                    <Loader2 className="h-3 w-3 animate-spin" />
                    loading more
                  </div>
                ) : (
                  <span className="text-xs uppercase tracking-[0.18em] text-muted-foreground">
                    end of stream
                  </span>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </>
  );
}

function DensityToggle({
  density,
  onChange,
}: {
  density: Density;
  onChange: (d: Density) => void;
}) {
  const options: Array<{ value: Density; icon: typeof Grid3x3; label: string }> = [
    { value: "compact", icon: Grid3x3, label: "Compact" },
    { value: "comfortable", icon: LayoutGrid, label: "Comfortable" },
    { value: "detail", icon: Rows3, label: "Detail" },
  ];
  return (
    <div className="inline-flex h-8 items-center rounded-md border border-border bg-card p-0.5 shadow-card">
      {options.map((opt) => (
        <TooltipWrapper key={opt.value} content={opt.label}>
          <button
            type="button"
            aria-label={opt.label}
            aria-pressed={density === opt.value}
            onClick={() => onChange(opt.value)}
            className={cn(
              "inline-flex h-7 w-7 items-center justify-center rounded-[6px] text-muted-foreground transition-colors",
              density === opt.value
                ? "bg-foreground text-background"
                : "hover:text-foreground",
            )}
          >
            <opt.icon className="h-3.5 w-3.5" />
          </button>
        </TooltipWrapper>
      ))}
    </div>
  );
}

function FileGroupSlot({
  group,
  selectedFiles,
  onCheckedChange,
  onFileClick,
  onTagClick,
  density,
}: {
  group: FileGroup;
  selectedFiles: Set<number>;
  onCheckedChange: (fileId: number) => void;
  onFileClick: (file: TelegramFile) => void;
  onTagClick: (tag: string) => void;
  density: Density;
}) {
  const grouped = group.files.length > 1;
  const file = group.files[0]!;

  if (!grouped) {
    return (
      <MediaTile
        file={file}
        selected={selectedFiles.has(file.id)}
        onSelect={() => onCheckedChange(file.id)}
        onOpen={() => onFileClick(file)}
        onTagClick={onTagClick}
        density={density}
      />
    );
  }

  return (
    <MediaTile
      file={file}
      selected={selectedFiles.has(file.id)}
      onSelect={() => onCheckedChange(file.id)}
      onOpen={() => onFileClick(file)}
      onTagClick={onTagClick}
      groupCount={group.files.length}
      groupFiles={group.files}
      density={density}
    />
  );
}

function MediaTile({
  file,
  selected,
  onSelect,
  onOpen,
  onTagClick,
  groupCount,
  groupFiles,
  density,
}: {
  file: TelegramFile;
  selected: boolean;
  onSelect: () => void;
  onOpen: () => void;
  onTagClick: (tag: string) => void;
  groupCount?: number;
  groupFiles?: TelegramFile[];
  density: Density;
}) {
  const { settings } = useSettings();
  const { downloadProgress, downloadSpeed } = useFileSpeed(file);
  const aspect = getPreviewAspect(file);
  const isCompact = density === "compact";
  const displayCaption =
    (groupFiles?.find((f) => f.caption.trim() !== "")?.caption ??
      file.caption ??
      "").trim();
  const showCaptionBelow = !isCompact && displayCaption !== "";
  const captionClampClass =
    density === "detail" ? "line-clamp-3" : "line-clamp-2";

  return (
    <div className="group relative flex flex-col gap-2">
      <div
        className={cn("media-tile", aspect)}
        data-selected={selected ? "true" : "false"}
      >
        <button
          type="button"
          onClick={onOpen}
          aria-label="Open viewer"
          className="absolute inset-0 z-0 block focus-visible:outline-none"
        >
          <FileImage file={file} className="h-full w-full object-cover" />
        </button>

        <div
          className={cn(
            "absolute left-2 top-2 z-10 transition-opacity",
            selected
              ? "opacity-100"
              : "opacity-0 group-hover:opacity-100 focus-within:opacity-100",
          )}
          onClick={(e) => e.stopPropagation()}
        >
          <Checkbox
            checked={selected}
            onCheckedChange={onSelect}
            className="h-5 w-5 rounded-sm border-white/70 bg-black/30 text-white shadow-card backdrop-blur-sm data-[state=checked]:border-brand data-[state=checked]:bg-brand"
          />
        </div>

        <div className="pointer-events-none absolute right-2 top-2 z-10 flex items-center gap-1">
          {groupCount && groupCount > 1 && (
            <span className="inline-flex items-center gap-1 rounded-full bg-black/55 px-1.5 py-0.5 text-[10px] font-medium text-white backdrop-blur-sm">
              <LayoutGrid className="h-2.5 w-2.5" strokeWidth={2.5} />
              {groupCount}
            </span>
          )}
          {file.type === "video" && (
            <span className="inline-flex items-center gap-1 rounded-full bg-black/55 px-1.5 py-0.5 text-[10px] font-medium text-white backdrop-blur-sm">
              <Film className="h-2.5 w-2.5" strokeWidth={2.5} />
              {file.extra && "duration" in file.extra
                ? formatDuration(file.extra.duration)
                : "video"}
            </span>
          )}
          {file.type === "audio" && (
            <span className="rounded-full bg-black/55 p-1 text-white backdrop-blur-sm">
              <Music2 className="h-2.5 w-2.5" strokeWidth={2.5} />
            </span>
          )}
          {file.type === "file" && (
            <span className="rounded-full bg-black/55 p-1 text-white backdrop-blur-sm">
              <FileText className="h-2.5 w-2.5" strokeWidth={2.5} />
            </span>
          )}
          {file.type === "photo" &&
            !groupCount &&
            file.reactionCount > 0 &&
            !isCompact && (
              <span className="inline-flex items-center gap-1 rounded-full bg-black/55 px-1.5 py-0.5 text-[10px] font-medium text-white backdrop-blur-sm">
                ♥ {file.reactionCount}
              </span>
            )}
        </div>

        {downloadProgress > 0 && downloadProgress !== 100 && (
          <div className="pointer-events-none absolute inset-x-0 bottom-0 z-10">
            <div className="h-0.5 w-full overflow-hidden bg-black/30">
              <div
                className="h-full bg-brand transition-[width] duration-300"
                style={{ width: `${downloadProgress}%` }}
              />
            </div>
          </div>
        )}

        {!isCompact && (
          <div className="pointer-events-none absolute inset-x-0 bottom-0 z-10 bg-gradient-to-t from-black/90 via-black/45 to-transparent p-2.5 pt-10 opacity-0 transition-opacity group-hover:opacity-100">
            {displayCaption !== "" && (
              <SpoiledWrapper hasSensitiveContent={file.hasSensitiveContent}>
                <div
                  className="pointer-events-auto mb-1.5"
                  onClick={(e) => e.stopPropagation()}
                >
                  <FileCaptionText
                    text={displayCaption}
                    className="line-clamp-2 text-[12px] leading-snug text-white/95"
                    onTagClick={onTagClick}
                  />
                </div>
              </SpoiledWrapper>
            )}
            <div className="flex items-end justify-between gap-2 text-[11px] text-white/95">
              <div className="min-w-0 space-y-0.5">
                <div className="truncate font-medium">
                  {prettyBytes(file.size)}
                  {downloadSpeed > 0 &&
                    file.downloadStatus === "downloading" && (
                      <span className="ml-1.5 font-mono opacity-80">
                        ·{" "}
                        {prettyBytes(downloadSpeed, {
                          bits: settings?.speedUnits === "bits",
                        })}
                        /s
                      </span>
                    )}
                </div>
                <div className="text-[10px] uppercase tracking-[0.14em] text-white/65">
                  {formatDistanceToNow(new Date(file.date * 1000), {
                    addSuffix: true,
                  })}
                </div>
              </div>
              <div
                className="pointer-events-auto"
                onClick={(e) => e.stopPropagation()}
              >
                <FileControl
                  file={file}
                  downloadSpeed={downloadSpeed}
                  hovered={true}
                />
              </div>
            </div>
          </div>
        )}

        {selected && (
          <div className="pointer-events-none absolute inset-0 z-0 bg-brand/10" />
        )}
      </div>

      {density !== "compact" && (
        <div className="flex items-center justify-between gap-2 px-0.5">
          <div className="flex items-center gap-2">
            <FileStatus file={file} hideText={density === "comfortable"} />
            {groupCount && groupCount > 1 && (
              <MessageGroupDownloadButton files={groupFiles ?? []} />
            )}
          </div>
          {density === "detail" && (
            <span className="text-[10px] uppercase tracking-[0.14em] text-muted-foreground">
              {file.type}
            </span>
          )}
        </div>
      )}

      {showCaptionBelow && (
        <SpoiledWrapper hasSensitiveContent={file.hasSensitiveContent}>
          <FileCaptionText
            text={displayCaption}
            className={cn(
              "px-0.5 text-[13px] leading-snug text-muted-foreground",
              captionClampClass,
            )}
            onTagClick={onTagClick}
          />
        </SpoiledWrapper>
      )}

      {downloadProgress > 0 &&
        downloadProgress !== 100 &&
        density === "detail" && (
          <div className="space-y-1 px-0.5">
            <Progress value={downloadProgress} />
            <div className="flex items-center justify-between font-mono text-[10px] tabular-nums text-muted-foreground">
              <span>{downloadProgress.toFixed(0)}%</span>
              <span>{prettyBytes(file.downloadedSize)}</span>
            </div>
          </div>
        )}
    </div>
  );
}

function MessageGroupDownloadButton({ files }: { files: TelegramFile[] }) {
  const downloadableFiles = files.filter(
    (f) => f.downloadStatus === "idle" || f.downloadStatus === "error",
  );
  const { trigger, isMutating } = useSWRMutation(
    "/files/start-download-multiple",
    (
      key,
      {
        arg,
      }: {
        arg: {
          files: Array<{
            telegramId: number;
            chatId: number;
            messageId: number;
            fileId: number;
            uniqueId: string;
          }>;
        };
      },
    ) => POST(key, arg),
  );

  if (downloadableFiles.length === 0) return null;

  const handleClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      const result = (await trigger({
        files: downloadableFiles.map((file) => ({
          telegramId: file.telegramId ?? 0,
          chatId: file.chatId ?? 0,
          messageId: file.messageId ?? 0,
          fileId: file.id ?? 0,
          uniqueId: file.uniqueId,
        })),
      })) as { processed?: number; failed?: number } | undefined;

      const processed = Math.max(
        0,
        Number(result?.processed ?? downloadableFiles.length),
      );
      const failed = Math.max(0, Number(result?.failed ?? 0));

      if (processed === 0 && failed > 0) {
        toast({
          title: "Download failed",
          description: "None of the items in this message could be started.",
          variant: "error",
        });
        return;
      }

      toast({
        title: failed > 0 ? "Started with skips" : "Download started",
        description:
          failed > 0
            ? `Started ${processed} items, skipped ${failed}.`
            : `Started ${processed} items from this message.`,
        variant: failed > 0 ? "warning" : "success",
      });
    } catch (error) {
      toast({
        title: "Download failed",
        description:
          error instanceof Error ? error.message : "Failed to start download.",
        variant: "error",
      });
    }
  };

  return (
    <TooltipWrapper content={`Download all (${downloadableFiles.length})`}>
      <Button
        size="icon-xs"
        variant="soft"
        onClick={handleClick}
        disabled={isMutating}
      >
        {isMutating ? (
          <Loader2 className="h-3 w-3 animate-spin" />
        ) : (
          <Download className="h-3 w-3" />
        )}
      </Button>
    </TooltipWrapper>
  );
}

function getPreviewAspect(file: TelegramFile) {
  if (file.extra?.width && file.extra?.height) {
    if (file.extra.height > file.extra.width * 1.25) return "aspect-[3/4]";
    if (file.extra.width > file.extra.height * 1.25) return "aspect-[4/3]";
  }
  if (file.type === "video") return "aspect-[4/3]";
  return "aspect-square";
}

function formatDuration(seconds: number) {
  if (!seconds || seconds < 0) return "0:00";
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}:${s.toString().padStart(2, "0")}`;
}

function capitalize(s: string) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

"use client";

import React, { useEffect, useRef, useState } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Button } from "@/components/ui/button";
import {
  LoaderPinwheel,
  SquareChevronLeft,
  WandSparkles,
} from "lucide-react";
import { useFiles } from "@/hooks/use-files";
import FileNotFount from "@/components/file-not-found";
import type { TelegramFile } from "@/lib/types";
import FileViewer from "@/components/file-viewer";
import FileFilters from "./file-filters";
import { Badge } from "@/components/ui/badge";
import FileBatchControl from "@/components/file-batch-control";
import FileImage from "@/components/file-image";
import FileStatus from "@/components/file-status";
import FileExtra from "@/components/file-extra";
import FileControl from "@/components/file-control";
import FileTags from "@/components/file-tags";
import { Progress } from "@/components/ui/progress";
import { useFileSpeed } from "@/hooks/use-file-speed";
import prettyBytes from "pretty-bytes";
import { cn } from "@/lib/utils";
import { useSettings } from "@/hooks/use-settings";

interface FileTableProps {
  accountId: string;
  chatId: string;
  messageThreadId?: number;
  link?: string;
}

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
    size,
    files,
    hasMore,
    handleLoadMore,
  } = useFilesProps;
  const [currentViewFile, setCurrentViewFile] = useState<
    TelegramFile | undefined
  >();
  const [viewerOpen, setViewerOpen] = useState(false);

  useEffect(() => {
    if (files.length === 0 || !currentViewFile) {
      return;
    }

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
    if (!hasMore || isLoading) {
      return;
    }

    const node = loadMoreRef.current;
    if (!node) {
      return;
    }

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry?.isIntersecting) {
          void handleLoadMore();
        }
      },
      { rootMargin: "400px 0px" },
    );

    observer.observe(node);
    return () => observer.disconnect();
  }, [handleLoadMore, hasMore, isLoading, files.length]);

  const activeFilterCount = Object.entries(filters).filter(([key, value]) => {
    if (["offline", "sort", "order", "dateType", "sizeUnit"].includes(key)) {
      return false;
    }
    if (key === "type") {
      return value !== "media";
    }
    if (typeof value === "string") {
      return value !== "";
    }
    if (typeof value === "boolean") {
      return value;
    }
    if (Array.isArray(value)) {
      return value.length > 0;
    }
    return false;
  }).length;

  const toggleSelectAll = () => {
    if (files.length === 0) {
      return;
    }

    if (selectedFiles.size === files.length) {
      setSelectedFiles(new Set());
      return;
    }

    setSelectedFiles(new Set(files.map((file) => file.id)));
  };

  const handleSelectFile = (fileId: number) => {
    const nextSelected = new Set(selectedFiles);
    if (nextSelected.has(fileId)) {
      nextSelected.delete(fileId);
    } else {
      nextSelected.add(fileId);
    }
    setSelectedFiles(nextSelected);
  };

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

      <div className="space-y-6">
        <div className="flex flex-col gap-4 rounded-[28px] bg-muted p-4 md:flex-row md:items-center md:justify-between md:p-5">
          <div className="flex flex-wrap items-center gap-2">
            {messageThreadId && (
              <Button variant="ghost" onClick={() => window.history.back()}>
                <SquareChevronLeft className="h-4 w-4" />
                Back
              </Button>
            )}

            {link ? (
              <Badge variant="outline" className="gap-2 px-3 py-2">
                <WandSparkles className="h-3.5 w-3.5" />
                {link}
              </Badge>
            ) : (
              <>
                <Badge variant="outline" className="px-3 py-2 capitalize">
                  {filters.type}
                </Badge>
                <Badge variant="outline" className="px-3 py-2">
                  {files.length} pins
                </Badge>
                {activeFilterCount > 0 && (
                  <Badge variant="secondary" className="px-3 py-2">
                    {activeFilterCount} filters
                  </Badge>
                )}
                <FileFilters
                  telegramId={accountId}
                  chatId={chatId}
                  filters={filters}
                  onFiltersChange={handleFilterChange}
                  clearFilters={clearFilters}
                />
              </>
            )}
          </div>

          <Button variant="secondary" size="sm" onClick={toggleSelectAll}>
            {selectedFiles.size === files.length && files.length > 0
              ? "Clear selection"
              : "Select visible"}
          </Button>
        </div>

        <FileBatchControl
          files={files}
          selectedFiles={selectedFiles}
          setSelectedFiles={setSelectedFiles}
          updateField={updateField}
        />

        <div className="rounded-[32px] border border-border/80 bg-card p-4 md:p-6">
          {size === 1 && isLoading ? (
            <div className="flex min-h-[60vh] items-center justify-center">
              <LoaderPinwheel
                className="h-8 w-8 animate-spin"
                style={{ strokeWidth: "0.8px" }}
              />
            </div>
          ) : files.length === 0 ? (
            <FileNotFount />
          ) : (
            <>
              <div className="columns-1 gap-4 md:columns-2 xl:columns-3 2xl:columns-4">
                {files.map((file) => (
                  <FilePinCard
                    key={`${file.messageId}-${file.uniqueId}`}
                    file={file}
                    checked={selectedFiles.has(file.id)}
                    onCheckedChange={() => handleSelectFile(file.id)}
                    onFileClick={() => {
                      setCurrentViewFile(file);
                      setViewerOpen(true);
                    }}
                    updateField={updateField}
                  />
                ))}
              </div>

              <div ref={loadMoreRef} className="flex justify-center pt-6">
                {hasMore ? (
                  <div className="inline-flex items-center gap-3 rounded-full bg-muted px-4 py-3 text-sm text-muted-foreground">
                    <LoaderPinwheel
                      className="h-4 w-4 animate-spin"
                      style={{ strokeWidth: "0.8px" }}
                    />
                    Loading more pins
                  </div>
                ) : (
                  <div className="inline-flex items-center rounded-full bg-muted px-4 py-3 text-sm text-muted-foreground">
                    End of board
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </>
  );
}

function FilePinCard({
  file,
  checked,
  onCheckedChange,
  onFileClick,
  updateField,
}: {
  file: TelegramFile;
  checked: boolean;
  onCheckedChange: () => void;
  onFileClick: () => void;
  updateField: (
    uniqueId: string,
    patch: Partial<TelegramFile>,
  ) => Promise<void>;
}) {
  const { settings } = useSettings();
  const { downloadProgress, downloadSpeed } = useFileSpeed(file);

  return (
    <div className="mb-4 break-inside-avoid">
      <div
        className={cn(
          "overflow-hidden rounded-[28px] border border-border/80 bg-card transition-colors hover:bg-muted/40",
          checked && "border-primary",
        )}
      >
        <div className="relative p-3">
          <div
            className="absolute left-5 top-5 z-10"
            onClick={(event) => event.stopPropagation()}
          >
            <Checkbox checked={checked} onCheckedChange={onCheckedChange} />
          </div>

          {file.reactionCount > 0 && (
            <Badge className="absolute right-5 top-5 z-10 rounded-full bg-primary px-3 py-1 text-xs text-primary-foreground">
              {file.reactionCount}
            </Badge>
          )}

          <button type="button" className="block w-full text-left" onClick={onFileClick}>
            <div
              className={cn(
                "overflow-hidden rounded-[24px] border-[8px] border-white bg-muted",
                getPreviewAspect(file),
              )}
            >
              <FileImage file={file} className="h-full w-full object-cover" />
            </div>
          </button>
        </div>

        <div className="space-y-4 px-4 pb-4 pt-1">
          <div className="space-y-3">
            <FileExtra file={file} rowHeight="s" ellipsis={true} />
            <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              <span>{prettyBytes(file.size)}</span>
              <span>&bull;</span>
              <span className="capitalize">{file.type}</span>
              {downloadSpeed > 0 && file.downloadStatus === "downloading" && (
                <>
                  <span>&bull;</span>
                  <span>
                    {prettyBytes(downloadSpeed, {
                      bits: settings?.speedUnits === "bits",
                    })}
                    /s
                  </span>
                </>
              )}
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <FileStatus file={file} className="justify-start" />
            {file.loaded && (
              <FileTags
                file={file}
                onTagsUpdate={(tags) =>
                  updateField(file.uniqueId, { tags: tags.join(",") })
                }
              />
            )}
          </div>

          {downloadProgress > 0 && downloadProgress !== 100 && (
            <div className="space-y-2">
              <Progress value={downloadProgress} className="h-2" />
              <div className="flex items-center justify-between text-xs text-muted-foreground">
                <span>{downloadProgress.toFixed(0)}%</span>
                <span>{prettyBytes(file.downloadedSize)}</span>
              </div>
            </div>
          )}

          <div onClick={(event) => event.stopPropagation()}>
            <FileControl
              file={file}
              downloadSpeed={downloadSpeed}
              hovered={true}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

function getPreviewAspect(file: TelegramFile) {
  if (file.extra?.width && file.extra?.height) {
    if (file.extra.height > file.extra.width * 1.2) {
      return "aspect-[4/5]";
    }

    if (file.extra.width > file.extra.height * 1.25) {
      return "aspect-[4/3]";
    }
  }

  if (file.type === "video") {
    return "aspect-[4/3]";
  }

  return "aspect-square";
}

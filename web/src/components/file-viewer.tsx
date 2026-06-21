"use client";

import { type TelegramFile } from "@/lib/types";
import React, { useEffect, useState } from "react";
import { Dialog, DialogOverlay, DialogPortal, DialogTitle } from "./ui/dialog";
import { VisuallyHidden } from "@radix-ui/react-visually-hidden";
import * as DialogPrimitive from "@radix-ui/react-dialog";
import { cn } from "@/lib/utils";
import FileVideo from "./file-video";
import {
  ArrowDown,
  ChevronLeft,
  ChevronRight,
  Film,
  Info,
  Loader2,
  X,
} from "lucide-react";
import DouyinFrameGalleryDialog from "@/components/douyin-frame-gallery";
import { AnimatePresence, motion } from "framer-motion";
import type { FileFilter } from "@/lib/types";
import FileExtra from "@/components/file-extra";
import { Button } from "@/components/ui/button";
import useFileSwitch from "@/hooks/use-file-switch";
import FileImage from "./file-image";
import SpoiledWrapper from "@/components/spoiled-wrapper";
import FileCaptionText from "@/components/file-caption-text";
import { useCurrentFileWorkspace } from "@/hooks/use-file-workspace";
import prettyBytes from "pretty-bytes";
import { formatDistanceToNow } from "date-fns";

type FileViewerProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  file: TelegramFile;
  onFileChange: (file: TelegramFile) => void;
  filters: FileFilter;
  setFilters: (filters: FileFilter) => Promise<void>;
  hasMore: boolean;
  loadMore: () => Promise<void>;
  isLoading: boolean;
};

export default function FileViewer({
  open,
  onOpenChange,
  onFileChange,
  file,
  filters,
  setFilters,
  hasMore,
  loadMore,
  isLoading,
}: FileViewerProps) {
  const [showInfo, setShowInfo] = useState(false);
  const [showFrames, setShowFrames] = useState(false);
  const { handleNavigation, direction } = useFileSwitch({
    file,
    onFileChange,
    hasMore,
    handleLoadMore: loadMore,
  });
  const { commands } = useCurrentFileWorkspace();
  const downloadAction = commands
    .actions(file)
    .find((action) => action.command === "start");

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (file === undefined || !open) return;
      if (e.key === "ArrowLeft") handleNavigation(-1);
      else if (e.key === "ArrowRight") handleNavigation(1);
      else if (e.key === "i" || e.key === "I") setShowInfo((v) => !v);
      else if (e.key === "Escape") onOpenChange(false);
      else if (e.key === "d" || e.key === "D") {
        void downloadAction?.run();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [downloadAction, handleNavigation, file, open, onOpenChange]);

  const slideVariants = {
    enter: (d: number) => ({ x: d > 0 ? 80 : -80, opacity: 0 }),
    center: { zIndex: 1, x: 0, opacity: 1 },
    exit: (d: number) => ({ zIndex: 0, x: d < 0 ? 80 : -80, opacity: 0 }),
  };

  if (!file) return null;

  const showMessageCaption = shouldShowMessageCaption(file);
  const handleTagClick = (tag: string) => {
    void setFilters({ ...filters, search: tag });
    onOpenChange(false);
  };

  const canExtractFrames =
    file.source === "douyin" &&
    file.type === "video" &&
    file.downloadStatus === "completed";

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogPortal>
        <DialogOverlay className="bg-[#0a0a0a]/97 fixed inset-0 z-50 backdrop-blur-md" />

        <DialogPrimitive.Content
          data-fileid={file.id}
          data-prev={file.prev?.id}
          data-next={file.next?.id}
          className={cn(
            "fixed inset-0 z-50 flex flex-col text-white outline-none data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0",
          )}
          aria-describedby={undefined}
          onInteractOutside={(e) => {
            if (e.target instanceof Element) {
              if (e.target.getAttribute("data-state")) {
                onOpenChange(false);
              }
            }
            e.preventDefault();
          }}
        >
          <VisuallyHidden>
            <DialogTitle>File viewer</DialogTitle>
          </VisuallyHidden>

          {/* Top toolbar */}
          <div className="flex shrink-0 items-center justify-between gap-3 border-b border-white/10 bg-gradient-to-b from-black/70 to-transparent px-4 py-3 md:px-6">
            <div className="flex min-w-0 items-center gap-3">
              <div className="hidden text-[10px] uppercase tracking-[0.2em] text-white/50 md:inline">
                Viewer
              </div>
              <div className="flex min-w-0 items-baseline gap-2">
                <span className="font-display text-lg leading-none tracking-tight">
                  {capitalize(file.type)}
                </span>
                <span className="hidden truncate font-mono text-[11px] text-white/55 sm:inline">
                  · {prettyBytes(file.size)}
                </span>
                <span className="hidden truncate font-mono text-[11px] text-white/55 md:inline">
                  ·{" "}
                  {formatDistanceToNow(new Date(file.date * 1000), {
                    addSuffix: true,
                  })}
                </span>
              </div>
            </div>

            <div className="flex items-center gap-1.5">
              {downloadAction && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => void downloadAction.run()}
                  disabled={downloadAction.pending}
                  className="text-white hover:bg-white/10 hover:text-white"
                >
                  {downloadAction.pending ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <ArrowDown className="h-4 w-4" />
                  )}
                  Download
                </Button>
              )}
              {canExtractFrames && (
                <button
                  type="button"
                  onClick={() => setShowFrames(true)}
                  className="inline-flex h-8 items-center gap-1.5 rounded-md px-2.5 text-xs font-medium text-white/70 transition-colors hover:bg-white/10 hover:text-white"
                >
                  <Film className="h-3.5 w-3.5" />
                  <span className="hidden sm:inline">Frames</span>
                </button>
              )}
              <button
                type="button"
                onClick={() => setShowInfo((v) => !v)}
                className={cn(
                  "inline-flex h-8 items-center gap-1.5 rounded-md px-2.5 text-xs font-medium transition-colors",
                  showInfo
                    ? "bg-white/15 text-white"
                    : "text-white/70 hover:bg-white/10 hover:text-white",
                )}
              >
                <Info className="h-3.5 w-3.5" />
                <span className="hidden sm:inline">Info</span>
                <kbd className="hidden rounded bg-white/10 px-1 font-mono text-[10px] sm:inline">
                  I
                </kbd>
              </button>
              <button
                type="button"
                onClick={() => onOpenChange(false)}
                className="inline-flex h-8 w-8 items-center justify-center rounded-md text-white/70 transition-colors hover:bg-white/10 hover:text-white"
                aria-label="Close"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>

          {/* Body */}
          <div className="flex min-h-0 flex-1">
            <div className="relative flex min-h-0 flex-1 items-center justify-center overflow-hidden">
              {/* Prev / next zones */}
              {file.prev && (
                <button
                  type="button"
                  className="group absolute bottom-0 left-0 top-0 z-10 flex w-20 cursor-pointer items-center justify-start pl-3 transition-opacity hover:bg-gradient-to-r hover:from-black/40 hover:to-transparent md:w-28"
                  onClick={() => handleNavigation(-1)}
                  aria-label="Previous"
                >
                  <span className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-white/0 text-white/70 transition-colors group-hover:bg-white/10 group-hover:text-white">
                    <ChevronLeft className="h-5 w-5" strokeWidth={1.75} />
                  </span>
                </button>
              )}

              {(file.next ?? hasMore) && (
                <button
                  type="button"
                  className="group absolute bottom-0 right-0 top-0 z-10 flex w-20 cursor-pointer items-center justify-end pr-3 transition-opacity hover:bg-gradient-to-l hover:from-black/40 hover:to-transparent md:w-28"
                  onClick={() => handleNavigation(1)}
                  aria-label="Next"
                >
                  <span className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-white/0 text-white/70 transition-colors group-hover:bg-white/10 group-hover:text-white">
                    <ChevronRight className="h-5 w-5" strokeWidth={1.75} />
                  </span>
                </button>
              )}

              <AnimatePresence
                initial={false}
                custom={direction}
                mode="popLayout"
              >
                <motion.div
                  key={file.id}
                  custom={direction}
                  variants={slideVariants}
                  initial="enter"
                  animate="center"
                  exit="exit"
                  transition={{
                    x: { type: "spring", stiffness: 320, damping: 32 },
                    opacity: { duration: 0.18 },
                  }}
                  className="relative max-h-full max-w-full"
                  style={{
                    maxWidth: showInfo
                      ? "calc(100vw - 380px)"
                      : "calc(100vw - 6rem)",
                    maxHeight: "calc(100vh - 5rem)",
                  }}
                >
                  {file.type === "video" &&
                  file.downloadStatus === "completed" ? (
                    <FileVideo file={file} />
                  ) : (
                    <FileImage file={file} isFullPreview />
                  )}
                </motion.div>
              </AnimatePresence>

              {isLoading && (
                <div className="pointer-events-none absolute inset-0 z-20 flex items-center justify-center">
                  <Loader2 className="h-6 w-6 animate-spin text-white/70" />
                </div>
              )}
            </div>

            {/* Right info panel */}
            <AnimatePresence initial={false}>
              {showInfo && (
                <motion.aside
                  key="info"
                  initial={{ x: 60, opacity: 0 }}
                  animate={{ x: 0, opacity: 1 }}
                  exit={{ x: 60, opacity: 0 }}
                  transition={{
                    type: "spring",
                    stiffness: 320,
                    damping: 32,
                  }}
                  className="hidden w-[360px] shrink-0 overflow-y-auto border-l border-white/10 bg-black/60 p-5 backdrop-blur-md md:block"
                >
                  <div className="space-y-5 text-sm">
                    <div>
                      <h3 className="text-[10px] uppercase tracking-[0.2em] text-white/50">
                        Caption
                      </h3>
                      {showMessageCaption ? (
                        <SpoiledWrapper
                          hasSensitiveContent={file.hasSensitiveContent}
                        >
                          <FileCaptionText
                            text={file.caption}
                            className="mt-2 leading-relaxed text-white/90"
                            onTagClick={handleTagClick}
                          />
                        </SpoiledWrapper>
                      ) : (
                        <p className="mt-2 italic text-white/40">No caption.</p>
                      )}
                    </div>

                    <div className="h-px w-full bg-white/10" />

                    <div className="space-y-1">
                      <h3 className="text-[10px] uppercase tracking-[0.2em] text-white/50">
                        Details
                      </h3>
                      <FileExtra
                        file={file}
                        rowHeight="s"
                        onTagClick={handleTagClick}
                      />
                    </div>

                    <div className="h-px w-full bg-white/10" />

                    <div className="grid grid-cols-2 gap-3">
                      <Meta label="Size" value={prettyBytes(file.size)} />
                      <Meta label="Type" value={capitalize(file.type)} />
                      <Meta
                        label="Status"
                        value={capitalize(file.downloadStatus)}
                      />
                      {file.extra && "duration" in file.extra ? (
                        <Meta
                          label="Duration"
                          value={formatDuration(file.extra.duration)}
                        />
                      ) : (
                        file.extra?.width &&
                        file.extra?.height && (
                          <Meta
                            label="Resolution"
                            value={`${file.extra.width}×${file.extra.height}`}
                          />
                        )
                      )}
                    </div>

                    <div className="h-px w-full bg-white/10" />

                    <div className="space-y-2">
                      <h3 className="text-[10px] uppercase tracking-[0.2em] text-white/50">
                        Shortcuts
                      </h3>
                      <Shortcuts />
                    </div>
                  </div>
                </motion.aside>
              )}
            </AnimatePresence>
          </div>
        </DialogPrimitive.Content>
      </DialogPortal>
      {canExtractFrames && (
        <DouyinFrameGalleryDialog
          open={showFrames}
          onOpenChange={setShowFrames}
          uniqueId={file.uniqueId}
        />
      )}
    </Dialog>
  );
}

function Meta({ label, value }: { label: string; value: string }) {
  return (
    <div className="space-y-0.5">
      <div className="text-[10px] uppercase tracking-[0.18em] text-white/50">
        {label}
      </div>
      <div className="text-sm font-medium text-white/90">{value}</div>
    </div>
  );
}

function Shortcuts() {
  const items: Array<[string, string]> = [
    ["←  →", "Navigate"],
    ["I", "Toggle info"],
    ["D", "Download"],
    ["Esc", "Close"],
  ];
  return (
    <ul className="space-y-1.5">
      {items.map(([keys, action]) => (
        <li
          key={action}
          className="flex items-center justify-between text-xs text-white/60"
        >
          <span>{action}</span>
          <kbd className="rounded border border-white/15 bg-white/5 px-1.5 py-0.5 font-mono text-[10px] text-white/80">
            {keys}
          </kbd>
        </li>
      ))}
    </ul>
  );
}

function shouldShowMessageCaption(file: TelegramFile) {
  return (
    (file.type === "photo" || file.type === "video") &&
    file.caption.trim() !== ""
  );
}

function capitalize(s: string) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

function formatDuration(seconds: number) {
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}:${s.toString().padStart(2, "0")}`;
}

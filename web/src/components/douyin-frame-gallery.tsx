"use client";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { toast } from "@/hooks/use-toast";
import { DELETE, getApiUrl, POST } from "@/lib/api";
import type { DouyinFrame } from "@/lib/types";
import { cn } from "@/lib/utils";
import {
  ChevronLeft,
  ChevronRight,
  Film,
  Images,
  Loader2,
  Trash2,
} from "lucide-react";
import prettyBytes from "pretty-bytes";
import { useState } from "react";
import useSWR from "swr";

type ExtractMode = "interval" | "timestamp" | "keyframe";

type ExtractResult = {
  extracted: number;
  frames: DouyinFrame[];
};

function frameSrc(frame: DouyinFrame) {
  if (frame.url?.startsWith("http")) {
    return frame.url;
  }
  if (frame.url) {
    return `${getApiUrl()}${frame.url}`;
  }
  return `${getApiUrl()}/douyin/file/${encodeURIComponent(
    frame.fileUniqueId,
  )}/frames/${frame.id}`;
}

export function DouyinFrameGalleryDialog({
  open,
  onOpenChange,
  uniqueId,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  uniqueId: string;
}) {
  const [mode, setMode] = useState<ExtractMode>("interval");
  const [interval, setIntervalValue] = useState("2");
  const [timestampMs, setTimestampMs] = useState("0");
  const [maxFrames, setMaxFrames] = useState("30");
  const [extracting, setExtracting] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [selectedFrameId, setSelectedFrameId] = useState<number | null>(null);

  const framesKey = open
    ? `/douyin/file/${encodeURIComponent(uniqueId)}/frames`
    : null;
  const { data: frames = [], mutate } = useSWR<DouyinFrame[]>(framesKey);
  const selectedFrameIndex =
    frames.length === 0
      ? -1
      : Math.max(
          0,
          frames.findIndex((frame) => frame.id === selectedFrameId),
        );
  const selectedFrame =
    selectedFrameIndex >= 0 ? frames[selectedFrameIndex] : undefined;

  const goToIndex = (index: number) => {
    if (frames.length === 0) return;
    const clamped = Math.min(Math.max(index, 0), frames.length - 1);
    const nextFrame = frames[clamped];
    if (nextFrame) {
      setSelectedFrameId(nextFrame.id);
    }
  };

  const handleExtract = async () => {
    setExtracting(true);
    try {
      const previousSelectedFrameId = selectedFrame?.id ?? selectedFrameId;
      const body: Record<string, unknown> = {
        mode,
        format: "jpg",
      };
      if (mode === "interval") {
        body.interval = Number(interval) || 1;
        body.maxFrames = Number(maxFrames) || undefined;
      } else if (mode === "timestamp") {
        body.timestampMs = Number(timestampMs) || 0;
      } else {
        body.maxFrames = Number(maxFrames) || undefined;
      }
      const result = (await POST(
        `/douyin/file/${encodeURIComponent(uniqueId)}/frames/extract`,
        body,
      )) as ExtractResult;
      const refreshedFrames = (await mutate()) ?? result.frames ?? [];
      const previousFrameStillExists =
        previousSelectedFrameId !== null &&
        refreshedFrames.some((frame) => frame.id === previousSelectedFrameId);
      setSelectedFrameId(
        previousFrameStillExists
          ? previousSelectedFrameId
          : (refreshedFrames[0]?.id ?? null),
      );
      toast({
        variant: "success",
        title: "Frames extracted",
        description: `${result.extracted ?? result.frames?.length ?? 0} frames`,
      });
    } catch (error) {
      toast({
        variant: "error",
        title: "Frame extraction failed",
        description: error instanceof Error ? error.message : "Request failed.",
      });
    } finally {
      setExtracting(false);
    }
  };

  const handleDeleteAll = async () => {
    setDeleting(true);
    try {
      await DELETE(`/douyin/file/${encodeURIComponent(uniqueId)}/frames`);
      setSelectedFrameId(null);
      await mutate();
      toast({ variant: "success", title: "Frames deleted" });
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

  const hasFrames = frames.length > 0;
  const atFirst = selectedFrameIndex <= 0;
  const atLast = selectedFrameIndex >= frames.length - 1;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex h-[min(880px,92vh)] w-[min(96vw,1200px)] max-w-[min(96vw,1200px)] flex-col gap-0 overflow-hidden p-0">
        <DialogHeader className="shrink-0 space-y-1 border-b border-border px-5 py-4 pr-12 text-left sm:px-6">
          <DialogTitle className="flex items-center gap-2.5 text-base">
            <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-brand to-brand/70 text-brand-foreground shadow-sm shadow-brand/30">
              <Film className="h-4 w-4" />
            </span>
            Douyin video frames
          </DialogTitle>
          <DialogDescription>
            Extract still frames, scrub through the selection, and inspect each
            capture.
          </DialogDescription>
        </DialogHeader>

        {/* Extraction toolbar */}
        <div className="shrink-0 border-b border-border bg-muted/30 px-5 py-3 sm:px-6">
          <div className="flex flex-wrap items-end gap-x-4 gap-y-3">
            <Field label="Mode">
              <Select
                value={mode}
                onValueChange={(value) => setMode(value as ExtractMode)}
              >
                <SelectTrigger className="w-[150px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="interval">Interval</SelectItem>
                  <SelectItem value="timestamp">Timestamp</SelectItem>
                  <SelectItem value="keyframe">Keyframes</SelectItem>
                </SelectContent>
              </Select>
            </Field>

            {mode === "interval" && (
              <Field label="Interval (s)">
                <Input
                  type="number"
                  min={0.1}
                  step={0.1}
                  value={interval}
                  onChange={(event) => setIntervalValue(event.target.value)}
                  className="w-28"
                />
              </Field>
            )}

            {mode === "timestamp" && (
              <Field label="Timestamp (ms)">
                <Input
                  type="number"
                  min={0}
                  value={timestampMs}
                  onChange={(event) => setTimestampMs(event.target.value)}
                  className="w-32"
                />
              </Field>
            )}

            {mode !== "timestamp" && (
              <Field label="Max frames">
                <Input
                  type="number"
                  min={1}
                  value={maxFrames}
                  onChange={(event) => setMaxFrames(event.target.value)}
                  className="w-28"
                />
              </Field>
            )}

            <Button
              onClick={() => void handleExtract()}
              disabled={extracting}
              className="ml-auto shadow-sm shadow-brand/20"
            >
              {extracting ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Film className="h-4 w-4" />
              )}
              Extract
            </Button>
          </div>
        </div>

        {/* Body */}
        <div className="min-h-0 flex-1 overflow-y-auto lg:overflow-hidden">
          {!hasFrames ? (
            <div className="flex h-full min-h-[320px] items-center justify-center p-6">
              <div className="max-w-sm space-y-3 text-center">
                <span className="mx-auto flex h-14 w-14 items-center justify-center rounded-2xl border border-dashed border-border-strong bg-muted/40 text-muted-foreground/70">
                  <Images className="h-6 w-6" />
                </span>
                <p className="text-sm font-medium">No frames yet</p>
                <p className="text-sm text-muted-foreground">
                  Choose a mode and extract frames to start browsing them here.
                </p>
              </div>
            </div>
          ) : (
            <div className="grid min-h-0 lg:h-full lg:grid-cols-[minmax(0,1fr)_320px]">
              {/* Preview + scrubber */}
              <section className="flex min-h-0 flex-col gap-4 p-5 sm:p-6">
                {/* Stage — object-contain in an absolutely filled layer can
                    never overflow, so portrait or landscape frames always fit
                    fully without cropping. */}
                <div className="relative min-h-[280px] flex-1 overflow-hidden rounded-xl bg-gradient-to-b from-zinc-900 to-zinc-950 shadow-inner ring-1 ring-white/5">
                  {/* soft glow behind the frame */}
                  <div
                    aria-hidden
                    className="pointer-events-none absolute inset-0 bg-[radial-gradient(ellipse_at_center,_hsl(var(--brand)/0.12),_transparent_70%)]"
                  />
                  {selectedFrame && (
                    <div className="absolute inset-0 flex items-center justify-center p-4 sm:p-6">
                      {/* eslint-disable-next-line @next/next/no-img-element */}
                      <img
                        key={selectedFrame.id}
                        src={frameSrc(selectedFrame)}
                        alt={`Selected frame ${selectedFrame.frameIndex}`}
                        className="max-h-full max-w-full rounded-md object-contain shadow-2xl shadow-black/50"
                      />
                    </div>
                  )}

                  {/* Frame badge */}
                  {selectedFrame && (
                    <span className="absolute left-3 top-3 inline-flex items-center gap-2 rounded-lg bg-black/55 px-2.5 py-1 font-mono text-[11px] text-white shadow-sm backdrop-blur-md">
                      <span className="font-semibold">
                        #{selectedFrameIndex + 1}
                      </span>
                      <span className="text-white/40">·</span>
                      <span className="text-white/80">
                        {formatTimestamp(selectedFrame.timestampMs)}
                      </span>
                    </span>
                  )}

                  {/* Floating nav arrows */}
                  {frames.length > 1 && (
                    <>
                      <StageNav
                        side="left"
                        onClick={() => goToIndex(selectedFrameIndex - 1)}
                        disabled={atFirst}
                        label="Previous frame"
                      >
                        <ChevronLeft className="h-5 w-5" />
                      </StageNav>
                      <StageNav
                        side="right"
                        onClick={() => goToIndex(selectedFrameIndex + 1)}
                        disabled={atLast}
                        label="Next frame"
                      >
                        <ChevronRight className="h-5 w-5" />
                      </StageNav>
                    </>
                  )}
                </div>

                {/* Scrubber */}
                <div className="flex shrink-0 items-center gap-4">
                  <Slider
                    value={[selectedFrameIndex]}
                    min={0}
                    max={Math.max(0, frames.length - 1)}
                    step={1}
                    disabled={frames.length <= 1}
                    onValueChange={(value) => goToIndex(value[0] ?? 0)}
                    aria-label="Selected frame"
                    className="flex-1"
                  />
                  <span className="shrink-0 whitespace-nowrap font-mono text-xs text-muted-foreground tabular-nums">
                    {selectedFrameIndex + 1} / {frames.length}
                  </span>
                </div>

                {/* Metadata */}
                {selectedFrame && (
                  <dl className="grid shrink-0 grid-cols-2 gap-2 sm:grid-cols-4">
                    <FrameMeta
                      label="Index"
                      value={String(selectedFrame.frameIndex)}
                    />
                    <FrameMeta
                      label="Resolution"
                      value={
                        selectedFrame.width && selectedFrame.height
                          ? `${selectedFrame.width}×${selectedFrame.height}`
                          : "—"
                      }
                    />
                    <FrameMeta
                      label="Format"
                      value={selectedFrame.format.toUpperCase()}
                    />
                    <FrameMeta
                      label="Size"
                      value={
                        selectedFrame.size
                          ? prettyBytes(selectedFrame.size)
                          : "—"
                      }
                    />
                  </dl>
                )}
              </section>

              {/* Filmstrip */}
              <section className="flex min-h-0 flex-col border-t border-border bg-muted/20 lg:border-l lg:border-t-0">
                <div className="flex shrink-0 items-center justify-between gap-2 px-4 py-3">
                  <h3 className="flex items-center gap-2 text-sm font-medium">
                    Frames
                    <span className="rounded-full bg-muted px-1.5 py-0.5 font-mono text-[11px] text-muted-foreground tabular-nums">
                      {frames.length}
                    </span>
                  </h3>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => void handleDeleteAll()}
                    disabled={deleting}
                    className="h-7 px-2 text-destructive hover:bg-destructive-soft hover:text-destructive"
                  >
                    {deleting ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Trash2 className="h-3.5 w-3.5" />
                    )}
                    Clear
                  </Button>
                </div>
                <div className="min-h-0 flex-1 overflow-y-auto px-3 pb-4">
                  <div className="grid grid-cols-3 gap-2.5 sm:grid-cols-4 lg:grid-cols-2">
                    {frames.map((frame, index) => {
                      const selected = frame.id === selectedFrame?.id;
                      return (
                        <button
                          key={frame.id}
                          type="button"
                          onClick={() => setSelectedFrameId(frame.id)}
                          className={cn(
                            "group relative aspect-video overflow-hidden rounded-lg border bg-muted text-left transition-all duration-150 focus:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                            selected
                              ? "border-brand ring-2 ring-brand/40"
                              : "border-border hover:-translate-y-0.5 hover:border-brand/50 hover:shadow-md",
                          )}
                          aria-label={`Select frame ${index + 1}`}
                          aria-pressed={selected}
                        >
                          {/* eslint-disable-next-line @next/next/no-img-element */}
                          <img
                            src={frameSrc(frame)}
                            alt={`Frame ${frame.frameIndex}`}
                            loading="lazy"
                            className={cn(
                              "h-full w-full object-cover transition-opacity duration-150",
                              !selected &&
                                "opacity-80 group-hover:opacity-100",
                            )}
                          />
                          {/* gradient + labels overlay */}
                          <span className="pointer-events-none absolute inset-x-0 bottom-0 flex items-center justify-between gap-2 bg-gradient-to-t from-black/70 via-black/30 to-transparent px-2 pb-1 pt-4 font-mono text-[10px] text-white">
                            <span className="font-semibold">#{index + 1}</span>
                            <span className="text-white/80">
                              {formatTimestamp(frame.timestampMs)}
                            </span>
                          </span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              </section>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}

function StageNav({
  side,
  onClick,
  disabled,
  label,
  children,
}: {
  side: "left" | "right";
  onClick: () => void;
  disabled: boolean;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-label={label}
      className={cn(
        "absolute top-1/2 z-10 flex h-10 w-10 -translate-y-1/2 items-center justify-center rounded-full border border-white/10 bg-black/40 text-white/90 backdrop-blur-md transition-all hover:bg-black/60 focus:outline-none focus-visible:ring-2 focus-visible:ring-white/40 disabled:pointer-events-none disabled:opacity-0",
        side === "left" ? "left-3" : "right-3",
      )}
    >
      {children}
    </button>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1.5">
      <Label className="text-xs text-muted-foreground">{label}</Label>
      {children}
    </div>
  );
}

function FrameMeta({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border bg-muted/30 px-3 py-2">
      <dt className="text-[10px] uppercase tracking-[0.16em] text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1 truncate font-mono text-xs">{value}</dd>
    </div>
  );
}

function formatTimestamp(timestampMs: number) {
  return `${(timestampMs / 1000).toFixed(1)}s`;
}

export default DouyinFrameGalleryDialog;

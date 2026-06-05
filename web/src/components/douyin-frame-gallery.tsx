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
import { Film, Loader2, Trash2 } from "lucide-react";
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

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex max-h-[92vh] w-[min(96vw,1200px)] max-w-[min(96vw,1200px)] flex-col overflow-hidden p-4 sm:p-6">
        <DialogHeader className="shrink-0 pr-8">
          <DialogTitle>Douyin video frames</DialogTitle>
          <DialogDescription>
            Extract still frames, inspect the selected frame, and move through
            the frame list.
          </DialogDescription>
        </DialogHeader>

        <div className="grid shrink-0 gap-2 sm:grid-cols-[auto_1fr_1fr_auto] sm:items-end">
          <div className="space-y-1">
            <Label className="text-xs text-muted-foreground">Mode</Label>
            <Select
              value={mode}
              onValueChange={(value) => setMode(value as ExtractMode)}
            >
              <SelectTrigger className="w-[140px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="interval">Interval</SelectItem>
                <SelectItem value="timestamp">Timestamp</SelectItem>
                <SelectItem value="keyframe">Keyframes</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {mode === "interval" && (
            <div className="space-y-1">
              <Label className="text-xs text-muted-foreground">
                Interval (s)
              </Label>
              <Input
                type="number"
                min={0.1}
                step={0.1}
                value={interval}
                onChange={(event) => setIntervalValue(event.target.value)}
              />
            </div>
          )}

          {mode === "timestamp" && (
            <div className="space-y-1">
              <Label className="text-xs text-muted-foreground">
                Timestamp (ms)
              </Label>
              <Input
                type="number"
                min={0}
                value={timestampMs}
                onChange={(event) => setTimestampMs(event.target.value)}
              />
            </div>
          )}

          {mode !== "timestamp" && (
            <div className="space-y-1">
              <Label className="text-xs text-muted-foreground">Max frames</Label>
              <Input
                type="number"
                min={1}
                value={maxFrames}
                onChange={(event) => setMaxFrames(event.target.value)}
              />
            </div>
          )}

          <Button onClick={() => void handleExtract()} disabled={extracting}>
            {extracting ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Film className="h-4 w-4" />
            )}
            Extract
          </Button>
        </div>

        <div className="flex shrink-0 items-center justify-end gap-3">
          {frames.length > 0 && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => void handleDeleteAll()}
              disabled={deleting}
              className="text-destructive hover:text-destructive"
            >
              {deleting ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Trash2 className="h-4 w-4" />
              )}
              Delete all
            </Button>
          )}
        </div>

        <div className="min-h-0 flex-1 overflow-hidden">
          {frames.length === 0 ? (
            <div className="flex min-h-[360px] items-center justify-center rounded-lg border border-dashed border-border bg-muted/30 p-8 text-center">
              <div className="max-w-sm space-y-2">
                <Film className="mx-auto h-8 w-8 text-muted-foreground/70" />
                <p className="text-sm font-medium">No frames yet</p>
                <p className="text-sm text-muted-foreground">
                  Extract frames above to browse them here.
                </p>
              </div>
            </div>
          ) : (
            <div className="grid h-full min-h-0 gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
              <section className="flex min-h-0 flex-col gap-3">
                <div className="overflow-hidden rounded-lg border border-border bg-black">
                  {selectedFrame && (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img
                      key={selectedFrame.id}
                      src={frameSrc(selectedFrame)}
                      alt={`Selected frame ${selectedFrame.frameIndex}`}
                      className="aspect-video w-full object-contain"
                    />
                  )}
                </div>

                <div className="rounded-lg border border-border bg-muted/30 p-3">
                  <div className="mb-2 flex items-center justify-between gap-3 text-sm">
                    <span className="font-medium">
                      Frame {selectedFrameIndex + 1} / {frames.length}
                    </span>
                    {selectedFrame && (
                      <span className="font-mono text-xs text-muted-foreground">
                        {formatTimestamp(selectedFrame.timestampMs)}
                      </span>
                    )}
                  </div>
                  <Slider
                    value={[selectedFrameIndex]}
                    min={0}
                    max={Math.max(0, frames.length - 1)}
                    step={1}
                    disabled={frames.length <= 1}
                    onValueChange={(value) => {
                      const nextFrame = frames[value[0] ?? 0];
                      if (nextFrame) {
                        setSelectedFrameId(nextFrame.id);
                      }
                    }}
                    aria-label="Selected frame"
                  />
                </div>

                {selectedFrame && (
                  <dl className="grid shrink-0 grid-cols-2 gap-2 text-sm md:grid-cols-4">
                    <FrameMeta
                      label="Index"
                      value={String(selectedFrame.frameIndex)}
                    />
                    <FrameMeta
                      label="Resolution"
                      value={
                        selectedFrame.width && selectedFrame.height
                          ? `${selectedFrame.width}x${selectedFrame.height}`
                          : "-"
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
                          : "-"
                      }
                    />
                  </dl>
                )}
              </section>

              <section className="flex min-h-0 flex-col">
                <div className="mb-2 flex items-center justify-between">
                  <h3 className="text-sm font-medium">All frames</h3>
                  <span className="font-mono text-xs text-muted-foreground">
                    {frames.length}
                  </span>
                </div>
                <div className="-mx-1 min-h-0 flex-1 overflow-y-auto px-1">
                  <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-2">
                    {frames.map((frame, index) => {
                      const selected = frame.id === selectedFrame?.id;
                      return (
                        <button
                          key={frame.id}
                          type="button"
                          onClick={() => setSelectedFrameId(frame.id)}
                          className={cn(
                            "group overflow-hidden rounded-md border bg-muted text-left transition-colors focus:outline-none focus:ring-2 focus:ring-ring",
                            selected
                              ? "border-primary ring-2 ring-primary/30"
                              : "border-border hover:border-primary/60",
                          )}
                          aria-label={`Select frame ${index + 1}`}
                          aria-pressed={selected}
                        >
                          {/* eslint-disable-next-line @next/next/no-img-element */}
                          <img
                            src={frameSrc(frame)}
                            alt={`Frame ${frame.frameIndex}`}
                            loading="lazy"
                            className="aspect-video w-full object-cover"
                          />
                          <span className="flex items-center justify-between gap-2 px-2 py-1 font-mono text-[10px] text-muted-foreground">
                            <span>#{index + 1}</span>
                            <span>{formatTimestamp(frame.timestampMs)}</span>
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

function FrameMeta({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border bg-muted/30 px-3 py-2">
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

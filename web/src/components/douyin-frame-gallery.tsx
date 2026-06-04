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
import { Switch } from "@/components/ui/switch";
import { toast } from "@/hooks/use-toast";
import { DELETE, getApiUrl, POST } from "@/lib/api";
import type { DouyinFrame } from "@/lib/types";
import { Film, Loader2, Trash2 } from "lucide-react";
import { useState } from "react";
import useSWR from "swr";

type ExtractMode = "interval" | "timestamp" | "keyframe";

type ExtractResult = {
  jobId: string;
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
  const [replace, setReplace] = useState(false);
  const [extracting, setExtracting] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const framesKey = open
    ? `/douyin/file/${encodeURIComponent(uniqueId)}/frames`
    : null;
  const { data: frames = [], mutate } = useSWR<DouyinFrame[]>(framesKey);

  const handleExtract = async () => {
    setExtracting(true);
    try {
      const body: Record<string, unknown> = {
        mode,
        format: "jpg",
        replace,
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
      await mutate();
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
      <DialogContent className="flex max-h-[85vh] max-w-3xl flex-col">
        <DialogHeader>
          <DialogTitle>Video frames</DialogTitle>
          <DialogDescription>
            Extract still frames from this Douyin video.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-2 sm:grid-cols-[auto_1fr_1fr_auto] sm:items-end">
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

        <div className="flex items-center justify-between">
          <label className="inline-flex items-center gap-2 text-xs text-muted-foreground">
            <Switch checked={replace} onCheckedChange={setReplace} />
            Replace existing frames
          </label>
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

        <div className="-mx-1 flex-1 overflow-y-auto px-1">
          {frames.length === 0 ? (
            <p className="py-10 text-center text-sm text-muted-foreground">
              No frames yet. Extract some above.
            </p>
          ) : (
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 md:grid-cols-4">
              {frames.map((frame) => (
                <figure
                  key={frame.id}
                  className="overflow-hidden rounded-md border border-border bg-muted"
                >
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src={frameSrc(frame)}
                    alt={`Frame ${frame.frameIndex}`}
                    loading="lazy"
                    className="aspect-video w-full object-cover"
                  />
                  <figcaption className="px-1.5 py-1 font-mono text-[10px] text-muted-foreground">
                    {(frame.timestampMs / 1000).toFixed(1)}s
                  </figcaption>
                </figure>
              ))}
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default DouyinFrameGalleryDialog;

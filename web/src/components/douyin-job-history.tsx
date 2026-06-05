"use client";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Progress } from "@/components/ui/progress";
import { toast } from "@/hooks/use-toast";
import { POST } from "@/lib/api";
import type { DouyinJob, DouyinJobState } from "@/lib/types";
import { cn } from "@/lib/utils";
import { formatDistanceToNow } from "date-fns";
import { ListChecks, Loader2, RotateCcw, X } from "lucide-react";
import { useMemo, useState } from "react";
import useSWR from "swr";

const ACTIVE_STATES: DouyinJobState[] = ["queued", "running"];

const KIND_LABELS: Record<string, string> = {
  source_refresh: "Refresh",
  file_download: "Download",
  batch_download: "Batch download",
};

function canCancelJob(job: DouyinJob) {
  return (
    job.state === "running" &&
    job.kind === "file_download" &&
    Boolean(job.fileUniqueId)
  );
}

function stateVariant(state: DouyinJobState) {
  switch (state) {
    case "running":
      return "info" as const;
    case "queued":
      return "neutral" as const;
    case "completed":
      return "success" as const;
    case "failed":
      return "destructive" as const;
    case "cancelled":
      return "muted" as const;
    default:
      return "neutral" as const;
  }
}

function JobItem({
  job,
  onChanged,
}: {
  job: DouyinJob;
  onChanged: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const isActive = ACTIVE_STATES.includes(job.state);
  const percent =
    job.total > 0
      ? Math.min(100, Math.round(((job.success + job.failed) / job.total) * 100))
      : job.state === "completed"
        ? 100
        : 0;

  const act = async (action: "cancel" | "retry") => {
    setBusy(true);
    try {
      await POST(`/douyin/jobs/${encodeURIComponent(job.id)}/${action}`);
      onChanged();
      toast({
        variant: "success",
        title: action === "cancel" ? "Job cancelled" : "Job retried",
      });
    } catch (error) {
      toast({
        variant: "error",
        title: action === "cancel" ? "Cancel failed" : "Retry failed",
        description: error instanceof Error ? error.message : "Request failed.",
      });
    } finally {
      setBusy(false);
    }
  };

  const timestamp = job.completedAt || job.updatedAt || job.createdAt;

  return (
    <div className="space-y-1.5 rounded-md border border-border bg-card p-2.5">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <Badge variant={stateVariant(job.state)} size="sm">
            {job.state}
          </Badge>
          <span className="truncate text-xs font-medium">
            {KIND_LABELS[job.kind] ?? job.kind}
          </span>
        </div>
        <div className="flex shrink-0 items-center gap-1">
          {job.state === "failed" && (
            <Button
              variant="ghost"
              size="icon-xs"
              aria-label="Retry job"
              title="Retry"
              onClick={() => void act("retry")}
              disabled={busy}
            >
              <RotateCcw className="h-3.5 w-3.5" />
            </Button>
          )}
          {canCancelJob(job) && (
            <Button
              variant="ghost"
              size="icon-xs"
              aria-label="Cancel job"
              title="Cancel"
              onClick={() => void act("cancel")}
              disabled={busy}
            >
              <X className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      </div>

      {isActive && (
        <>
          <Progress value={percent} className="h-1" />
          <div className="flex items-center justify-between text-[10px] text-muted-foreground">
            <span className="truncate">{job.step || "Working..."}</span>
            {job.total > 0 && (
              <span className="font-mono tabular-nums">
                {job.success + job.failed}/{job.total}
              </span>
            )}
          </div>
        </>
      )}

      {!isActive && (
        <div className="flex items-center justify-between text-[10px] text-muted-foreground">
          <span className="truncate">
            {job.state === "failed" && job.error
              ? job.error
              : `${job.success} ok · ${job.failed} failed · ${job.skipped} skipped`}
          </span>
          {timestamp ? (
            <span className="shrink-0">
              {formatDistanceToNow(new Date(timestamp), {
                addSuffix: true,
              })}
            </span>
          ) : null}
        </div>
      )}
    </div>
  );
}

export default function DouyinJobHistory() {
  // SWR refreshInterval keeps history live even if the websocket "douyinJob"
  // event is unavailable; the ws handler also revalidates this key on receipt.
  const { data: jobs = [], mutate } = useSWR<DouyinJob[]>(
    "/douyin/jobs?limit=50",
    { refreshInterval: 3000 },
  );

  const { active, recent } = useMemo(() => {
    const activeJobs = jobs.filter((job) => ACTIVE_STATES.includes(job.state));
    const recentJobs = jobs.filter((job) => !ACTIVE_STATES.includes(job.state));
    return { active: activeJobs, recent: recentJobs };
  }, [jobs]);

  const onChanged = () => void mutate();

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Job history"
          className="relative"
        >
          {active.length > 0 ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <ListChecks className="h-4 w-4" />
          )}
          {active.length > 0 && (
            <span className="absolute -right-0.5 -top-0.5 flex h-4 min-w-4 items-center justify-center rounded-full bg-brand px-1 text-[9px] font-semibold text-brand-foreground">
              {active.length}
            </span>
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent
        align="end"
        className="max-h-[70vh] w-80 overflow-y-auto p-2"
      >
        <div className="space-y-2">
          {active.length > 0 && (
            <div className="space-y-1.5">
              <p className="px-1 text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
                Active
              </p>
              {active.map((job) => (
                <JobItem key={job.id} job={job} onChanged={onChanged} />
              ))}
            </div>
          )}
          <div className="space-y-1.5">
            <p className="px-1 text-[10px] uppercase tracking-[0.18em] text-muted-foreground">
              Recent
            </p>
            {recent.length === 0 ? (
              <p className="px-1 py-4 text-center text-xs text-muted-foreground">
                No recent jobs.
              </p>
            ) : (
              recent
                .slice(0, 30)
                .map((job) => (
                  <JobItem key={job.id} job={job} onChanged={onChanged} />
                ))
            )}
          </div>
        </div>
        <p
          className={cn(
            "mt-2 px-1 text-center text-[10px] text-muted-foreground",
            jobs.length === 0 && "hidden",
          )}
        >
          Updates automatically
        </p>
      </PopoverContent>
    </Popover>
  );
}

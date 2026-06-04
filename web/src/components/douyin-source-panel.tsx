"use client";

import DouyinJobHistory from "@/components/douyin-job-history";
import DouyinSourceManager from "@/components/douyin-source-manager";
import Files from "@/components/files";
import ThemeToggleButton from "@/components/theme-toggle-button";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { POST } from "@/lib/api";
import type { DouyinSource } from "@/lib/types";
import { cn } from "@/lib/utils";
import { Loader2, Plus, RefreshCw, Settings2 } from "lucide-react";
import Link from "next/link";
import { useCallback, useMemo, useState } from "react";
import useSWR from "swr";
import useSWRMutation from "swr/mutation";
import { toast } from "@/hooks/use-toast";

export default function DouyinSourcePanel() {
  const [selectedSourceId, setSelectedSourceId] = useState("");
  const [url, setUrl] = useState("");
  const [preloadOnly, setPreloadOnly] = useState(true);
  const [mode, setMode] = useState("post");
  const [sourceRefreshSignal, setSourceRefreshSignal] = useState(0);
  const [managerOpen, setManagerOpen] = useState(false);
  const {
    data: sources = [],
    isLoading,
    mutate,
  } = useSWR<DouyinSource[]>("/douyin/sources");
  const effectiveSourceId = selectedSourceId === "__all__" ? "" : selectedSourceId;
  const selectedSource = useMemo(
    () => sources.find((source) => source.id === effectiveSourceId),
    [effectiveSourceId, sources],
  );

  const { trigger: createSource, isMutating } = useSWRMutation(
    "/douyin/sources",
    (
      key,
      {
        arg,
      }: {
        arg: {
          url: string;
          mode: string;
          preloadOnly: boolean;
        };
      },
    ) => POST(key, arg) as Promise<DouyinSource>,
  );

  const { trigger: refreshSource, isMutating: isRefreshingSource } =
    useSWRMutation(
      "/douyin/sources/refresh",
      (
        _key,
        {
          arg,
        }: {
          arg: {
            sourceId: string;
          };
        },
      ) =>
        POST(`/douyin/sources/${encodeURIComponent(arg.sourceId)}/refresh`, {
          backfill: false,
        }) as Promise<{ discovered: number; new: number }>,
    );

  const handleAdd = async () => {
    const trimmed = url.trim();
    if (!trimmed) return;
    try {
      const source = await createSource({ url: trimmed, mode, preloadOnly });
      await mutate();
      setSelectedSourceId(source.id);
      setUrl("");
      toast({
        variant: "success",
        title: "Douyin source added",
        description: `Discovered ${source.discovered ?? 0} items.`,
      });
    } catch (error) {
      toast({
        variant: "error",
        title: "Douyin source failed",
        description: error instanceof Error ? error.message : "Request failed.",
      });
    }
  };

  const handleRefreshSelectedSource = useCallback(async () => {
    if (!effectiveSourceId) {
      await mutate();
      return;
    }
    const result = await refreshSource({ sourceId: effectiveSourceId });
    await mutate();
    toast({
      variant: "success",
      title: "Douyin source refreshed",
      description: `${result.new ?? 0} new · ${result.discovered ?? 0} discovered`,
    });
  }, [effectiveSourceId, mutate, refreshSource]);

  const handleRefreshSources = async () => {
    try {
      await handleRefreshSelectedSource();
      if (effectiveSourceId) {
        setSourceRefreshSignal((value) => value + 1);
      }
    } catch (error) {
      toast({
        variant: "error",
        title: "Douyin refresh failed",
        description: error instanceof Error ? error.message : "Request failed.",
      });
    }
  };

  return (
    <div className="app-shell px-4 py-4 md:px-6 md:py-6">
      <header className="sticky top-0 z-30 -mx-4 mb-6 border-b border-border/80 bg-background/85 px-4 backdrop-blur-md md:-mx-6 md:px-6">
        <div className="flex h-14 items-center justify-between gap-3">
          <Link href="/" className="inline-flex items-center gap-2.5">
            <span
              aria-hidden="true"
              className="relative inline-flex h-8 w-8 items-center justify-center overflow-hidden rounded-md border border-border bg-card shadow-card"
            >
              <span className="relative font-display text-[18px] leading-none tracking-tight text-foreground">
                D
              </span>
              <span className="absolute -bottom-0.5 right-1 h-1 w-1 rounded-full bg-brand" />
            </span>
            <div className="flex items-baseline gap-2">
              <span className="font-display text-xl leading-none tracking-tight">
                Douyin
              </span>
              <span className="hidden text-[10px] uppercase tracking-[0.18em] text-muted-foreground sm:inline">
                source library
              </span>
            </div>
          </Link>

          <div className="flex items-center gap-1.5">
            <DouyinJobHistory />
            <Button
              variant="ghost"
              size="icon-sm"
              aria-label="Manage sources"
              title="Manage sources"
              onClick={() => setManagerOpen(true)}
            >
              <Settings2 className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="icon-sm"
              aria-label={
                effectiveSourceId ? "Refresh Douyin source" : "Refresh sources"
              }
              onClick={() => void handleRefreshSources()}
              disabled={isRefreshingSource}
            >
              <RefreshCw
                className={cn(
                  "h-4 w-4",
                  (isLoading || isRefreshingSource) && "animate-spin",
                )}
              />
            </Button>
            <Link
              href="/accounts"
              className="hidden text-xs text-muted-foreground hover:text-foreground sm:inline"
            >
              Telegram
            </Link>
            <ThemeToggleButton />
          </div>
        </div>

        <div className="grid gap-2 pb-3 lg:grid-cols-[minmax(220px,320px)_1fr_auto] lg:items-end">
          <div className="space-y-1">
            <Label className="text-xs text-muted-foreground">Source</Label>
            <Select
              value={selectedSourceId || "__all__"}
              onValueChange={setSelectedSourceId}
            >
              <SelectTrigger>
                <SelectValue
                  placeholder={isLoading ? "Loading..." : "All Douyin sources"}
                />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All Douyin sources</SelectItem>
                {sources.map((source) => (
                  <SelectItem key={source.id} value={source.id}>
                    {source.displayName ||
                      source.authorName ||
                      source.title ||
                      source.urlType ||
                      source.id}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-1">
            <Label className="text-xs text-muted-foreground">Add URL</Label>
            <Input
              value={url}
              onChange={(event) => setUrl(event.target.value)}
              placeholder="https://www.douyin.com/video/..."
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  void handleAdd();
                }
              }}
            />
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Select value={mode} onValueChange={setMode}>
              <SelectTrigger className="w-[120px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="post">Posts</SelectItem>
                <SelectItem value="like">Likes</SelectItem>
                <SelectItem value="mix">Mixes</SelectItem>
                <SelectItem value="music">Music</SelectItem>
              </SelectContent>
            </Select>
            <label className="inline-flex h-9 items-center gap-2 rounded-md border border-border px-2.5 text-xs">
              <Switch checked={preloadOnly} onCheckedChange={setPreloadOnly} />
              Preload
            </label>
            <Button onClick={() => void handleAdd()} disabled={!url.trim() || isMutating}>
              {isMutating ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Plus className="h-4 w-4" />
              )}
              Add
            </Button>
          </div>
        </div>

        {selectedSource?.lastError && (
          <div className="pb-3 text-xs text-destructive">
            {selectedSource.lastError}
          </div>
        )}
      </header>

      <Files
        accountId="-1"
        chatId="-1"
        source="douyin"
        sourceId={effectiveSourceId || undefined}
        onRefreshSource={effectiveSourceId ? handleRefreshSelectedSource : undefined}
        refreshSignal={sourceRefreshSignal}
      />

      <DouyinSourceManager
        open={managerOpen}
        onOpenChange={setManagerOpen}
        sources={sources}
        mutate={mutate}
      />
    </div>
  );
}

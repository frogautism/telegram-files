import * as React from "react";
import { cn } from "@/lib/utils";

function Skeleton({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "rounded-md bg-[linear-gradient(90deg,hsl(var(--muted))_0%,hsl(var(--muted))_40%,hsl(var(--secondary))_50%,hsl(var(--muted))_60%,hsl(var(--muted))_100%)] bg-[length:200%_100%] animate-shimmer",
        className,
      )}
      {...props}
    />
  );
}

function MediaTileSkeleton({ aspect = "square" }: { aspect?: "square" | "portrait" | "landscape" }) {
  const aspectClass =
    aspect === "portrait"
      ? "aspect-[4/5]"
      : aspect === "landscape"
        ? "aspect-[4/3]"
        : "aspect-square";

  return (
    <div className={cn("media-tile", aspectClass)}>
      <Skeleton className="h-full w-full rounded-md" />
    </div>
  );
}

function MediaGridSkeleton({ count = 12 }: { count?: number }) {
  const aspects: Array<"square" | "portrait" | "landscape"> = [
    "square",
    "portrait",
    "landscape",
    "square",
    "portrait",
    "square",
  ];
  return (
    <div className="grid-media-comfortable">
      {Array.from({ length: count }).map((_, i) => (
        <MediaTileSkeleton key={i} aspect={aspects[i % aspects.length]} />
      ))}
    </div>
  );
}

export { Skeleton, MediaTileSkeleton, MediaGridSkeleton };

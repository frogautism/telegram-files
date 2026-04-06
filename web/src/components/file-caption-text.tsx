import React from "react";
import { cn } from "@/lib/utils";

type FileCaptionTextProps = {
  text: string;
  className?: string;
  onTagClick?: (tag: string) => void;
};

export default function FileCaptionText({
  text,
  className,
  onTagClick,
}: FileCaptionTextProps) {
  const tokenPattern = /(#[\p{L}\p{N}_-]+|@[A-Za-z0-9_./-]+\/?)/gu;
  const segments: React.ReactNode[] = [];
  let lastIndex = 0;

  for (const match of text.matchAll(tokenPattern)) {
    const [token] = match;
    const start = match.index ?? 0;
    if (start > lastIndex) {
      segments.push(text.slice(lastIndex, start));
    }

    if (onTagClick) {
      segments.push(
        <button
          key={`${token}-${start}`}
          type="button"
          className="rounded px-0.5 text-primary transition-colors hover:underline focus-visible:underline focus-visible:outline-none"
          onClick={(event) => {
            event.preventDefault();
            event.stopPropagation();
            onTagClick(token);
          }}
        >
          {token}
        </button>,
      );
    } else {
      segments.push(<span key={`${token}-${start}`}>{token}</span>);
    }

    lastIndex = start + token.length;
  }

  if (lastIndex < text.length) {
    segments.push(text.slice(lastIndex));
  }

  return (
    <span className={cn("whitespace-pre-line break-words", className)}>
      {segments.length > 0 ? segments : text}
    </span>
  );
}

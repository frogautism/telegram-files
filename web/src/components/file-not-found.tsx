import { FolderSearch, Sparkles } from "lucide-react";

export default function FileNotFound({
  title = "Nothing here yet",
  description = "Adjust your filters, try a different chat, or switch between live Telegram and your offline archive.",
}: {
  title?: string;
  description?: string;
}) {
  return (
    <div className="flex min-h-[55vh] items-center justify-center px-4 py-12">
      <div className="flex max-w-md flex-col items-center gap-5 text-center">
        <div className="relative flex h-20 w-20 items-center justify-center rounded-full border border-border bg-card shadow-card">
          <FolderSearch
            className="h-7 w-7 text-muted-foreground"
            strokeWidth={1.25}
          />
          <span className="absolute -right-1 -top-1 flex h-6 w-6 items-center justify-center rounded-full bg-brand text-brand-foreground shadow-card">
            <Sparkles className="h-3 w-3" strokeWidth={2} />
          </span>
        </div>
        <div className="space-y-2">
          <h3 className="font-display text-2xl tracking-tight">{title}</h3>
          <p className="text-balance text-sm leading-relaxed text-muted-foreground">
            {description}
          </p>
        </div>
      </div>
    </div>
  );
}

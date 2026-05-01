import * as React from "react";
import { type CSSProperties, useEffect, useState } from "react";
import { format } from "date-fns";
import {
  ArrowDownNarrowWide,
  ArrowUpNarrowWide,
  Calendar as CalendarRange,
  Search,
  SlidersHorizontal,
  X,
} from "lucide-react";
import {
  type DownloadStatus,
  type FileFilter,
  type FileType,
  type SortFields,
  type TransferStatus,
} from "@/lib/types";
import { Button } from "./ui/button";
import {
  Drawer,
  DrawerDescription,
  DrawerFooter,
  DrawerOverlay,
  DrawerPortal,
  DrawerTitle,
  DrawerTrigger,
} from "./ui/drawer";
import { Drawer as DrawerPrimitive } from "vaul";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { RangeSlider } from "@/components/ui/slider";
import { cn, split } from "@/lib/utils";
import { Input } from "@/components/ui/input";
import FileStatusFilter from "@/components/file-status-filter";
import { Switch } from "@/components/ui/switch";
import useIsMobile from "@/hooks/use-is-mobile";
import { TagsSelector } from "@/components/ui/tags-selector";
import { useSettings } from "@/hooks/use-settings";
import { useDebouncedCallback } from "use-debounce";

const TYPE_OPTIONS: Array<{ value: FileType | "all"; label: string }> = [
  { value: "media", label: "All media" },
  { value: "photo", label: "Photos" },
  { value: "video", label: "Videos" },
  { value: "file", label: "Files" },
  { value: "audio", label: "Audio" },
];

interface FileFiltersProps {
  telegramId: string;
  chatId: string;
  filters: FileFilter;
  onFiltersChange: (filters: FileFilter) => void;
  clearFilters: () => void;
}

export default function FileFilters({
  telegramId,
  chatId,
  filters,
  onFiltersChange,
  clearFilters,
}: FileFiltersProps) {
  const noAccountSpecified = telegramId === "-1" && chatId === "-1";
  const isMobile = useIsMobile();
  const [searchValue, setSearchValue] = useState(filters.search);
  const [drawerOpen, setDrawerOpen] = useState(false);

  useEffect(() => {
    setSearchValue(filters.search);
  }, [filters.search]);

  const debouncedSearch = useDebouncedCallback((value: string) => {
    onFiltersChange({ ...filters, search: value });
  }, 280);

  const handleSearchChange = (v: string) => {
    setSearchValue(v);
    debouncedSearch(v);
  };

  const handleTypeChange = (type: FileType | "all") => {
    onFiltersChange({ ...filters, type });
  };

  const handleOfflineToggle = (offline: boolean) => {
    onFiltersChange({ ...filters, offline });
  };

  const handleSortFieldChange = (sort: SortFields) => {
    onFiltersChange({ ...filters, sort });
  };

  const handleSortOrderToggle = () => {
    onFiltersChange({
      ...filters,
      order: filters.order === "asc" ? "desc" : "asc",
    });
  };

  const advancedFilterCount = Object.entries(filters).filter(([key, value]) => {
    if (
      [
        "offline",
        "sort",
        "order",
        "dateType",
        "sizeUnit",
        "type",
        "search",
      ].includes(key)
    )
      return false;
    if (typeof value === "string") return value !== "";
    if (typeof value === "boolean") return value;
    if (Array.isArray(value)) return value.length > 0;
    return false;
  }).length;

  return (
    <div className="flex flex-wrap items-center gap-2">
      <div className="relative min-w-[220px] flex-1 sm:max-w-xs">
        <Search className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={searchValue}
          onChange={(e) => handleSearchChange(e.target.value)}
          placeholder="Search captions, tags, names…"
          className="h-9 pl-8 pr-8"
        />
        {searchValue && (
          <button
            type="button"
            onClick={() => handleSearchChange("")}
            className="absolute right-2 top-1/2 -translate-y-1/2 rounded-full p-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
            aria-label="Clear search"
          >
            <X className="h-3 w-3" />
          </button>
        )}
      </div>

      <div className="hidden h-9 items-center gap-0.5 rounded-md border border-border bg-card p-0.5 text-xs shadow-card md:inline-flex">
        {TYPE_OPTIONS.map((opt) => (
          <button
            key={opt.value}
            onClick={() => handleTypeChange(opt.value)}
            className={cn(
              "h-7 rounded-[6px] px-2.5 font-medium transition-colors",
              (filters.type ?? "media") === opt.value
                ? "bg-foreground text-background"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            {opt.label}
          </button>
        ))}
      </div>

      <div className="md:hidden">
        <Select
          value={(filters.type as string) ?? "media"}
          onValueChange={(v) => handleTypeChange(v as FileType | "all")}
        >
          <SelectTrigger className="h-9 w-[140px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {TYPE_OPTIONS.map((opt) => (
              <SelectItem key={opt.value} value={opt.value}>
                {opt.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="ml-auto flex flex-wrap items-center gap-2">
        {filters.offline && (
          <div className="hidden items-center gap-1 rounded-md border border-border bg-card p-0.5 shadow-card sm:flex">
            <Select
              value={filters.sort ?? "date"}
              onValueChange={(v) => handleSortFieldChange(v as SortFields)}
            >
              <SelectTrigger className="h-7 min-w-[140px] border-0 bg-transparent text-xs shadow-none focus:ring-0">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="date">Sent date</SelectItem>
                <SelectItem value="completion_date">Downloaded date</SelectItem>
                <SelectItem value="size">File size</SelectItem>
                <SelectItem value="reaction_count">Reactions</SelectItem>
              </SelectContent>
            </Select>
            <Button
              variant="ghost"
              size="icon-xs"
              onClick={handleSortOrderToggle}
              aria-label="Toggle sort order"
            >
              {(filters.order ?? "desc") === "asc" ? (
                <ArrowUpNarrowWide className="h-3.5 w-3.5" />
              ) : (
                <ArrowDownNarrowWide className="h-3.5 w-3.5" />
              )}
            </Button>
          </div>
        )}

        {!noAccountSpecified && (
          <div className="inline-flex h-9 items-center gap-1.5 rounded-md border border-border bg-card px-2.5 text-xs shadow-card">
            <Label
              htmlFor="offline-mode"
              className="cursor-pointer font-medium text-muted-foreground"
            >
              {filters.offline ? "Archive" : "Live"}
            </Label>
            <Switch
              id="offline-mode"
              checked={filters.offline}
              onCheckedChange={handleOfflineToggle}
              aria-label="Toggle offline archive"
            />
          </div>
        )}

        <AdvancedFiltersDrawer
          open={drawerOpen}
          setOpen={setDrawerOpen}
          isMobile={isMobile}
          filters={filters}
          onFiltersChange={onFiltersChange}
          clearFilters={clearFilters}
          advancedFilterCount={advancedFilterCount}
          noAccountSpecified={noAccountSpecified}
        />
      </div>
    </div>
  );
}

function AdvancedFiltersDrawer({
  open,
  setOpen,
  isMobile,
  filters,
  onFiltersChange,
  clearFilters,
  advancedFilterCount,
  noAccountSpecified,
}: {
  open: boolean;
  setOpen: (v: boolean) => void;
  isMobile: boolean;
  filters: FileFilter;
  onFiltersChange: (filters: FileFilter) => void;
  clearFilters: () => void;
  advancedFilterCount: number;
  noAccountSpecified: boolean;
}) {
  const [localFilters, setLocalFilters] = useState<FileFilter>(filters);

  useEffect(() => {
    if (open) setLocalFilters(filters);
  }, [open, filters]);

  const handleStatusChange = (
    downloadStatus?: DownloadStatus,
    transferStatus?: TransferStatus,
  ) => {
    setLocalFilters((p) => ({ ...p, downloadStatus, transferStatus }));
  };

  const handleApply = () => {
    onFiltersChange(localFilters);
    setOpen(false);
  };

  const handleClear = () => {
    clearFilters();
    setOpen(false);
  };

  return (
    <Drawer
      open={open}
      onOpenChange={setOpen}
      direction={isMobile ? "bottom" : "right"}
      shouldScaleBackground={isMobile}
      preventScrollRestoration={true}
    >
      <DrawerTrigger asChild>
        <Button variant="outline" size="sm" className="relative gap-1.5">
          <SlidersHorizontal className="h-3.5 w-3.5" />
          <span className="hidden sm:inline">More</span>
          {advancedFilterCount > 0 && (
            <span className="ml-0.5 inline-flex h-4 min-w-4 items-center justify-center rounded-full bg-brand px-1 text-[10px] font-semibold text-brand-foreground">
              {advancedFilterCount}
            </span>
          )}
        </Button>
      </DrawerTrigger>
      <DrawerPortal>
        <DrawerOverlay />
        <DrawerPrimitive.Content
          className={cn(
            isMobile
              ? "fixed inset-x-0 bottom-0 z-50 mt-24 flex h-auto max-h-[88vh] flex-col rounded-t-2xl border border-border bg-card shadow-overlay"
              : "fixed bottom-2 right-2 top-2 z-50 flex w-[420px] outline-none",
          )}
          style={
            isMobile
              ? {}
              : ({ "--initial-transform": "calc(100% + 8px)" } as CSSProperties)
          }
        >
          {isMobile && (
            <div className="mx-auto mt-3 h-1.5 w-12 rounded-full bg-border-strong" />
          )}
          <div
            className={cn(
              "flex h-full w-full grow flex-col overflow-hidden bg-card",
              !isMobile && "rounded-xl border border-border shadow-overlay",
            )}
          >
            <div className="flex items-center justify-between border-b border-border px-5 py-4">
              <DrawerTitle className="font-display text-2xl tracking-tight">
                Refine
              </DrawerTitle>
              <DrawerDescription className="sr-only">
                Adjust filters for the media grid
              </DrawerDescription>
              <Button
                variant="ghost"
                size="icon-sm"
                onClick={() => setOpen(false)}
                aria-label="Close"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>

            <div className="flex-1 space-y-6 overflow-y-auto p-5">
              {!noAccountSpecified && !localFilters.offline && (
                <Section title="Live filters">
                  <ToggleRow
                    id="notDownload"
                    label="Only not downloaded"
                    hint="Focus on files still waiting to be saved."
                    checked={localFilters.downloadStatus === "idle"}
                    onCheckedChange={(checked) =>
                      setLocalFilters((p) => ({
                        ...p,
                        downloadStatus: checked ? "idle" : undefined,
                        alreadyDownloaded: checked
                          ? false
                          : p.alreadyDownloaded,
                      }))
                    }
                  />
                  <ToggleRow
                    id="alreadyDownloaded"
                    label="Already in archive"
                    hint="Show live Telegram files already saved on disk."
                    checked={Boolean(localFilters.alreadyDownloaded)}
                    onCheckedChange={(checked) =>
                      setLocalFilters((p) => ({
                        ...p,
                        alreadyDownloaded: checked,
                        downloadStatus: checked ? undefined : p.downloadStatus,
                      }))
                    }
                  />
                </Section>
              )}

              {localFilters.offline && (
                <>
                  <Section title="Status">
                    <FileStatusFilter
                      downloadStatus={localFilters.downloadStatus}
                      transferStatus={localFilters.transferStatus}
                      onChange={handleStatusChange}
                    />
                  </Section>

                  <Section title="Tags">
                    <TagsField
                      tags={localFilters.tags}
                      onChange={(tags) =>
                        setLocalFilters((p) => ({ ...p, tags }))
                      }
                    />
                  </Section>

                  <Section title="Date">
                    <DateField
                      dateType={localFilters.dateType}
                      dateRange={localFilters.dateRange}
                      onChange={(dateType, dateRange) =>
                        setLocalFilters((p) => ({ ...p, dateType, dateRange }))
                      }
                    />
                  </Section>

                  <Section title="File size">
                    <SizeField
                      sizeRange={localFilters.sizeRange}
                      sizeUnit={localFilters.sizeUnit}
                      onChange={(sizeRange, sizeUnit) =>
                        setLocalFilters((p) => ({ ...p, sizeRange, sizeUnit }))
                      }
                    />
                  </Section>
                </>
              )}
            </div>

            <DrawerFooter className="border-t border-border bg-card">
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  className="flex-1"
                  onClick={handleClear}
                >
                  Clear all
                </Button>
                <Button className="flex-1" onClick={handleApply}>
                  Apply
                </Button>
              </div>
            </DrawerFooter>
          </div>
        </DrawerPrimitive.Content>
      </DrawerPortal>
    </Drawer>
  );
}

function Section({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="space-y-3">
      <h3 className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">
        {title}
      </h3>
      {children}
    </section>
  );
}

function ToggleRow({
  id,
  label,
  hint,
  checked,
  onCheckedChange,
}: {
  id: string;
  label: string;
  hint: string;
  checked: boolean;
  onCheckedChange: (v: boolean) => void;
}) {
  return (
    <div className="flex items-start justify-between gap-3 rounded-md border border-border bg-surface p-3">
      <div className="space-y-1">
        <Label htmlFor={id} className="cursor-pointer text-sm font-medium">
          {label}
        </Label>
        <p className="text-xs text-muted-foreground">{hint}</p>
      </div>
      <Switch id={id} checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  );
}

function TagsField({
  tags,
  onChange,
}: {
  tags: string[];
  onChange: (tags: string[]) => void;
}) {
  const { settings } = useSettings();
  return (
    <TagsSelector
      value={tags}
      onChangeAction={onChange}
      tags={split(",", settings?.tags)}
    />
  );
}

function DateField({
  dateType,
  dateRange,
  onChange,
}: {
  dateType: "sent" | "downloaded" | undefined;
  dateRange: [string, string] | undefined;
  onChange: (
    type: "sent" | "downloaded",
    range: [string, string],
  ) => void;
}) {
  const [open, setOpen] = useState(false);
  const isMobile = useIsMobile();
  const [localType, setLocalType] = useState<"sent" | "downloaded">(
    dateType ?? "sent",
  );
  const [localRange, setLocalRange] = useState<
    [Date | undefined, Date | undefined]
  >([
    dateRange?.[0] ? new Date(dateRange[0]) : undefined,
    dateRange?.[1] ? new Date(dateRange[1]) : undefined,
  ]);

  const handleRangeSelect = (range?: {
    from: Date | undefined;
    to?: Date | undefined;
  }) => {
    if (!range) return;
    setLocalRange([range.from, range.to]);
    if (range.from && range.to) {
      onChange(localType, [
        format(range.from, "yyyy-MM-dd"),
        format(range.to, "yyyy-MM-dd"),
      ]);
    }
  };

  const display =
    dateRange?.[0] && dateRange[1]
      ? `${format(new Date(dateRange[0]), "MMM dd, y")} → ${format(new Date(dateRange[1]), "MMM dd, y")}`
      : "Select range";

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          className="w-full justify-start text-left font-normal text-muted-foreground"
        >
          <CalendarRange className="mr-2 h-4 w-4" />
          <span className="flex-1">{display}</span>
          <span className="ml-2 rounded-full bg-muted px-2 py-0.5 text-[10px] uppercase tracking-[0.14em] text-foreground">
            {localType}
          </span>
        </Button>
      </PopoverTrigger>
      <PopoverContent
        className="w-auto p-3"
        side={isMobile ? undefined : "left"}
        modal
      >
        <div className="mb-3 inline-flex h-8 items-center rounded-md border border-border p-0.5 text-xs">
          <button
            onClick={() => setLocalType("sent")}
            className={cn(
              "h-7 rounded-[6px] px-3 font-medium transition-colors",
              localType === "sent"
                ? "bg-foreground text-background"
                : "text-muted-foreground",
            )}
          >
            Sent
          </button>
          <button
            onClick={() => setLocalType("downloaded")}
            className={cn(
              "h-7 rounded-[6px] px-3 font-medium transition-colors",
              localType === "downloaded"
                ? "bg-foreground text-background"
                : "text-muted-foreground",
            )}
          >
            Downloaded
          </button>
        </div>
        <Calendar
          mode="range"
          selected={{ from: localRange[0], to: localRange[1] }}
          onSelect={handleRangeSelect}
          numberOfMonths={isMobile ? 1 : 2}
          defaultMonth={localRange[0] ?? new Date()}
        />
      </PopoverContent>
    </Popover>
  );
}

function SizeField({
  sizeRange,
  sizeUnit,
  onChange,
}: {
  sizeRange: [number, number] | undefined;
  sizeUnit: "KB" | "MB" | "GB" | undefined;
  onChange: (range: [number, number], unit: "KB" | "MB" | "GB") => void;
}) {
  const defaultRange: [number, number] = [0, 1000];
  const [localRange, setLocalRange] = useState<[number, number]>(
    sizeRange ?? defaultRange,
  );
  const [localUnit, setLocalUnit] = useState<"KB" | "MB" | "GB">(
    sizeUnit ?? "MB",
  );

  const handleChange = (newValue: number[]) => {
    const range: [number, number] = [newValue[0]!, newValue[1]!];
    setLocalRange(range);
    onChange(range, localUnit);
  };

  return (
    <div className="space-y-3">
      <div className="inline-flex h-8 items-center rounded-md border border-border p-0.5 text-xs">
        {(["KB", "MB", "GB"] as const).map((u) => (
          <button
            key={u}
            onClick={() => {
              setLocalUnit(u);
              onChange(localRange, u);
            }}
            className={cn(
              "h-7 rounded-[6px] px-3 font-medium transition-colors",
              localUnit === u
                ? "bg-foreground text-background"
                : "text-muted-foreground",
            )}
          >
            {u}
          </button>
        ))}
      </div>
      <div className="px-2 pt-2" onPointerDown={(e) => e.stopPropagation()}>
        <RangeSlider
          value={localRange}
          min={0}
          max={1000}
          step={1}
          minStepsBetweenThumbs={1}
          className="w-full"
          onValueChange={handleChange}
        />
      </div>
      <div className="flex items-center justify-between font-mono text-xs tabular-nums text-muted-foreground">
        <span>
          {localRange[0]} {localUnit}
        </span>
        <span>
          {localRange[1]} {localUnit}
        </span>
      </div>
    </div>
  );
}

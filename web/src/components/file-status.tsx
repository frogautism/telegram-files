import { type TelegramFile } from "@/lib/types";
import React from "react";
import { cn } from "@/lib/utils";
import { TooltipWrapper } from "@/components/ui/tooltip";
import { AnimatePresence, motion } from "framer-motion";
import {
  CheckCircle2,
  Clock,
  Download,
  FolderSync,
  Pause,
  XCircle,
} from "lucide-react";

type StatusDef = {
  icon: typeof Clock;
  className: string;
  text: string;
};

export const DOWNLOAD_STATUS: Record<string, StatusDef> = {
  idle: {
    icon: Clock,
    className: "bg-muted text-muted-foreground",
    text: "Idle",
  },
  downloading: {
    icon: Download,
    className: "bg-info-soft text-info-soft-foreground",
    text: "Downloading",
  },
  paused: {
    icon: Pause,
    className: "bg-warning-soft text-warning-soft-foreground",
    text: "Paused",
  },
  completed: {
    icon: CheckCircle2,
    className: "bg-success-soft text-success-soft-foreground",
    text: "Completed",
  },
  error: {
    icon: XCircle,
    className: "bg-destructive-soft text-destructive-soft-foreground",
    text: "Error",
  },
};

export const TRANSFER_STATUS: Record<string, StatusDef> = {
  idle: {
    icon: Clock,
    className: "bg-muted text-muted-foreground",
    text: "Idle",
  },
  transferring: {
    icon: FolderSync,
    className: "bg-info-soft text-info-soft-foreground",
    text: "Transferring",
  },
  completed: {
    icon: CheckCircle2,
    className: "bg-success-soft text-success-soft-foreground",
    text: "Transferred",
  },
  error: {
    icon: XCircle,
    className: "bg-destructive-soft text-destructive-soft-foreground",
    text: "Transfer error",
  },
};

const badgeVariants = {
  initial: { opacity: 0, y: -2 },
  animate: { opacity: 1, y: 0, transition: { duration: 0.18 } },
  exit: { opacity: 0, y: -2, transition: { duration: 0.12 } },
};

function StatusPill({
  className,
  text,
  Icon,
  hideText,
}: {
  className: string;
  text: string;
  Icon: typeof Clock;
  hideText?: boolean;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] font-medium leading-5 tracking-tight",
        className,
      )}
    >
      <Icon className="h-3 w-3" strokeWidth={2.25} />
      {!hideText && text}
    </span>
  );
}

export default function FileStatus({
  file,
  className,
  hideText,
}: {
  file: TelegramFile;
  className?: string;
  hideText?: boolean;
}) {
  return (
    <div
      className={cn("flex flex-wrap items-center gap-1.5", className)}
    >
      <AnimatePresence initial={false}>
        {file.alreadyDownloaded && !file.loaded && (
          <motion.div
            key="archive-match"
            variants={badgeVariants}
            initial="initial"
            animate="animate"
            exit="exit"
          >
            <TooltipWrapper content="Already in your archive">
              <span className="inline-flex items-center gap-1.5 rounded-full bg-info-soft px-2 py-0.5 text-[11px] font-medium leading-5 text-info-soft-foreground">
                <CheckCircle2 className="h-3 w-3" />
                In archive
              </span>
            </TooltipWrapper>
          </motion.div>
        )}
        {file.transferStatus === "idle" && (
          <motion.div
            key="download-status"
            variants={badgeVariants}
            initial="initial"
            animate="animate"
            exit="exit"
          >
            <TooltipWrapper content={DOWNLOAD_STATUS[file.downloadStatus]!.text}>
              <span>
                <StatusPill
                  className={DOWNLOAD_STATUS[file.downloadStatus]!.className}
                  text={DOWNLOAD_STATUS[file.downloadStatus]!.text}
                  Icon={DOWNLOAD_STATUS[file.downloadStatus]!.icon}
                  hideText={hideText}
                />
              </span>
            </TooltipWrapper>
          </motion.div>
        )}
        {file.downloadStatus === "completed" &&
          file.transferStatus &&
          file.transferStatus !== "idle" && (
            <motion.div
              key="transfer-status"
              variants={badgeVariants}
              initial="initial"
              animate="animate"
              exit="exit"
            >
              <TooltipWrapper content={TRANSFER_STATUS[file.transferStatus]!.text}>
                <span>
                  <StatusPill
                    className={TRANSFER_STATUS[file.transferStatus]!.className}
                    text={TRANSFER_STATUS[file.transferStatus]!.text}
                    Icon={TRANSFER_STATUS[file.transferStatus]!.icon}
                    hideText={hideText}
                  />
                </span>
              </TooltipWrapper>
            </motion.div>
          )}
      </AnimatePresence>
    </div>
  );
}

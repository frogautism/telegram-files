import { Button } from "@/components/ui/button";
import {
  Download,
  FileX,
  LoaderCircle,
  Pause,
  SquareX,
  StepForward,
} from "lucide-react";
import React, { useState } from "react";
import { type TelegramFile } from "@/lib/types";
import { TooltipWrapper } from "@/components/ui/tooltip";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "./ui/dialog";
import { BatchFileTags } from "@/components/file-tags";
import {
  type BatchFileAction,
  reportBatchOutcome,
} from "@/hooks/use-file-workspace-commands";
import { useCurrentFileWorkspace } from "@/hooks/use-file-workspace";

interface FileBatchControlProps {
  selectedFiles: Set<number>;
  setSelectedFiles: (files: Set<number>) => void;
  files: TelegramFile[];
}

export default function FileBatchControl({
  selectedFiles,
  setSelectedFiles,
  files,
}: FileBatchControlProps) {
  const { commands } = useCurrentFileWorkspace();
  const selectedFileObjects = Array.from(selectedFiles)
    .map((id) => files.find((f) => f.id === id))
    .filter(Boolean) as TelegramFile[];
  const loadedFiles = selectedFileObjects.filter((file) => file.loaded);
  const visibleActions = commands.batchActions(selectedFileObjects);

  return (
    <>
      {selectedFiles.size > 0 && (
        <div className="flex flex-col rounded-lg bg-muted/50 p-4 transition-all duration-300 animate-in slide-in-from-bottom-2 md:flex-row md:items-center md:justify-between">
          <span className="mb-3 text-sm font-medium md:mb-0">
            {selectedFiles.size} {selectedFiles.size === 1 ? "file" : "files"}{" "}
            selected
          </span>
          <div className="flex flex-wrap gap-2">
            {loadedFiles.length > 0 && (
              <BatchFileTags
                files={loadedFiles}
                onTagsUpdate={() => setSelectedFiles(new Set())}
              />
            )}
            {visibleActions.map((action) => (
              <ControlButton
                key={action.command}
                action={action}
                selectedFiles={selectedFiles}
                setSelectedFiles={setSelectedFiles}
              />
            ))}
            <Button
              size="sm"
              variant="outline"
              onClick={() => setSelectedFiles(new Set())}
            >
              Clear Selection
            </Button>
          </div>
        </div>
      )}
    </>
  );
}

interface ControlButtonProps {
  action: BatchFileAction;
  selectedFiles: Set<number>;
  setSelectedFiles: (files: Set<number>) => void;
}

function ControlButton({
  action,
  selectedFiles,
  setSelectedFiles,
}: ControlButtonProps) {
  const [confirmDialogOpen, setConfirmDialogOpen] = useState(false);
  const [isMutating, setIsMutating] = useState(false);
  const invalidCount = selectedFiles.size - action.count;

  const handleAction = async () => {
    setIsMutating(true);
    try {
      if (reportBatchOutcome(action.label, await action.run())) {
        setSelectedFiles(new Set());
      }
    } finally {
      setIsMutating(false);
      setConfirmDialogOpen(false);
    }
  };

  const handleClick = () => {
    if (action.confirm) {
      setConfirmDialogOpen(true);
    } else {
      void handleAction();
    }
  };

  return (
    <>
      <TooltipWrapper
        content={`${action.label} ${action.availableCount} available files${
          action.blockedCount > 0
            ? ` (${action.blockedCount} already in progress)`
            : ""
        }`}
      >
        <Button
          size="sm"
          className={actionClassName(action.command)}
          onClick={handleClick}
          disabled={action.availableCount === 0 || isMutating}
        >
          {isMutating ? (
            <LoaderCircle
              className="mr-2 h-4 w-4 animate-spin"
              style={{ strokeWidth: "0.8px" }}
            />
          ) : (
            <>
              {actionIcon(action.command)}
              {action.label} ({action.availableCount})
            </>
          )}
        </Button>
      </TooltipWrapper>

      <Dialog open={confirmDialogOpen} onOpenChange={setConfirmDialogOpen}>
        <DialogContent className="max-w-xl sm:max-w-md">
          <DialogHeader className="space-y-2">
            <DialogTitle className="text-center text-xl font-semibold">
              {`Confirm ${action.label} Action`}
            </DialogTitle>
            <div className="flex justify-center">
              {action.command === "remove" ? (
                <div className="rounded-full bg-red-100 p-3 dark:bg-red-900/30">
                  <FileX className="h-6 w-6 text-red-600 dark:text-red-400" />
                </div>
              ) : action.command === "cancel" ? (
                <div className="rounded-full bg-red-100 p-3 dark:bg-red-900/30">
                  <SquareX className="h-6 w-6 text-red-600 dark:text-red-400" />
                </div>
              ) : action.command === "start" ? (
                <div className="rounded-full bg-blue-100 p-3 dark:bg-blue-900/30">
                  <Download className="h-6 w-6 text-blue-600 dark:text-blue-400" />
                </div>
              ) : action.command === "resume" ? (
                <div className="rounded-full bg-green-100 p-3 dark:bg-green-900/30">
                  <StepForward className="h-6 w-6 text-green-600 dark:text-green-400" />
                </div>
              ) : (
                <div className="rounded-full bg-yellow-100 p-3 dark:bg-yellow-900/30">
                  <Pause className="h-6 w-6 text-yellow-600 dark:text-yellow-400" />
                </div>
              )}
            </div>
          </DialogHeader>

          <div className="pb-6 pt-2">
            <p className="mb-3 text-center text-sm text-muted-foreground">
              Are you sure you want to {action.label.toLowerCase()} the selected
              files?
            </p>

            <div className="mt-4 flex flex-col gap-3">
              {action.availableCount > 0 && (
                <div className="overflow-hidden rounded-lg border border-green-200 dark:border-green-800">
                  <div className="border-b border-green-200 bg-green-50 px-4 py-2 dark:border-green-800 dark:bg-green-900/20">
                    <span className="text-sm font-medium text-green-800 dark:text-green-300">
                      Files to process
                    </span>
                  </div>
                  <div className="flex items-center bg-white p-4 dark:bg-background">
                    <div className="mr-3 flex h-8 w-8 items-center justify-center rounded-full bg-green-100 dark:bg-green-900/30">
                      <span className="text-sm font-semibold text-green-700 dark:text-green-300">
                        {action.availableCount}
                      </span>
                    </div>
                    <div>
                      <p className="text-sm font-medium">
                        {action.availableCount}{" "}
                        {action.availableCount === 1 ? "file" : "files"} will be
                        processed
                      </p>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        These files are in the correct state for this operation
                      </p>
                    </div>
                  </div>
                </div>
              )}

              {action.blockedCount > 0 && (
                <div className="overflow-hidden rounded-lg border border-yellow-200 dark:border-yellow-800">
                  <div className="border-b border-yellow-200 bg-yellow-50 px-4 py-2 dark:border-yellow-800 dark:bg-yellow-900/20">
                    <span className="text-sm font-medium text-yellow-800 dark:text-yellow-300">
                      Files already in progress
                    </span>
                  </div>
                  <div className="bg-white p-4 text-sm dark:bg-background">
                    {action.blockedCount}{" "}
                    {action.blockedCount === 1 ? "file is" : "files are"} locked
                    by another action and will be skipped.
                  </div>
                </div>
              )}

              {invalidCount > 0 && (
                <div className="overflow-hidden rounded-lg border border-red-200 dark:border-red-800">
                  <div className="border-b border-red-200 bg-red-50 px-4 py-2 dark:border-red-800 dark:bg-red-900/20">
                    <span className="text-sm font-medium text-red-800 dark:text-red-300">
                      Files that will be skipped
                    </span>
                  </div>
                  <div className="flex items-center bg-white p-4 dark:bg-background">
                    <div className="mr-3 flex h-8 w-8 items-center justify-center rounded-full bg-red-100 dark:bg-red-900/30">
                      <span className="text-sm font-semibold text-red-700 dark:text-red-300">
                        {invalidCount}
                      </span>
                    </div>
                    <div>
                      <p className="text-sm font-medium">
                        {invalidCount} {invalidCount === 1 ? "file" : "files"}{" "}
                        cannot be processed
                      </p>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        These files are in an incompatible state for this
                        operation
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>

          <DialogFooter className="gap-3 sm:justify-center">
            <DialogClose asChild>
              <Button variant="outline" className="min-w-24">
                Cancel
              </Button>
            </DialogClose>
            <Button
              onClick={() => {
                void handleAction();
              }}
              className={`min-w-24 ${
                action.destructive
                  ? "bg-red-500 text-white hover:bg-red-600"
                  : action.command === "resume"
                    ? "bg-green-500 text-white hover:bg-green-600"
                    : action.command === "pause"
                      ? "bg-yellow-500 text-white hover:bg-yellow-600"
                      : ""
              }`}
              disabled={action.availableCount === 0 || isMutating}
            >
              {action.label}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function actionIcon(command: BatchFileAction["command"]) {
  const className = "mr-2 h-4 w-4";
  if (command === "start") return <Download className={className} />;
  if (command === "resume") return <StepForward className={className} />;
  if (command === "pause") return <Pause className={className} />;
  if (command === "cancel") return <SquareX className={className} />;
  return <FileX className={className} />;
}

function actionClassName(command: BatchFileAction["command"]) {
  if (command === "resume") return "bg-green-500 hover:bg-green-600 text-white";
  if (command === "pause")
    return "bg-yellow-500 hover:bg-yellow-600 text-white";
  if (command === "cancel" || command === "remove") {
    return "bg-red-500 hover:bg-red-600 text-white";
  }
  return undefined;
}

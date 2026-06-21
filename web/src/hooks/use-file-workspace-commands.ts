import { useCallback, useMemo, useRef, useState } from "react";
import { toast } from "@/hooks/use-toast";
import {
  actionForCommand,
  actionsForFile,
  isCommandEligible,
  type FileActionDefinition,
} from "@/lib/file-command-policy";
import type {
  LiveFileStatus,
  LiveFileStatuses,
} from "@/lib/file-workspace-cache";
import { fileStatusKey } from "@/lib/file-workspace-cache";
import {
  type FileSourceAdapter,
  type FileCommand,
} from "@/lib/file-workspace-source";
import type { TelegramFile } from "@/lib/types";

export type FileAction = FileActionDefinition & {
  pending: boolean;
  run: () => Promise<void>;
};

export type BatchFileAction = FileActionDefinition & {
  files: TelegramFile[];
  count: number;
  availableCount: number;
  blockedCount: number;
  pending: boolean;
  run: () => Promise<BatchCommandOutcome>;
};

export type BatchCommandOutcome = {
  processed: number;
  failed: number;
  blocked: number;
  errors: string[];
};

export type TagCommandOutcome = { ok: true } | { ok: false; error: string };

type UseFileWorkspaceCommandsInput = {
  adapter: FileSourceAdapter;
  setStatuses: React.Dispatch<React.SetStateAction<LiveFileStatuses>>;
  patchFiles: (
    uniqueIds: ReadonlySet<string>,
    patch: Partial<TelegramFile>,
  ) => Promise<void>;
};

export function useFileWorkspaceCommands({
  adapter,
  setStatuses,
  patchFiles,
}: UseFileWorkspaceCommandsInput) {
  const pendingRef = useRef(new Set<string>());
  const [, setPendingVersion] = useState(0);

  const isPending = useCallback(
    (file: TelegramFile) => pendingRef.current.has(adapter.identity(file)),
    [adapter],
  );

  const acquire = useCallback(
    (files: TelegramFile[], command: FileCommand) => {
      const admitted = files.filter(
        (file) => isCommandEligible(file, command) && !isPending(file),
      );
      admitted.forEach((file) =>
        pendingRef.current.add(adapter.identity(file)),
      );
      if (admitted.length > 0) setPendingVersion((version) => version + 1);
      return admitted;
    },
    [adapter, isPending],
  );

  const release = useCallback(
    (files: TelegramFile[]) => {
      files.forEach((file) =>
        pendingRef.current.delete(adapter.identity(file)),
      );
      if (files.length > 0) setPendingVersion((version) => version + 1);
    },
    [adapter],
  );

  const run = useCallback(
    async (file: TelegramFile, command: FileCommand) => {
      const [admitted] = acquire([file], command);
      if (!admitted) return;
      applyOptimisticStatus(setStatuses, [admitted], command);
      try {
        await adapter.command(admitted, command);
      } catch (error) {
        showCommandError(actionForCommand(command).label, error);
      } finally {
        release([admitted]);
      }
    },
    [acquire, adapter, release, setStatuses],
  );

  const runBatch = useCallback(
    async (
      files: TelegramFile[],
      command: FileCommand,
    ): Promise<BatchCommandOutcome> => {
      const eligible = files.filter((file) => isCommandEligible(file, command));
      const admitted = acquire(eligible, command);
      const blocked = eligible.length - admitted.length;
      if (admitted.length === 0) {
        return { processed: 0, failed: 0, blocked, errors: [] };
      }
      applyOptimisticStatus(setStatuses, admitted, command);

      try {
        const result = await adapter.batchCommand(admitted, command);
        return { ...result, blocked, errors: [] };
      } catch (error) {
        return {
          processed: 0,
          failed: admitted.length,
          blocked,
          errors: [errorMessage(error)],
        };
      } finally {
        release(admitted);
      }
    },
    [acquire, adapter, release, setStatuses],
  );

  const updateTags = useCallback(
    async (file: TelegramFile, tags: string[]): Promise<TagCommandOutcome> => {
      const value = tags.join(",");
      try {
        const uniqueId = adapter.tagIdentity(file);
        await patchFiles(new Set([uniqueId]), { tags: value });
        await adapter.tags(file, value);
        return { ok: true };
      } catch (error) {
        return { ok: false, error: errorMessage(error) };
      }
    },
    [adapter, patchFiles],
  );

  const updateTagsMany = useCallback(
    async (
      files: TelegramFile[],
      tags: string[],
    ): Promise<TagCommandOutcome> => {
      const value = tags.join(",");
      try {
        const uniqueIds = new Set(files.map(adapter.tagIdentity));
        await patchFiles(uniqueIds, {
          tags: value,
        });
        await adapter.batchTags(files, value);
        return { ok: true };
      } catch (error) {
        return { ok: false, error: errorMessage(error) };
      }
    },
    [adapter, patchFiles],
  );

  const actions = useCallback(
    (file: TelegramFile): FileAction[] =>
      actionsForFile(file).map((definition) => ({
        ...definition,
        pending: isPending(file),
        run: () => run(file, definition.command),
      })),
    [isPending, run],
  );

  const batchActions = useCallback(
    (files: TelegramFile[]): BatchFileAction[] =>
      (Object.keys(BATCH_ORDER) as FileCommand[])
        .sort((left, right) => BATCH_ORDER[left] - BATCH_ORDER[right])
        .map((command) => {
          const definition = actionForCommand(command);
          const eligible = files.filter((file) =>
            isCommandEligible(file, command),
          );
          const blockedCount = eligible.filter(isPending).length;
          return {
            ...definition,
            files: eligible,
            count: eligible.length,
            availableCount: eligible.length - blockedCount,
            blockedCount,
            pending: blockedCount === eligible.length,
            confirm:
              definition.confirm ||
              (command === "start" && eligible.length > 5),
            run: () => runBatch(eligible, command),
          };
        })
        .filter((descriptor) => descriptor.count > 0),
    [isPending, runBatch],
  );

  return useMemo(
    () => ({
      actions,
      batchActions,
      runBatch,
      updateTags,
      updateTagsMany,
    }),
    [actions, batchActions, runBatch, updateTags, updateTagsMany],
  );
}

const BATCH_ORDER: Record<FileCommand, number> = {
  start: 0,
  resume: 1,
  pause: 2,
  cancel: 3,
  remove: 4,
};

function applyOptimisticStatus(
  setStatuses: React.Dispatch<React.SetStateAction<LiveFileStatuses>>,
  files: TelegramFile[],
  command: FileCommand,
) {
  setStatuses((current) => {
    const next = { ...current };
    files.forEach((file) => {
      const key = fileStatusKey(file.id, file.uniqueId);
      next[key] = {
        fileId: file.id,
        downloadStatus: file.downloadStatus,
        localPath: file.localPath,
        completionDate: file.completionDate,
        downloadedSize: file.downloadedSize,
        transferStatus: file.transferStatus,
        thumbnailFile: file.thumbnailFile,
        ...current[key],
        ...optimisticPatch(file, command),
      };
    });
    return next;
  });
}

function optimisticPatch(
  file: TelegramFile,
  command: FileCommand,
): Partial<LiveFileStatus> {
  if (command === "start" || command === "resume") {
    return { downloadStatus: "downloading", removed: false };
  }
  if (command === "pause") return { downloadStatus: "paused" };
  if (command === "cancel") {
    return { downloadStatus: "idle", downloadedSize: 0 };
  }
  return {
    downloadStatus: "idle",
    localPath: "",
    completionDate: 0,
    downloadedSize: 0,
    transferStatus: "idle",
    removed: file.originalDeleted,
  };
}

/**
 * Translates a batch command result into a user-facing toast. Returns true when
 * real work happened (so callers can clear their selection), false otherwise.
 */
export function reportBatchOutcome(
  label: string,
  outcome: BatchCommandOutcome,
  options: { itemNoun?: string } = {},
): boolean {
  const noun = options.itemNoun ?? "file";
  const plural = (count: number) => (count === 1 ? noun : `${noun}s`);
  const { processed, failed, blocked, errors } = outcome;

  if (processed === 0 && failed === 0 && blocked > 0) {
    toast({
      title: `${label} already in progress`,
      description: `${blocked} ${plural(blocked)} ${
        blocked === 1 ? "is" : "are"
      } already being processed.`,
      variant: "warning",
    });
    return false;
  }

  if (processed === 0 && failed > 0) {
    toast({
      title: `${label} failed`,
      description:
        errors.join(" ") || `None of the selected ${noun}s could be processed.`,
      variant: "error",
    });
    return false;
  }

  const skipped = failed + blocked;
  if (skipped > 0) {
    toast({
      title: `${label} completed with skips`,
      description: [
        `Processed ${processed} ${plural(processed)} and skipped ${skipped}.`,
        ...errors,
      ].join(" "),
      variant: "warning",
    });
    return true;
  }

  toast({
    title: `${label} completed`,
    description: `Successfully processed ${processed} ${plural(processed)}.`,
    variant: "success",
  });
  return true;
}

function showCommandError(label: string, error: unknown) {
  toast({
    title: `${label} failed`,
    description: error instanceof Error ? error.message : "The request failed.",
    variant: "error",
  });
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "The request failed.";
}

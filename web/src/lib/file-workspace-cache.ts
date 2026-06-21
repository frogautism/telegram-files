import type {
  DownloadStatus,
  TelegramFile,
  Thumbnail,
  TransferStatus,
} from "@/lib/types";

export type FilePage = {
  files: TelegramFile[];
  count: number;
  nextFromMessageId: number;
};

export type LiveFileStatus = {
  fileId: number;
  downloadStatus: DownloadStatus;
  localPath?: string;
  completionDate?: number;
  downloadedSize: number;
  transferStatus?: TransferStatus;
  thumbnailFile?: Thumbnail;
  removed?: boolean;
};

export type FileStatusEvent = LiveFileStatus & {
  source?: "telegram" | "douyin";
  uniqueId: string;
};

export type LiveFileStatuses = Record<string, LiveFileStatus>;

const fileCacheKey = (file: TelegramFile) =>
  `${file.telegramId}:${file.chatId}:${file.messageId}:${file.uniqueId}`;

export const fileStatusKey = (fileId: number | undefined, uniqueId: string) =>
  `${fileId ?? 0}:${uniqueId}`;

export function rebuildVisiblePages(
  currentPages: FilePage[],
  freshFirstPage: FilePage,
) {
  if (currentPages.length === 0) {
    return [freshFirstPage];
  }

  const pageLengths = currentPages.map((page, index) =>
    index === 0 ? freshFirstPage.files.length : page.files.length,
  );
  const totalSlots = pageLengths.reduce((sum, length) => sum + length, 0);
  if (totalSlots === 0) {
    return [freshFirstPage];
  }

  const seen = new Set<string>();
  const mergedFiles = [
    ...freshFirstPage.files,
    ...currentPages.flatMap((page) => page.files),
  ].filter((file) => {
    const key = fileCacheKey(file);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  const trimmedFiles = mergedFiles.slice(0, totalSlots);
  const currentLastPage = currentPages.at(-1);
  const hasMoreBeyondLoaded =
    mergedFiles.length > trimmedFiles.length ||
    freshFirstPage.count > trimmedFiles.length ||
    (currentLastPage?.nextFromMessageId ?? 0) !== 0;

  let offset = 0;
  return currentPages.map((page, index) => {
    const pageLength = pageLengths[index] ?? 0;
    const pageFiles = trimmedFiles.slice(offset, offset + pageLength);
    offset += pageLength;
    const lastFile = pageFiles.at(-1);
    const isLastPage = index === currentPages.length - 1;
    const hasLoadedNextPage = offset < trimmedFiles.length;

    return {
      ...page,
      files: pageFiles,
      count: isLastPage
        ? hasMoreBeyondLoaded
          ? Math.max(
              trimmedFiles.length + 1,
              freshFirstPage.count,
              currentLastPage?.count ?? 0,
            )
          : trimmedFiles.length
        : page.count,
      nextFromMessageId: lastFile
        ? hasLoadedNextPage || (isLastPage && hasMoreBeyondLoaded)
          ? lastFile.messageId
          : 0
        : 0,
    };
  });
}

export function patchFiles(
  pages: FilePage[] | undefined,
  uniqueIds: ReadonlySet<string>,
  patch: Partial<TelegramFile>,
) {
  if (!pages) return [];
  return pages.map((page) => ({
    ...page,
    files: page.files.map((file) =>
      uniqueIds.has(file.uniqueId) ? { ...file, ...patch } : file,
    ),
  }));
}

export function mergeStatusEvent(
  current: LiveFileStatuses,
  pages: FilePage[] | undefined,
  event: FileStatusEvent,
) {
  const visibleIds = new Set<number>();
  pages?.forEach((page) =>
    page.files.forEach((file) => {
      if (file.uniqueId === event.uniqueId) visibleIds.add(file.id);
    }),
  );
  const exactKey = fileStatusKey(event.fileId, event.uniqueId);
  const aliasKey =
    visibleIds.size === 1
      ? fileStatusKey([...visibleIds][0], event.uniqueId)
      : undefined;
  const nextStatus = event.removed
    ? removedStatus(event.fileId)
    : mergeStatus(current[exactKey], event);

  return {
    ...current,
    [exactKey]: nextStatus,
    ...(aliasKey && aliasKey !== exactKey
      ? {
          [aliasKey]: event.removed
            ? removedStatus(event.fileId)
            : mergeStatus(current[aliasKey], event),
        }
      : {}),
  };
}

export function materializeFiles(
  pages: FilePage[] | undefined,
  statuses: LiveFileStatuses,
) {
  const files =
    pages?.flatMap((page) =>
      page.files.flatMap((file) => {
        const status = statuses[fileStatusKey(file.id, file.uniqueId)];
        if (file.originalDeleted && status?.removed) return [];
        return [
          {
            ...file,
            id: status?.fileId ?? file.id,
            downloadStatus: status?.downloadStatus ?? file.downloadStatus,
            localPath: status?.localPath ?? file.localPath,
            completionDate: status?.completionDate ?? file.completionDate,
            downloadedSize: status?.downloadedSize ?? file.downloadedSize,
            transferStatus: status?.transferStatus ?? file.transferStatus,
            thumbnailFile: status?.thumbnailFile ?? file.thumbnailFile,
          },
        ];
      }),
    ) ?? [];

  files.forEach((file, index) => {
    file.prev = files[index - 1];
    file.next = files[index + 1];
  });
  return files;
}

function removedStatus(fileId: number): LiveFileStatus {
  return {
    fileId,
    downloadStatus: "idle",
    localPath: "",
    completionDate: 0,
    downloadedSize: 0,
    transferStatus: "idle",
    removed: true,
  };
}

function mergeStatus(
  current: LiveFileStatus | undefined,
  event: FileStatusEvent,
): LiveFileStatus {
  return {
    fileId: event.fileId,
    downloadStatus: event.downloadStatus ?? current?.downloadStatus ?? "idle",
    localPath: event.localPath ?? current?.localPath,
    completionDate: event.completionDate ?? current?.completionDate,
    downloadedSize: event.downloadedSize ?? current?.downloadedSize ?? 0,
    transferStatus: event.transferStatus ?? current?.transferStatus,
    thumbnailFile: event.thumbnailFile ?? current?.thumbnailFile,
  };
}

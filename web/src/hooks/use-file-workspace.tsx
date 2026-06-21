import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import useSWRInfinite from "swr/infinite";
import { useFileWorkspaceCommands } from "@/hooks/use-file-workspace-commands";
import { useLocalStorage } from "@/hooks/use-local-storage";
import { useWebsocket } from "@/hooks/use-websocket";
import { request } from "@/lib/api";
import { getFilesApiPath, isGroupChatId } from "@/lib/chat-target";
import {
  type FilePage,
  type FileStatusEvent,
  type LiveFileStatuses,
  materializeFiles,
  mergeStatusEvent,
  patchFiles,
  rebuildVisiblePages,
} from "@/lib/file-workspace-cache";
import { createFileSourceAdapter } from "@/lib/file-workspace-source";
import type { FileFilter, TelegramFile } from "@/lib/types";
import { WebSocketMessageType } from "@/lib/websocket-types";

const DEFAULT_FILTERS: FileFilter = {
  search: "",
  type: "media",
  downloadStatus: undefined,
  transferStatus: undefined,
  alreadyDownloaded: false,
  offline: false,
  tags: [],
};

type FileWorkspaceBaseConfig = {
  link?: string;
};

export type FileWorkspaceConfig = FileWorkspaceBaseConfig &
  (
    | {
        source?: "telegram";
        accountId: string;
        chatId: string;
        messageThreadId?: number;
        sourceId?: never;
      }
    | { source: "douyin"; sourceId?: string }
  );

export function useFileWorkspace(config: FileWorkspaceConfig) {
  const isDouyin = config.source === "douyin";
  const source = isDouyin ? "douyin" : "telegram";
  const accountId = isDouyin ? undefined : config.accountId;
  const chatId = isDouyin ? undefined : config.chatId;
  const messageThreadId = isDouyin ? undefined : config.messageThreadId;
  const link = config.link;
  const noAccountSpecified =
    source === "telegram" && accountId === "-1" && chatId === "-1";
  const isGroupChat = isGroupChatId(chatId);
  const listUrl = getListUrl(config);
  const sourceAdapter = useMemo(
    () => createFileSourceAdapter(source),
    [source],
  );
  const { lastJsonMessage } = useWebsocket();
  const [statuses, setStatuses] = useState<LiveFileStatuses>({});
  const [filters, storeFilters, clearFilters] = useLocalStorage<FileFilter>(
    "telegramFileListFilter",
    {
      ...DEFAULT_FILTERS,
      offline: source === "douyin" || noAccountSpecified || isGroupChat,
    },
  );

  const searchParams = useCallback(
    () =>
      buildSearchParams(filters, {
        isGroupChat,
        link,
        messageThreadId,
      }),
    [filters, isGroupChat, link, messageThreadId],
  );
  const getKey = useCallback(
    (page: number, previousPage: FilePage | null) =>
      getPageKey({
        page,
        previousPage,
        filters,
        isGroupChat,
        listUrl,
        searchParams,
      }),
    [filters, isGroupChat, listUrl, searchParams],
  );

  const {
    data: pages,
    isLoading,
    isValidating,
    size,
    setSize,
    error,
    mutate,
  } = useSWRInfinite<FilePage, Error>(getKey, {
    revalidateFirstPage: false,
    keepPreviousData: true,
    revalidateOnFocus: false,
    revalidateOnReconnect: false,
  });

  const refreshVisiblePages = useCallback(async () => {
    const firstPageKey = getKey(0, null);
    if (!firstPageKey) return;
    const freshFirstPage = await request<FilePage>(firstPageKey);
    await mutate(
      (current) =>
        current?.length
          ? rebuildVisiblePages(current, freshFirstPage)
          : [freshFirstPage],
      false,
    );
  }, [getKey, mutate]);

  const patchVisibleFiles = useCallback(
    async (uniqueIds: ReadonlySet<string>, patch: Partial<TelegramFile>) => {
      await mutate((current) => patchFiles(current, uniqueIds, patch), false);
    },
    [mutate],
  );
  const fileCommands = useFileWorkspaceCommands({
    adapter: sourceAdapter,
    setStatuses,
    patchFiles: patchVisibleFiles,
  });

  useEffect(() => {
    if (lastJsonMessage?.type !== WebSocketMessageType.FILE_STATUS) return;
    const event = lastJsonMessage.data as FileStatusEvent;
    if (!eventMatchesSource(event, source ?? "telegram")) return;
    setStatuses((current) => mergeStatusEvent(current, pages, event));
  }, [lastJsonMessage, pages, source]);

  useEffect(() => {
    if (source !== "telegram" || !accountId || !chatId) return;
    if (lastJsonMessage?.type !== WebSocketMessageType.CHAT_UPDATE) return;
    const update = lastJsonMessage.data as {
      telegramId?: string;
      chatId?: string;
    };
    if ((update.telegramId ?? "") !== accountId) return;
    if (!isGroupChat && (update.chatId ?? "") !== chatId) return;

    const timeoutId = window.setTimeout(() => {
      void refreshVisiblePages();
    }, 300);
    return () => window.clearTimeout(timeoutId);
  }, [
    accountId,
    chatId,
    isGroupChat,
    lastJsonMessage,
    refreshVisiblePages,
    source,
  ]);

  useEffect(() => {
    if (
      (source === "douyin" || noAccountSpecified || isGroupChat) &&
      !filters.offline
    ) {
      storeFilters((current) => ({ ...current, offline: true }));
    }
  }, [filters.offline, isGroupChat, noAccountSpecified, source, storeFilters]);

  const files = useMemo(
    () => materializeFiles(pages, statuses).map(sourceAdapter.normalize),
    [pages, sourceAdapter, statuses],
  );
  const hasMore = useMemo(() => calculateHasMore(pages), [pages]);
  const isLoadingMore = Boolean(pages) && size > (pages?.length ?? 0);

  const loadMore = useCallback(async () => {
    if (isLoading || isValidating || !hasMore || error) return;
    await setSize((current) => current + 1);
  }, [error, hasMore, isLoading, isValidating, setSize]);

  const setFilters = useCallback(
    async (next: FileFilter) => {
      if (JSON.stringify(next) === JSON.stringify(filters)) return;
      storeFilters(next);
      await setSize(1);
    },
    [filters, setSize, storeFilters],
  );

  const reload = useCallback(async () => {
    setStatuses({});
    if (pages?.length) await refreshVisiblePages();
    else await mutate();
  }, [mutate, pages, refreshVisiblePages]);

  return {
    query: {
      size,
      files,
      filters,
      isLoading: (!pages && (isLoading || isValidating)) || isLoadingMore,
      isRefreshing: Boolean(pages) && isValidating && !isLoadingMore,
      isLoadingMore,
      hasMore,
      totalCount: pages?.[0]?.count ?? files.length,
    },
    commands: {
      ...fileCommands,
      reload,
      setFilters,
      clearFilters,
      loadMore,
    },
  };
}

export type FileWorkspace = ReturnType<typeof useFileWorkspace>;

const FileWorkspaceContext = createContext<FileWorkspace | null>(null);

export function FileWorkspaceProvider({
  workspace,
  children,
}: {
  workspace: FileWorkspace;
  children: ReactNode;
}) {
  return (
    <FileWorkspaceContext.Provider value={workspace}>
      {children}
    </FileWorkspaceContext.Provider>
  );
}

export function useCurrentFileWorkspace() {
  const workspace = useContext(FileWorkspaceContext);
  if (!workspace) {
    throw new Error(
      "useCurrentFileWorkspace must be used inside FileWorkspaceProvider.",
    );
  }
  return workspace;
}

function getListUrl(config: FileWorkspaceConfig) {
  if (config.source === "douyin") {
    return config.sourceId
      ? `/douyin/sources/${config.sourceId}/files`
      : "/douyin/files";
  }
  return config.accountId === "-1" && config.chatId === "-1"
    ? "/files"
    : getFilesApiPath(config.accountId, config.chatId);
}

function buildSearchParams(
  filters: FileFilter,
  options: {
    isGroupChat: boolean;
    messageThreadId?: number;
    link?: string;
  },
) {
  const { isGroupChat, messageThreadId, link } = options;
  const sort = filters.sort ?? (isGroupChat ? "date" : undefined);
  const order = filters.order ?? (isGroupChat ? "desc" : undefined);
  return new URLSearchParams({
    ...(filters.search && { search: filters.search }),
    ...(filters.type && { type: filters.type }),
    ...(filters.downloadStatus && { downloadStatus: filters.downloadStatus }),
    ...(filters.transferStatus && { transferStatus: filters.transferStatus }),
    ...(filters.alreadyDownloaded && { alreadyDownloaded: "true" }),
    ...(filters.offline && { offline: "true" }),
    ...(filters.tags.length > 0 && { tags: filters.tags.join(",") }),
    ...(messageThreadId && { messageThreadId: messageThreadId.toString() }),
    ...(link && { link }),
    ...(filters.dateType && { dateType: filters.dateType }),
    ...(filters.dateRange && { dateRange: filters.dateRange.join(",") }),
    ...(filters.sizeRange && { sizeRange: filters.sizeRange.join(",") }),
    ...(filters.sizeUnit && { sizeUnit: filters.sizeUnit }),
    ...(sort && { sort }),
    ...(order && { order }),
  });
}

function getPageKey({
  page,
  previousPage,
  filters,
  isGroupChat,
  listUrl,
  searchParams,
}: {
  page: number;
  previousPage: FilePage | null;
  filters: FileFilter;
  isGroupChat: boolean;
  listUrl: string;
  searchParams: () => URLSearchParams;
}) {
  const params = searchParams();
  if (page === 0) return `${listUrl}?${params.toString()}`;
  if (!previousPage) return null;

  params.set("fromMessageId", previousPage.nextFromMessageId.toString());
  const lastFile = previousPage.files.at(-1);
  if ((filters.offline || isGroupChat) && lastFile) {
    const sort = filters.sort ?? (isGroupChat ? "date" : undefined);
    const cursor =
      sort === "size"
        ? lastFile.size
        : sort === "completion_date"
          ? lastFile.completionDate
          : sort === "date"
            ? lastFile.date
            : sort === "reaction_count"
              ? lastFile.reactionCount
              : undefined;
    if (cursor !== undefined) params.set("fromSortField", cursor.toString());
  }
  return `${listUrl}?${params.toString()}`;
}

function calculateHasMore(pages: FilePage[] | undefined) {
  if (!pages?.length) return true;
  const fetched = pages.reduce((total, page) => total + page.files.length, 0);
  const lastPage = pages.at(-1);
  return Boolean(
    lastPage && lastPage.count > fetched && lastPage.nextFromMessageId !== 0,
  );
}

function eventMatchesSource(
  event: FileStatusEvent,
  source: "telegram" | "douyin",
) {
  if (source === "douyin") return !event.source || event.source === "douyin";
  return event.source !== "douyin";
}

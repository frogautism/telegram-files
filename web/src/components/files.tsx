"use client";
import FileList from "@/components/mobile/file-list";
import { FileTable } from "@/components/file-table";
import useIsMobile from "@/hooks/use-is-mobile";

export default function Files({
  accountId,
  chatId,
  messageThreadId,
  link,
  source = "telegram",
  sourceId,
  onRefreshSource,
  refreshSignal,
}: {
  accountId: string;
  chatId: string;
  messageThreadId?: number;
  link?: string;
  source?: "telegram" | "douyin";
  sourceId?: string;
  onRefreshSource?: () => Promise<void>;
  refreshSignal?: number;
}) {
  const isMobile = useIsMobile();

  if (isMobile) {
    return (
      <FileList
        accountId={accountId}
        chatId={chatId}
        link={link}
        source={source}
        sourceId={sourceId}
        onRefreshSource={onRefreshSource}
        refreshSignal={refreshSignal}
      />
    );
  } else {
    return (
      <FileTable
        accountId={accountId}
        chatId={chatId}
        messageThreadId={messageThreadId}
        link={link}
        source={source}
        sourceId={sourceId}
        onRefreshSource={onRefreshSource}
        refreshSignal={refreshSignal}
      />
    );
  }
}

"use client";
import FileList from "@/components/mobile/file-list";
import { FileTable } from "@/components/file-table";
import useIsMobile from "@/hooks/use-is-mobile";
import type { FileWorkspaceConfig } from "@/hooks/use-file-workspace";

type FilesProps = FileWorkspaceConfig & {
  onRefreshSource?: () => Promise<void>;
  refreshSignal?: number;
};

export default function Files(props: FilesProps) {
  const isMobile = useIsMobile();
  const commonProps = {
    link: props.link,
    onRefreshSource: props.onRefreshSource,
    refreshSignal: props.refreshSignal,
  };

  if (isMobile) {
    return props.source === "douyin" ? (
      <FileList {...commonProps} source="douyin" sourceId={props.sourceId} />
    ) : (
      <FileList
        {...commonProps}
        accountId={props.accountId}
        chatId={props.chatId}
        source="telegram"
      />
    );
  }
  return props.source === "douyin" ? (
    <FileTable {...commonProps} source="douyin" sourceId={props.sourceId} />
  ) : (
    <FileTable
      {...commonProps}
      accountId={props.accountId}
      chatId={props.chatId}
      messageThreadId={props.messageThreadId}
      source="telegram"
    />
  );
}

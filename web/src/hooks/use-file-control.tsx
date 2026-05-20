import { type TelegramFile } from "@/lib/types";
import useSWRMutation from "swr/mutation";
import { POST } from "@/lib/api";
import { toast } from "@/hooks/use-toast";

export function useFileControl(file: TelegramFile) {
  const isDouyin = file.source === "douyin";
  const { trigger: startDownload, isMutating: starting } = useSWRMutation(
    isDouyin ? "/douyin/file/start-download" : `/${file.telegramId}/file/start-download`,
    (
      key: string,
      {
        arg,
      }: {
        arg: {
          chatId: number;
          messageId: number;
          fileId: number;
          uniqueId?: string;
        };
      },
    ) => POST(key, arg),
  );
  const { trigger: cancelDownload, isMutating: cancelling } = useSWRMutation(
    isDouyin ? "/douyin/file/cancel-download" : `/${file.telegramId}/file/cancel-download`,
    (key: string, { arg }: { arg: { fileId: number; uniqueId?: string } }) => POST(key, arg),
  );
  const { trigger: togglePauseDownload, isMutating: togglingPause } =
    useSWRMutation(
      isDouyin
        ? "/douyin/file/toggle-pause-download"
        : `/${file.telegramId}/file/toggle-pause-download`,
      (key: string, { arg }: { arg: { fileId: number; uniqueId?: string; isPaused: boolean } }) =>
        POST(key, arg),
    );
  const { trigger: removeFile, isMutating: removing } = useSWRMutation(
    isDouyin ? "/douyin/file/remove" : `/${file.telegramId}/file/remove`,
    (key: string, { arg }: { arg: { fileId: number; uniqueId: string } }) =>
      POST(key, arg),
  );

  const downloadControl = {
    cancel: (fileId: number) => {
      void cancelDownload({ fileId, uniqueId: file.uniqueId });
    },
    start: (fileId: number) => {
      if (file) {
        if (file.downloadStatus !== "idle" && file.downloadStatus !== "error") {
          return;
        }
        if (!file.uniqueId || file.uniqueId.trim() === "") {
          toast({
            variant: "error",
            description: "☹️Sorry, this file cannot be downloaded",
          });
          return;
        }
        void startDownload({
          chatId: file.chatId,
          fileId,
          messageId: file.messageId,
          uniqueId: file.uniqueId,
        });
      }
    },
    togglePause: (fileId: number) => {
      if (file) {
        if (
          file.downloadStatus !== "downloading" &&
          file.downloadStatus !== "paused"
        ) {
          return;
        }
        void togglePauseDownload({
          fileId,
          uniqueId: file.uniqueId,
          isPaused: file.downloadStatus === "downloading",
        });
      }
    },
    remove: (fileId: number) => {
      if (file) {
        void removeFile({ fileId, uniqueId: file.uniqueId });
      }
    },
    cancelling,
    starting,
    togglingPause,
    removing,
  };

  return {
    ...downloadControl,
  };
}

import { type TelegramFile } from "@/lib/types";
import type { FileAction } from "@/hooks/use-file-workspace-commands";
import { useCurrentFileWorkspace } from "@/hooks/use-file-workspace";
import { Button } from "@/components/ui/button";
import {
  ArrowDown,
  FileX,
  Loader2,
  MessageSquareText,
  Pause,
  SquareX,
  StepForward,
  Unlink,
} from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
  TooltipWrapper,
} from "@/components/ui/tooltip";
import { type ReactNode } from "react";
import prettyBytes from "pretty-bytes";
import { AnimatePresence, motion } from "framer-motion";
import { Badge } from "@/components/ui/badge";
import { useRouter } from "next/navigation";
import { useTelegramMethod } from "@/hooks/use-telegram-method";
import { toast } from "@/hooks/use-toast";
import { useMaybeTelegramChat } from "@/hooks/use-telegram-chat";
import { useSettings } from "@/hooks/use-settings";

interface ActionButtonProps {
  tooltipText: string;
  icon: ReactNode;
  onClick: () => void;
  loading: boolean;
  isMobile?: boolean;
}

const ActionButton = ({
  tooltipText,
  icon,
  onClick,
  loading,
  isMobile,
}: ActionButtonProps) => (
  <Tooltip>
    <TooltipTrigger asChild>
      <Button
        variant={isMobile ? "default" : "ghost"}
        size={isMobile ? "icon" : "xs"}
        onClick={onClick}
      >
        {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : icon}
      </Button>
    </TooltipTrigger>
    <TooltipContent>
      <p>{tooltipText}</p>
    </TooltipContent>
  </Tooltip>
);

export default function FileControl({
  file,
  downloadSpeed,
  hovered,
  isMobile,
}: {
  file: TelegramFile;
  downloadSpeed?: number;
  hovered?: boolean;
  isMobile?: boolean;
}) {
  const router = useRouter();
  const { executeMethod, isMethodExecuting } = useTelegramMethod();
  const { settings } = useSettings();
  const { chat } = useMaybeTelegramChat() ?? {};
  const showDownloadInfo =
    !hovered &&
    !file.originalDeleted &&
    (file.downloadStatus === "downloading" || file.downloadStatus === "paused");
  const iconSize = isMobile ? "!h-3 !w-3" : "h-4 w-4";

  const { commands } = useCurrentFileWorkspace();
  const fileActions = commands.actions(file);
  const removeAction = fileActions.find(
    (action) => action.command === "remove",
  );

  const removeBtnProps = removeAction
    ? actionButtonProps(removeAction, iconSize)
    : undefined;

  const replyBtnProps: ActionButtonProps = {
    onClick: () => {
      if (file.threadChatId !== 0 && file.messageThreadId !== 0) {
        router.push(
          `/accounts?id=${file.telegramId}&chatId=${file.threadChatId}&messageThreadId=${file.messageThreadId}`,
        );
        return;
      } else {
        void executeMethod({
          data: {
            chatId: file.chatId,
            messageId: file.messageId,
          },
          method: "GetMessageThread",
        })
          .then((result) => {
            if (!result) {
              toast({
                variant: "error",
                description: "Failed to get message thread",
              });
              return;
            }
            const { chatId, messageThreadId } = result as {
              chatId: number;
              messageThreadId: number;
            };
            router.push(
              `/accounts?id=${file.telegramId}&chatId=${chatId}&messageThreadId=${messageThreadId}`,
            );
          })
          .catch(() => {
            toast({
              variant: "error",
              description: "Failed to get message thread",
            });
          });
      }
    },
    tooltipText: "View Comments",
    icon: <MessageSquareText className={iconSize} />,
    loading: isMethodExecuting,
  };

  const actionButtons = file.originalDeleted ? (
    <div className="w-full">
      <div
        className="flex w-full items-center justify-end space-x-4 md:justify-around md:space-x-2"
        onClick={(e) => e.preventDefault()}
      >
        <TooltipWrapper content="Missing Original Message">
          <Badge className="bg-yellow-300 text-yellow-900 hover:bg-yellow-400 dark:bg-yellow-800 dark:text-yellow-300 dark:hover:bg-yellow-700">
            <Unlink className="h-4 w-4" />
          </Badge>
        </TooltipWrapper>
        {removeBtnProps && (
          <ActionButton isMobile={isMobile} {...removeBtnProps} />
        )}
      </div>
    </div>
  ) : (
    <div className="w-full">
      <div
        className="flex w-full items-center justify-end space-x-4 md:justify-around md:space-x-2"
        onClick={(e) => e.preventDefault()}
      >
        {file.hasReply &&
          (chat?.kind === "group" || chat?.type === "channel") && (
            <ActionButton isMobile={isMobile} {...replyBtnProps} />
          )}
        {fileActions.map((action) => (
          <ActionButton
            key={action.command}
            isMobile={isMobile}
            {...actionButtonProps(action, iconSize)}
          />
        ))}
      </div>
    </div>
  );

  if (isMobile) {
    return <TooltipProvider>{actionButtons}</TooltipProvider>;
  }

  return (
    <TooltipProvider>
      <div className="relative h-6 overflow-hidden">
        <AnimatePresence mode="wait">
          {showDownloadInfo ? (
            <motion.div
              key="downloadInfo"
              initial={{ y: 20, opacity: 0 }}
              animate={{ y: 0, opacity: 1 }}
              exit={{ y: -20, opacity: 0 }}
              transition={{ duration: 0.2, ease: "easeOut" }}
              className="absolute w-full"
            >
              <span className="text-nowrap text-xs">
                {file.downloadStatus === "downloading" && downloadSpeed
                  ? `${prettyBytes(downloadSpeed, { bits: settings?.speedUnits === "bits" })}/s`
                  : prettyBytes(file.downloadedSize)}
              </span>
            </motion.div>
          ) : (
            <motion.div
              key="actionButtons"
              initial={{ y: 20, opacity: 0 }}
              animate={{ y: 0, opacity: 1 }}
              exit={{ y: -20, opacity: 0 }}
              transition={{ duration: 0.2, ease: "easeOut" }}
              className="absolute w-full"
            >
              {actionButtons}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </TooltipProvider>
  );
}

export function MobileFileControl({ file }: { file: TelegramFile }) {
  const { commands } = useCurrentFileWorkspace();
  const actions = commands.actions(file);

  return (
    <div className="flex w-full items-center justify-between space-x-2">
      {actions.map((action) => (
        <Button
          key={action.command}
          className="w-full"
          variant={action.destructive ? "destructive" : "default"}
          onClick={() => void action.run()}
        >
          {action.pending ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            actionIcon(action.command, "h-4 w-4")
          )}
          <span className="ml-2">{action.label}</span>
        </Button>
      ))}
    </div>
  );
}

function actionButtonProps(
  action: FileAction,
  iconSize: string,
): ActionButtonProps {
  return {
    onClick: () => void action.run(),
    tooltipText: action.tooltip,
    icon: actionIcon(action.command, iconSize),
    loading: action.pending,
  };
}

function actionIcon(command: FileAction["command"], className: string) {
  if (command === "start") return <ArrowDown className={className} />;
  if (command === "pause") return <Pause className={className} />;
  if (command === "resume") return <StepForward className={className} />;
  if (command === "cancel") return <SquareX className={className} />;
  return <FileX className={className} />;
}

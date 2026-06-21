import type { DownloadStatus, TelegramFile } from "@/lib/types";
import type { FileCommand } from "@/lib/file-workspace-source";

export type FileActionDefinition = {
  command: FileCommand;
  label: string;
  tooltip: string;
  destructive?: boolean;
  confirm: boolean;
};

const ACTIONS: Record<FileCommand, FileActionDefinition> = {
  start: {
    command: "start",
    label: "Download",
    tooltip: "Start Download",
    confirm: false,
  },
  pause: {
    command: "pause",
    label: "Pause",
    tooltip: "Pause",
    confirm: false,
  },
  resume: {
    command: "resume",
    label: "Continue",
    tooltip: "Resume",
    confirm: false,
  },
  cancel: {
    command: "cancel",
    label: "Cancel",
    tooltip: "Cancel",
    destructive: true,
    confirm: true,
  },
  remove: {
    command: "remove",
    label: "Delete",
    tooltip: "Remove",
    destructive: true,
    confirm: true,
  },
};

const COMMANDS_BY_STATUS: Record<DownloadStatus, FileCommand[]> = {
  idle: ["start"],
  error: ["start"],
  downloading: ["pause", "cancel"],
  paused: ["resume", "cancel"],
  completed: ["remove"],
};

export function actionsForFile(file: TelegramFile) {
  if (file.originalDeleted) {
    return [ACTIONS.remove];
  }
  return COMMANDS_BY_STATUS[file.downloadStatus].map(
    (command) => ACTIONS[command],
  );
}

export function actionForCommand(command: FileCommand) {
  return ACTIONS[command];
}

export function isCommandEligible(file: TelegramFile, command: FileCommand) {
  if (file.originalDeleted) {
    return command === "remove";
  }
  return COMMANDS_BY_STATUS[file.downloadStatus].includes(command);
}

import { request } from "@/lib/api";
import type { TelegramFile } from "@/lib/types";

export type FileSource = "telegram" | "douyin";
export type FileCommand = "start" | "pause" | "resume" | "cancel" | "remove";

export type BatchActionResult = {
  processed: number;
  failed: number;
};

export type FileSourceAdapter = {
  normalize: (file: TelegramFile) => TelegramFile;
  identity: (file: TelegramFile) => string;
  tagIdentity: (file: TelegramFile) => string;
  command: (file: TelegramFile, command: FileCommand) => Promise<void>;
  batchCommand: (
    files: TelegramFile[],
    command: FileCommand,
  ) => Promise<BatchActionResult>;
  tags: (file: TelegramFile, tags: string) => Promise<void>;
  batchTags: (files: TelegramFile[], tags: string) => Promise<void>;
};

export function createFileSourceAdapter(source: FileSource): FileSourceAdapter {
  return source === "douyin" ? douyinAdapter : telegramAdapter;
}

const telegramAdapter: FileSourceAdapter = {
  normalize: (file) => ({ ...file, source: "telegram" }),
  identity(file) {
    return file.uniqueId.trim()
      ? `telegram:${file.telegramId}:${file.uniqueId}`
      : `telegram:${file.telegramId}:${file.chatId}:${file.messageId}`;
  },
  tagIdentity: requireUniqueId,
  async command(file, command) {
    await post(
      `/${file.telegramId}/file/${singleAction(command)}`,
      telegramCommandPayload(file, command),
    );
  },
  async batchCommand(files, command) {
    return postBatch(
      `/files/${batchAction(command)}`,
      files.map((file) => telegramCommandPayload(file, command)),
      command,
    );
  },
  async tags(file, tags) {
    await post(`/file/${requireUniqueId(file)}/update-tags`, { tags });
  },
  async batchTags(files, tags) {
    await post("/files/update-tags", {
      files: files.map((file) => ({ uniqueId: requireUniqueId(file) })),
      tags,
    });
  },
};

const douyinAdapter: FileSourceAdapter = {
  normalize: (file) => ({ ...file, source: "douyin" }),
  identity(file) {
    return file.uniqueId.trim()
      ? `douyin:${file.uniqueId}`
      : `douyin:${file.sourceId ?? ""}:${file.awemeId ?? ""}:${file.fileName}:${file.date}`;
  },
  tagIdentity: requireUniqueId,
  async command(file, command) {
    await post(
      `/douyin/file/${singleAction(command)}`,
      douyinCommandPayload(file, command),
    );
  },
  async batchCommand(files, command) {
    return postBatch(
      `/douyin/files/${batchAction(command)}`,
      files.map((file) => douyinCommandPayload(file, command)),
      command,
    );
  },
  async tags(file, tags) {
    await post(`/douyin/file/${requireUniqueId(file)}/update-tags`, { tags });
  },
  async batchTags(files, tags) {
    await post("/douyin/files/update-tags", {
      files: files.map((file) => ({ uniqueId: requireUniqueId(file) })),
      tags,
    });
  },
};

function telegramCommandPayload(file: TelegramFile, command: FileCommand) {
  const telegramId = requirePositive(file.telegramId, "telegramId");
  if (command === "start") {
    return {
      telegramId,
      fileId: requirePositive(file.id, "fileId"),
      uniqueId: file.uniqueId,
      chatId: requireNonZero(file.chatId, "chatId"),
      messageId: requireNonZero(file.messageId, "messageId"),
    };
  }
  if (command === "remove") {
    const fileId = file.id > 0 ? file.id : undefined;
    const uniqueId = file.uniqueId.trim() || undefined;
    if (!fileId && !uniqueId) {
      throw new Error("File removal requires a file ID or unique ID.");
    }
    return {
      telegramId,
      ...(fileId ? { fileId } : {}),
      ...(uniqueId ? { uniqueId } : {}),
    };
  }
  return {
    telegramId,
    fileId: requirePositive(file.id, "fileId"),
    uniqueId: file.uniqueId,
    ...pausePayload(command),
  };
}

function douyinCommandPayload(file: TelegramFile, command: FileCommand) {
  const fileId = file.id > 0 ? file.id : undefined;
  const uniqueId = file.uniqueId.trim() || undefined;
  if (!fileId && !uniqueId) {
    throw new Error("Douyin files require a unique ID or file ID.");
  }
  return {
    ...(fileId ? { fileId } : {}),
    ...(uniqueId ? { uniqueId } : {}),
    ...pausePayload(command),
  };
}

async function postBatch(path: string, files: object[], command: FileCommand) {
  const response = await post<Partial<BatchActionResult>>(path, {
    files,
    ...pausePayload(command),
  });
  return {
    processed: Math.max(0, Number(response?.processed ?? files.length)),
    failed: Math.max(0, Number(response?.failed ?? 0)),
  };
}

async function post<T = void>(path: string, payload: object) {
  return request<T>(path, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

function requireUniqueId(file: TelegramFile) {
  const uniqueId = file.uniqueId.trim();
  if (!uniqueId) throw new Error("Tag updates require a file unique ID.");
  return uniqueId;
}

function requirePositive(value: number, name: string) {
  if (value <= 0) throw new Error(`${name} must be positive.`);
  return value;
}

function requireNonZero(value: number, name: string) {
  if (value === 0) throw new Error(`${name} is required.`);
  return value;
}

function pausePayload(command: FileCommand) {
  if (command === "pause") return { isPaused: true };
  if (command === "resume") return { isPaused: false };
  return {};
}

function singleAction(command: FileCommand) {
  if (command === "start") return "start-download";
  if (command === "cancel") return "cancel-download";
  if (command === "remove") return "remove";
  return "toggle-pause-download";
}

function batchAction(command: FileCommand) {
  if (command === "start") return "start-download-multiple";
  if (command === "cancel") return "cancel-download-multiple";
  if (command === "remove") return "remove-multiple";
  return "toggle-pause-download-multiple";
}

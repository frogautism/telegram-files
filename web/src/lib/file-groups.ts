import type { TelegramFile } from "@/lib/types";

export type FileGroup = {
  key: string;
  files: TelegramFile[];
};

export function getFileGroupKey(file: TelegramFile): string {
  const isGroupedMedia =
    (file.type === "photo" || file.type === "video") && file.mediaAlbumId !== 0;
  if (isGroupedMedia) {
    return `album:${file.chatId}:${file.mediaAlbumId}`;
  }
  return `message:${file.chatId}:${file.messageId}`;
}

export function groupFilesByMessage(files: TelegramFile[]): FileGroup[] {
  const groups: FileGroup[] = [];
  for (const file of files) {
    const key = getFileGroupKey(file);
    const previous = groups[groups.length - 1];
    if (previous && previous.key === key) {
      previous.files.push(file);
      continue;
    }
    groups.push({ key, files: [file] });
  }
  return groups;
}

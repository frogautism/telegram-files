export function isGroupChatId(chatId?: string | null): boolean {
  return Boolean(chatId && chatId.startsWith("group:"));
}

export function getGroupId(chatId?: string | null): string | undefined {
  if (!isGroupChatId(chatId)) {
    return undefined;
  }
  const value = chatId!.slice("group:".length).trim();
  return value || undefined;
}

export function getFilesApiPath(accountId: string, chatId: string): string {
  const groupId = getGroupId(chatId);
  if (groupId) {
    return `/telegram/${accountId}/chat-group/${groupId}/files`;
  }
  return `/telegram/${accountId}/chat/${chatId}/files`;
}

export function getFilesCountApiPath(
  accountId: string,
  chatId: string,
): string {
  const groupId = getGroupId(chatId);
  if (groupId) {
    return `/telegram/${accountId}/chat-group/${groupId}/files/count`;
  }
  return `/telegram/${accountId}/chat/${chatId}/files/count`;
}

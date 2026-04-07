"use client";
import { useRouter, useSearchParams } from "next/navigation";
import { useToast } from "@/hooks/use-toast";
import { createContext, useContext, useMemo, useState } from "react";
import { type TelegramChat } from "@/lib/types";
import useSWR from "swr";
import { useDebouncedCallback } from "use-debounce";
import { getGroupId, isGroupChatId } from "@/lib/chat-target";

interface TelegramChatContextType {
  isLoading: boolean;
  reload: () => Promise<unknown>;
  chatId: string | undefined;
  chat?: TelegramChat;
  chats: TelegramChat[];
  query: string;
  archived: boolean;
  handleChatChange: (chatId: string) => void;
  handleQueryChange: (search: string) => void;
  handleArchivedChange: (archived: boolean) => void;
}

const TelegramChatContext = createContext<TelegramChatContextType | undefined>(
  undefined,
);

interface TelegramChatProviderProps {
  children: React.ReactNode;
}

export const TelegramChatProvider: React.FC<TelegramChatProviderProps> = ({
  children,
}) => {
  const [query, setQuery] = useState("");
  const [archived, setArchived] = useState(false);
  const router = useRouter();
  const { toast } = useToast();
  const searchParams = useSearchParams();
  const accountId = searchParams.get("id") ?? "";
  const chatId = searchParams.get("chatId") ?? "";
  const selectedGroupId = getGroupId(chatId);

  const handleQueryChange = useDebouncedCallback((search: string) => {
    setQuery(search);
  }, 500);

  const {
    data: directChats,
    isLoading: directChatsLoading,
    mutate: mutateDirectChats,
  } = useSWR<TelegramChat[]>(
    accountId
      ? `/telegram/${accountId}/chats?query=${query}&archived=${archived}&chatId=${isGroupChatId(chatId) ? "" : (chatId ?? "")}`
      : null,
  );
  const {
    data: groupChats,
    isLoading: groupChatsLoading,
    mutate: mutateGroupChats,
  } = useSWR<TelegramChat[]>(
    accountId
      ? `/telegram/${accountId}/chat-groups?query=${query}&chatId=${selectedGroupId ? `group:${selectedGroupId}` : ""}`
      : null,
  );
  const chats = useMemo(
    () => [...(groupChats ?? []), ...(directChats ?? [])],
    [directChats, groupChats],
  );

  const chat = useMemo(
    () => chats?.find((c) => c.id === chatId),
    [chatId, chats],
  );

  const handleChatChange = (newChatId: string) => {
    if (newChatId === chatId) {
      return;
    }
    const chat = chats?.find((c) => c.id === newChatId);
    if (!chat) {
      toast({ variant: "error", description: "Failed to switch chat" });
      return;
    }
    router.push(`/accounts?id=${accountId}&chatId=${newChatId}`);
  };

  return (
    <TelegramChatContext.Provider
      value={{
        isLoading: directChatsLoading || groupChatsLoading,
        reload: () => Promise.all([mutateDirectChats(), mutateGroupChats()]),
        chatId,
        chat,
        chats,
        query,
        archived,
        handleChatChange,
        handleQueryChange: handleQueryChange,
        handleArchivedChange: setArchived,
      }}
    >
      {children}
    </TelegramChatContext.Provider>
  );
};

export function useTelegramChat() {
  const context = useContext(TelegramChatContext);
  if (!context) {
    throw new Error(
      "useTelegramChat must be used within a TelegramChatProvider",
    );
  }
  return context;
}

export function useMaybeTelegramChat() {
  const context = useContext(TelegramChatContext);
  if (!context) {
    return undefined;
  }
  return context;
}

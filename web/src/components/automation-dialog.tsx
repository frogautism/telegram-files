import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import React, { useState } from "react";
import useSWR from "swr";
import useSWRMutation from "swr/mutation";
import { POST, request } from "@/lib/api";
import { useDebounce } from "use-debounce";
import { useToast } from "@/hooks/use-toast";
import { AutomationButton } from "@/components/automation-button";
import { useTelegramChat } from "@/hooks/use-telegram-chat";
import { useTelegramAccount } from "@/hooks/use-telegram-account";
import { Label } from "@/components/ui/label";
import {
  type Auto,
  type AutoTransferPreset,
  type AutoTransferRule,
  type TelegramChat,
} from "@/lib/types";
import { Badge } from "./ui/badge";
import { cn } from "@/lib/utils";
import AutomationForm from "@/components/automation-form";
import { isGroupChatId } from "@/lib/chat-target";

const DEFAULT_AUTO: Auto = {
  preload: {
    enabled: false,
  },
  download: {
    enabled: false,
    rule: {
      query: "",
      fileTypes: [],
      downloadHistory: true,
      downloadCommentFiles: false,
      filterExpr: "",
    },
  },
  transfer: {
    enabled: false,
    rule: {
      transferHistory: true,
      destination: "",
      transferPolicy: "GROUP_BY_CHAT",
      duplicationPolicy: "OVERWRITE",
      extra: {},
    },
  },
};

export default function AutomationDialog() {
  const { accountId } = useTelegramAccount();
  const { isLoading, chat, reload } = useTelegramChat();

  if (isLoading) {
    return (
      <div className="h-10 w-36 animate-pulse rounded-md bg-muted"></div>
    );
  }

  if (!accountId || !chat) {
    return null;
  }

  return (
    <AutomationDialogContent
      key={`${accountId}:${chat.id}:${chat.groupId ?? ""}`}
      accountId={accountId}
      chat={chat}
      reload={reload}
    />
  );
}

function AutomationDialogContent({
  accountId,
  chat,
  reload,
}: {
  accountId: string;
  chat: TelegramChat;
  reload: () => Promise<unknown>;
}) {
  const { toast } = useToast();
  const [open, setOpen] = useState(false);
  const [editMode, setEditMode] = useState(false);
  const [auto, setAuto] = useState<Auto>(() => autoFromChat(chat.auto));
  const [presetName, setPresetName] = useState("");
  const [appliedPresetId, setAppliedPresetId] = useState("");
  const [isPresetSaving, setIsPresetSaving] = useState(false);
  const [deletingPresetId, setDeletingPresetId] = useState("");
  const isGroupChat = Boolean(chat?.id && isGroupChatId(chat.id));
  const {
    data: transferPresets = [],
    mutate: mutateTransferPresets,
  } = useSWR<AutoTransferPreset[]>(
    accountId.startsWith("pending-")
      ? undefined
      : `/${accountId}/auto-transfer-presets`,
    (key) => request<AutoTransferPreset[]>(key),
    {
      revalidateOnFocus: false,
    },
  );
  const { trigger: triggerAuto, isMutating: isAutoMutating } = useSWRMutation(
    isGroupChat && chat.groupId
      ? `/${accountId}/chat-group/${chat.groupId}/update-auto-settings`
      : `/${accountId}/file/update-auto-settings?telegramId=${accountId}&chatId=${chat.id}`,
    (
      key: string,
      {
        arg,
      }: {
        arg: Auto;
      },
    ) => {
      return POST(key, arg);
    },
    {
      onSuccess: () => {
        toast({
          variant: "success",
          title: "Auto settings updated!",
        });
        void reload();
        setEditMode(false);
        setTimeout(() => {
          setOpen(false);
        }, 1000);
      },
    },
  );

  const [debounceIsAutoMutating] = useDebounce(isAutoMutating, 500, {
    leading: true,
  });

  const handleApplyPreset = (preset: AutoTransferPreset) => {
    setAuto({
      ...auto,
      transfer: {
        enabled: true,
        rule: cloneTransferRule(preset.rule),
      },
    });
    setPresetName(preset.name);
    setAppliedPresetId(preset.id);
    toast({
      variant: "success",
      title: "Transfer preset applied",
    });
  };

  const handleSavePreset = async () => {
    const name = presetName.trim();
    if (!name) {
      toast({
        variant: "warning",
        title: "Preset name is required",
      });
      return;
    }

    const validationError = transferRuleValidationError(auto.transfer.rule);
    if (validationError) {
      toast({
        variant: "warning",
        title: validationError,
      });
      return;
    }

    setIsPresetSaving(true);
    try {
      await POST(`/${accountId}/auto-transfer-presets`, {
        id: appliedPresetId || undefined,
        name,
        rule: auto.transfer.rule,
      });
      await mutateTransferPresets();
      setPresetName("");
      setAppliedPresetId("");
      toast({
        variant: "success",
        title: "Transfer preset saved",
      });
    } finally {
      setIsPresetSaving(false);
    }
  };

  const handleDeletePreset = async (preset: AutoTransferPreset) => {
    setDeletingPresetId(preset.id);
    try {
      await POST(`/${accountId}/auto-transfer-presets/${preset.id}/delete`);
      await mutateTransferPresets();
      if (appliedPresetId === preset.id) {
        setAppliedPresetId("");
        setPresetName("");
      }
      toast({
        variant: "success",
        title: "Transfer preset deleted",
      });
    } finally {
      setDeletingPresetId("");
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        asChild
        onClick={(e) => {
          e.stopPropagation();
          setOpen(!open);
        }}
      >
        <AutomationButton auto={chat.auto} />
      </DialogTrigger>
      <DialogContent
        aria-describedby={undefined}
        onPointerDownOutside={() => setOpen(false)}
        onClick={(e) => e.stopPropagation()}
        className="h-full w-full overflow-auto md:h-auto md:max-h-[85%] md:min-w-[560px]"
      >
        <DialogHeader>
          <DialogTitle className="text-2xl">
            Automation for {chat?.name ?? "Unknown Chat"}
          </DialogTitle>
          <DialogDescription>
            Configure preload, download, and transfer rules for this
            {isGroupChat ? " group chat." : " chat."}
          </DialogDescription>
        </DialogHeader>
        {!editMode && chat?.auto ? (
          <div className="space-y-4">
            <div className="space-y-4 rounded-md border border-border bg-card p-5">
              <div className="flex items-center justify-between">
                <Label className="text-sm font-semibold text-foreground">
                  Auto Preload
                </Label>
                <Badge
                  variant="outline"
                  className={cn(
                    "border-none px-2 py-1 text-xs",
                    chat.auto.preload.enabled
                      ? "bg-[#dce7dd] text-[#103c25]"
                      : "bg-muted text-muted-foreground",
                  )}
                >
                  {chat.auto.preload.enabled ? "Enabled" : "Disabled"}
                </Badge>
              </div>
              {(chat.auto.state & (1 << 1)) != 0 && (
                <p className="text-xs text-muted-foreground">
                  All historical files are preloaded.
                </p>
              )}
            </div>
            <div className="space-y-4 rounded-md border border-border bg-card p-5">
              <div className="flex items-center justify-between">
                <Label className="text-sm font-semibold text-foreground">
                  Auto Download
                </Label>
                <Badge
                  variant="outline"
                  className={cn(
                    "border-none px-2 py-1 text-xs",
                    chat.auto.download.enabled
                      ? "bg-[#dce7dd] text-[#103c25]"
                      : "bg-muted text-muted-foreground",
                  )}
                >
                  {chat.auto.download.enabled ? "Enabled" : "Disabled"}
                </Badge>
              </div>
              {auto.download.enabled && (
                <>
                  {(chat.auto.state & (1 << 2)) != 0 && (
                    <p className="text-xs text-muted-foreground">
                      All historical files are started to be downloaded.
                    </p>
                  )}
                  <div className="space-y-3">
                    {/* Query Keyword Section */}
                    <div className="rounded-md bg-muted p-3">
                      <div className="flex flex-col space-y-1">
                        <span className="text-xs font-medium text-muted-foreground">
                          Query Keyword
                        </span>
                        <span className="text-sm text-muted-foreground">
                          {chat.auto.download.rule.query ||
                            "No keyword specified"}
                        </span>
                      </div>
                    </div>
                    <div className="rounded-md bg-muted p-3">
                      <div className="flex flex-col space-y-1">
                        <span className="text-xs font-medium text-muted-foreground">
                          Filter Expression
                        </span>
                        <span className="text-sm text-muted-foreground">
                          {chat.auto.download.rule.filterExpr ||
                            "No filter expression specified"}
                        </span>
                      </div>
                    </div>

                    <div className="rounded-md bg-muted p-3">
                      <span className="text-xs font-medium text-muted-foreground">
                        File Types
                      </span>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {chat.auto.download.rule.fileTypes.length > 0 ? (
                          chat.auto.download.rule.fileTypes.map((type) => (
                            <Badge
                              key={type}
                              variant="secondary"
                              className="flex items-center gap-1 bg-card px-3 py-1 capitalize text-foreground"
                            >
                              {type}
                            </Badge>
                          ))
                        ) : (
                          <span className="text-sm text-muted-foreground">
                            No file types selected
                          </span>
                        )}
                      </div>
                    </div>

                    <div className="flex items-center justify-between rounded-md bg-muted p-3">
                      <span className="text-xs font-medium text-muted-foreground">
                        Download History
                      </span>
                      <Badge
                        className={cn(
                          "border-none px-2 py-1 text-xs",
                          !chat.auto.download.rule.downloadHistory &&
                            "bg-muted text-muted-foreground",
                        )}
                      >
                        {chat.auto.download.rule.downloadHistory
                          ? "Enabled"
                          : "Disabled"}
                      </Badge>
                    </div>

                    <div className="flex items-center justify-between rounded-md bg-muted p-3">
                      <span className="text-xs font-medium text-muted-foreground">
                        Download Comment Files
                      </span>
                      <Badge
                        className={cn(
                          "border-none px-2 py-1 text-xs",
                          !chat.auto.download.rule.downloadCommentFiles &&
                            "bg-muted text-muted-foreground",
                        )}
                      >
                        {chat.auto.download.rule.downloadCommentFiles
                          ? "Enabled"
                          : "Disabled"}
                      </Badge>
                    </div>
                  </div>
                </>
              )}
            </div>

            <div className="space-y-4 rounded-md border border-border bg-card p-5">
              <div className="flex items-center justify-between">
                <Label className="text-sm font-semibold text-foreground">
                  Auto Transfer
                </Label>
                <Badge
                  variant="outline"
                  className={cn(
                    "border-none px-2 py-1 text-xs",
                    chat.auto.transfer.enabled
                      ? "bg-[#dce7dd] text-[#103c25]"
                      : "bg-muted text-muted-foreground",
                  )}
                >
                  {chat.auto.transfer.enabled ? "Enabled" : "Disabled"}
                </Badge>
              </div>
              {chat.auto.transfer.enabled && (
                <>
                  {(chat.auto.state & (1 << 4)) != 0 && (
                    <p className="text-xs text-muted-foreground">
                      All historical download files are transferred.
                    </p>
                  )}
                  <div className="space-y-3">
                    <div className="rounded-md bg-muted p-3">
                      <div className="flex flex-col space-y-1">
                        <span className="text-xs font-medium text-muted-foreground">
                          Destination Folder
                        </span>
                        <span className="text-sm text-muted-foreground">
                          {chat.auto.transfer.rule.destination}
                        </span>
                      </div>
                    </div>
                    <div className="flex flex-col space-y-3 rounded-md bg-muted p-3">
                      <div className="flex items-center justify-between">
                        <span className="text-xs text-muted-foreground">
                          Transfer Policy
                        </span>
                        <Badge variant="outline" className="font-normal">
                          {chat.auto.transfer.rule.transferPolicy}
                        </Badge>
                      </div>
                      {chat.auto.transfer.rule.transferPolicy ===
                        "GROUP_BY_AI" && (
                        <div className="mt-2 w-full whitespace-pre-line rounded-md bg-card p-2 text-xs text-muted-foreground">
                          {chat.auto.transfer.rule.extra.promptTemplate}
                        </div>
                      )}
                    </div>
                    <div className="flex items-center justify-between rounded-md bg-muted p-3">
                      <span className="text-xs text-muted-foreground">
                        Duplication Policy
                      </span>
                      <Badge variant="outline" className="font-normal">
                        {chat.auto.transfer.rule.duplicationPolicy}
                      </Badge>
                    </div>
                    <div className="flex items-center justify-between rounded-md bg-muted p-3">
                      <span className="text-xs text-muted-foreground">
                        Transfer History
                      </span>
                      <Badge
                        className={cn(
                          "border-none px-2 py-1 text-xs",
                          !chat.auto.transfer.rule.transferHistory &&
                            "bg-muted text-muted-foreground",
                        )}
                      >
                        {chat.auto.transfer.rule.transferHistory
                          ? "Enabled"
                          : "Disabled"}
                      </Badge>
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        ) : (
          <AutomationForm
            auto={auto}
            onChange={setAuto}
            transferPresets={transferPresets}
            presetName={presetName}
            onPresetNameChange={setPresetName}
            onApplyTransferPreset={handleApplyPreset}
            onSaveTransferPreset={handleSavePreset}
            onDeleteTransferPreset={handleDeletePreset}
            isPresetSaving={isPresetSaving}
            deletingPresetId={deletingPresetId}
          />
        )}
        <DialogFooter className="gap-2 border-t border-border pt-4">
          {!editMode && chat?.auto ? (
            <Button variant="outline" onClick={() => setEditMode(true)}>
              Edit
            </Button>
          ) : (
            <>
              <Button
                variant="outline"
                onClick={() => setOpen(false)}
                disabled={debounceIsAutoMutating}
              >
                Cancel
              </Button>
              <Button
                onClick={() => {
                  const validationError = transferRuleValidationError(
                    auto.transfer.rule,
                  );
                  if (auto.transfer.enabled && validationError) {
                    toast({
                      variant: "warning",
                      title: validationError,
                      description:
                        "Please enter a valid destination folder path",
                    });
                    return;
                  }
                  void triggerAuto(auto);
                }}
                disabled={debounceIsAutoMutating}
              >
                {debounceIsAutoMutating ? "Submitting..." : "Submit"}
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function cloneTransferRule(rule: AutoTransferRule): AutoTransferRule {
  return {
    transferHistory: Boolean(rule.transferHistory),
    destination: rule.destination ?? "",
    transferPolicy: rule.transferPolicy ?? "GROUP_BY_CHAT",
    duplicationPolicy: rule.duplicationPolicy ?? "OVERWRITE",
    extra: { ...(rule.extra ?? {}) },
  };
}

function autoFromChat(auto?: TelegramChat["auto"]): Auto {
  return {
    preload: {
      enabled: Boolean(auto?.preload.enabled ?? DEFAULT_AUTO.preload.enabled),
    },
    download: {
      enabled: Boolean(auto?.download.enabled ?? DEFAULT_AUTO.download.enabled),
      rule: {
        query: auto?.download.rule.query ?? DEFAULT_AUTO.download.rule.query,
        fileTypes: [...(auto?.download.rule.fileTypes ?? [])],
        downloadHistory: Boolean(
          auto?.download.rule.downloadHistory ??
            DEFAULT_AUTO.download.rule.downloadHistory,
        ),
        downloadCommentFiles: Boolean(
          auto?.download.rule.downloadCommentFiles ??
            DEFAULT_AUTO.download.rule.downloadCommentFiles,
        ),
        filterExpr:
          auto?.download.rule.filterExpr ?? DEFAULT_AUTO.download.rule.filterExpr,
      },
    },
    transfer: {
      enabled: Boolean(auto?.transfer.enabled ?? DEFAULT_AUTO.transfer.enabled),
      rule: cloneTransferRule(auto?.transfer.rule ?? DEFAULT_AUTO.transfer.rule),
    },
  };
}

function transferRuleValidationError(rule: AutoTransferRule): string | null {
  const folderPathRegex = /^[\/\\]?(?:[^<>:"|?*\/\\]+[\/\\]?)*$/;
  if (rule.destination.length === 0 || !folderPathRegex.test(rule.destination)) {
    return "Invalid destination folder";
  }
  return null;
}

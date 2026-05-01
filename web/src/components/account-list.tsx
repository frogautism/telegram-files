import { type TelegramAccount } from "@/lib/types";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { ArrowUpRight, PhoneCall } from "lucide-react";
import { Spoiler } from "spoiled";
import AccountDeleteDialog from "@/components/account-delete-dialog";
import { AccountDialog } from "@/components/account-dialog";
import { Button } from "@/components/ui/button";

interface AccountListProps {
  accounts: TelegramAccount[];
  onSelectAccount: (accountId: string) => void;
}

export function AccountList({ accounts, onSelectAccount }: AccountListProps) {
  return (
    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
      {accounts.map((account) => {
        const isActive = account.status === "active";
        return (
          <div
            key={account.id}
            className="group relative cursor-pointer overflow-hidden rounded-lg border border-border bg-card shadow-card transition-all hover:-translate-y-0.5 hover:border-border-strong hover:shadow-pop"
            onClick={() => onSelectAccount(account.id)}
          >
            <AccountDeleteDialog
              telegramId={account.id}
              className="absolute right-3 top-3 z-10 hidden group-hover:inline-flex"
            />
            <div className="p-5">
              <div className="mb-4 flex items-start gap-3">
                <Avatar className="h-11 w-11 rounded-md ring-1 ring-border">
                  <AvatarImage src={`data:image/jpeg;base64,${account.avatar}`} />
                  <AvatarFallback className="rounded-md bg-muted font-display text-base">
                    {account.name[0]}
                  </AvatarFallback>
                </Avatar>
                <div className="min-w-0 flex-1">
                  <h3 className="truncate text-base font-medium leading-tight">
                    {account.name}
                  </h3>
                  <p className="mt-1 truncate text-xs text-muted-foreground">
                    {isActive ? (
                      <span className="inline-flex items-center gap-1.5">
                        <PhoneCall className="h-3 w-3" strokeWidth={2} />
                        <Spoiler>{account.phoneNumber}</Spoiler>
                      </span>
                    ) : (
                      "Authorization required"
                    )}
                  </p>
                </div>
                <span
                  className={
                    "inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[10px] font-medium uppercase tracking-[0.12em] " +
                    (isActive
                      ? "bg-success-soft text-success-soft-foreground"
                      : "bg-warning-soft text-warning-soft-foreground")
                  }
                >
                  <span
                    className={
                      "h-1.5 w-1.5 rounded-full " +
                      (isActive ? "bg-success" : "bg-warning")
                    }
                  />
                  {account.status}
                </span>
              </div>

              <p className="mb-4 truncate font-mono text-[11px] text-muted-foreground">
                {account.rootPath}
              </p>

              <div className="flex items-center gap-2">
                <Button size="sm" className="gap-1.5">
                  Open vault
                  <ArrowUpRight className="h-3.5 w-3.5" />
                </Button>
                {!isActive && (
                  <AccountDialog>
                    <Button variant="outline" size="sm">
                      Activate
                    </Button>
                  </AccountDialog>
                )}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

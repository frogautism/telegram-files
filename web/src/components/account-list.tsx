import { type TelegramAccount } from "@/lib/types";
import { Card, CardContent } from "@/components/ui/card";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Circle, PhoneCall } from "lucide-react";
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
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
      {accounts.map((account) => (
        <Card
          key={account.id}
          className="group relative cursor-pointer overflow-hidden transition-colors hover:bg-muted"
          onClick={(_e) => {
            onSelectAccount(account.id);
          }}
        >
          <AccountDeleteDialog
            telegramId={account.id}
            className="absolute right-4 top-4 z-10 hidden group-hover:inline-flex"
          />
          <CardContent className="p-6">
            <div className="mb-5 flex h-36 items-start justify-between rounded-[24px] bg-muted p-5">
              <Avatar className="h-16 w-16 border-4 border-card">
                <AvatarImage src={`data:image/jpeg;base64,${account.avatar}`} />
                <AvatarFallback>{account.name[0]}</AvatarFallback>
              </Avatar>
              <Badge
                variant={account.status === "active" ? "default" : "secondary"}
                className="gap-2"
              >
                <Circle
                  className={`h-2.5 w-2.5 ${account.status === "active" ? "text-[#103c25]" : "text-muted-foreground"}`}
                />
                {account.status}
              </Badge>
            </div>

            <div className="space-y-3">
              <div className="space-y-1">
                <h3 className="text-xl font-semibold">{account.name}</h3>
                <p className="text-sm text-muted-foreground">
                  {account.status === "active" ? (
                    <span className="inline-flex items-center gap-1">
                      <PhoneCall className="h-3.5 w-3.5" />
                      <Spoiler>{account.phoneNumber}</Spoiler>
                    </span>
                  ) : (
                    "Authorization required"
                  )}
                </p>
              </div>

              <p className="line-clamp-2 text-sm text-muted-foreground">
                {account.rootPath}
              </p>

              <div className="flex items-center justify-between gap-3 pt-2">
                <Button variant="secondary" size="sm">
                  Open board
                </Button>
                {account.status === "inactive" && (
                  <AccountDialog>
                    <Button variant="outline" size="sm">
                      Activate
                    </Button>
                  </AccountDialog>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

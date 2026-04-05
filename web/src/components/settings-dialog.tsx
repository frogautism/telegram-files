import React, { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Settings } from "lucide-react";
import { VisuallyHidden } from "@radix-ui/react-visually-hidden";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import FileStatistics from "@/components/file-statistics";
import { useTelegramAccount } from "@/hooks/use-telegram-account";
import Proxys from "@/components/proxys";
import SettingsForm from "@/components/settings-form";
import About from "@/components/about";
import { ChartColumnIncreasingIcon } from "@/components/ui/chart-column-increasing";
import { LayoutPanelTopIcon } from "@/components/ui/layout-panel-top";
import FilePhaseStatistics from "@/components/file-phase-statistics";
import DebugTelegramMethod from "@/components/debug-telegram-method";

export const SettingsDialog: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);
  const { account, accountId } = useTelegramAccount();

  return (
    <Dialog open={isOpen} onOpenChange={setIsOpen}>
      <DialogTrigger
        asChild
        onClick={() => {
          setIsOpen(!isOpen);
        }}
      >
        <Button variant="ghost" size="icon" className="rounded-full border border-input bg-card">
          <Settings className="h-4 w-4" />
        </Button>
      </DialogTrigger>
      <DialogContent
        className="h-full w-full max-w-full overflow-hidden md:h-[86vh] md:w-[min(1120px,92vw)] md:max-w-[1120px]"
        onPointerDownOutside={() => setIsOpen(false)}
        aria-describedby={undefined}
      >
        <VisuallyHidden>
          <DialogTitle>Settings</DialogTitle>
        </VisuallyHidden>
        <div className="space-y-4 overflow-hidden">
          <div className="space-y-2 pr-10">
            <p className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">
              Workspace settings
            </p>
            <div className="flex flex-wrap items-center justify-between gap-3">
              <h2 className="text-3xl font-semibold">Configure TeleFiles</h2>
              {account?.name && (
                <div className="rounded-full bg-muted px-4 py-2 text-sm text-muted-foreground">
                  {account.name}
                </div>
              )}
            </div>
          </div>
          <Tabs
            defaultValue="general"
            className="flex h-[calc(100vh-11rem)] flex-col overflow-hidden md:h-[calc(86vh-7rem)]"
          >
            <TabsList className="justify-start overflow-auto bg-muted">
              <TabsTrigger value="general">General</TabsTrigger>
              <TabsTrigger value="statistics">Statistics</TabsTrigger>
              <TabsTrigger value="proxys">Proxys</TabsTrigger>
              <TabsTrigger value="api">API</TabsTrigger>
              <TabsTrigger value="about">About</TabsTrigger>
            </TabsList>
            <TabsContent value="general" className="mt-4 overflow-hidden">
              <SettingsForm />
            </TabsContent>
            <TabsContent value="statistics" className="mt-4 h-full overflow-hidden">
              <div className="no-scrollbar flex h-full flex-col overflow-y-scroll">
                {accountId ? (
                  <Tabs defaultValue="panel">
                    <TabsList className="mb-4 w-fit bg-muted">
                      <TabsTrigger
                        value="panel"
                        className="h-10 w-10 data-[state=active]:bg-card"
                      >
                        <LayoutPanelTopIcon />
                      </TabsTrigger>
                      <TabsTrigger
                        value="phase"
                        className="h-10 w-10 data-[state=active]:bg-card"
                      >
                        <ChartColumnIncreasingIcon />
                      </TabsTrigger>
                    </TabsList>
                    <TabsContent value="panel">
                      <FileStatistics telegramId={accountId} />
                    </TabsContent>
                    <TabsContent value="phase">
                      <FilePhaseStatistics telegramId={accountId} />
                    </TabsContent>
                  </Tabs>
                ) : (
                  <div className="flex flex-1 items-center justify-center">
                    <p className="text-lg text-muted-foreground">
                      Please select an account to view statistics
                    </p>
                  </div>
                )}
              </div>
            </TabsContent>
            <TabsContent value="proxys" className="mt-4 h-full overflow-hidden">
              <div className="no-scrollbar flex h-full flex-col overflow-y-scroll">
                <Proxys
                  telegramId={accountId}
                  proxyName={account?.proxy}
                  enableSelect={true}
                />
              </div>
            </TabsContent>
            <TabsContent value="api" className="mt-4 h-full overflow-hidden">
              <DebugTelegramMethod />
            </TabsContent>
            <TabsContent value="about" className="mt-4 h-full overflow-hidden">
              <About />
            </TabsContent>
          </Tabs>
        </div>
      </DialogContent>
    </Dialog>
  );
};

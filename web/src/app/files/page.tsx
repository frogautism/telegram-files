"use client";
import Files from "@/components/files";
import ThemeToggleButton from "@/components/theme-toggle-button";
import Link from "next/link";
import TelegramIcon from "@/components/telegram-icon";
import { Card, CardContent } from "@/components/ui/card";

export default function AccountPage() {
  return (
    <div className="app-shell px-4 py-4 md:px-6 md:py-6">
      <Card className="mb-6 bg-card/95 backdrop-blur">
        <CardContent className="p-4 md:p-5">
          <div className="flex items-center justify-between gap-4">
            <Link
              href="/"
              className="inline-flex items-center gap-3 rounded-[20px] bg-muted px-3 py-2"
            >
              <div className="flex h-11 w-11 items-center justify-center rounded-full bg-primary text-primary-foreground">
                <TelegramIcon className="h-5 w-5" />
              </div>
              <div>
                <p className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">
                  Offline board
                </p>
                <h1 className="text-xl font-semibold">Downloaded library</h1>
              </div>
            </Link>

            <ThemeToggleButton />
          </div>
        </CardContent>
      </Card>
      <Files accountId="-1" chatId="-1" />
    </div>
  );
}

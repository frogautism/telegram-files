import { env } from "@/env";

export function getApiUrl(): string {
  const url = env.NEXT_PUBLIC_API_URL;
  if (url.startsWith("http")) {
    return url;
  }
  if (typeof window === "undefined") {
    return url;
  }
  return `${window.location.protocol}//${window.location.host}${url}`;
}

export function getWsUrl(): string {
  const url = env.NEXT_PUBLIC_WS_URL;
  if (url.startsWith("ws")) {
    return url;
  }
  if (typeof window === "undefined") {
    return url;
  }
  return `${window.location.protocol === "https:" ? "wss" : "ws"}://${
    window.location.host
  }${url}`;
}

export async function request<T = any>(
  api: string,
  requestInit?: RequestInit,
): Promise<T> {
  const headers = new Headers(requestInit?.headers);
  const body = requestInit?.body;
  const shouldSetJsonContentType =
    body !== undefined &&
    !(body instanceof FormData) &&
    !(body instanceof URLSearchParams) &&
    !(body instanceof Blob);
  if (!headers.has("Content-Type") && shouldSetJsonContentType) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(`${getApiUrl()}${api}`, {
    ...requestInit,
    credentials: requestInit?.credentials ?? "include",
    headers,
  });
  const responseText = await response.text();
  const method = requestInit?.method ?? "GET";

  if (!responseText) {
    if (!response.ok) {
      throw new Error(
        `Request failed with status ${response.status} (${method} ${api})`,
      );
    }
    return undefined as T;
  }

  let data: unknown;
  try {
    data = JSON.parse(responseText) as unknown;
  } catch {
    if (!response.ok) {
      throw new Error(`${responseText} (${method} ${api})`);
    }
    throw new RequestParsedError(responseText);
  }
  if (!response.ok) {
    const responseData = data as Record<string, unknown>;
    const errorMessage =
      responseData.error ??
      responseData.detail ??
      responseData.message ??
      `Request failed with status ${response.status}`;
    throw new Error(`${String(errorMessage)} (${method} ${api})`);
  }

  return data as T;
}

export class RequestParsedError extends Error {
  responseText: string;

  constructor(responseText: string) {
    super("Parse JSON Error");
    this.responseText = responseText;
  }
}

export function localStorageProvider() {
  let entries: Iterable<readonly [string, unknown]> = [];
  try {
    entries = JSON.parse(
      localStorage.getItem("telegram-files") ?? "[]",
    ) as Iterable<readonly [string, unknown]>;
  } catch {
    entries = [];
  }
  const map = new Map<string, unknown>(entries);

  window.addEventListener("beforeunload", () => {
    const appCache = JSON.stringify(Array.from(map.entries()));
    localStorage.setItem("telegram-files", appCache);
  });

  return map;
}

export async function POST(api: string, data?: any): Promise<any> {
  return await request(api, {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export async function PATCH(api: string, data?: any): Promise<any> {
  return await request(api, {
    method: "PATCH",
    body: JSON.stringify(data),
  });
}

export async function DELETE(api: string, data?: any): Promise<any> {
  return await request(api, {
    method: "DELETE",
    body: data === undefined ? undefined : JSON.stringify(data),
  });
}

export type TelegramApiArg = {
  data: any;
  method: string;
};

export async function telegramApi(
  api: string,
  {
    arg,
  }: {
    arg: TelegramApiArg;
  },
): Promise<any> {
  return await request(`${api}/${arg.method}`, {
    method: "POST",
    body: arg.data ? JSON.stringify(arg.data) : undefined,
  });
}

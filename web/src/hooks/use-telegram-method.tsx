import { useCallback, useEffect, useRef, useState } from "react";
import useSWRMutation from "swr/mutation";
import type { TelegramApiResult } from "@/lib/types";
import { telegramApi, type TelegramApiArg } from "@/lib/api";
import { useWebsocket } from "@/hooks/use-websocket";

export function useTelegramMethod() {
  const pendingRequestsRef = useRef<
    Map<
      string,
      {
        resolve: (value: any) => void;
        reject: (reason?: any) => void;
        timeoutId: ReturnType<typeof setTimeout>;
      }
    >
  >(new Map());

  const lastResultRef = useRef<{
    code: string | null;
    result: unknown;
  }>({ code: null, result: null });

  const [pendingCount, setPendingCount] = useState(0); // 用 state 追踪 ref 的 size

  const { lastJsonMessage } = useWebsocket();

  const [lastMethod, setLastMethod] = useState<{
    code: string | null;
    result: unknown;
  }>({
    code: null,
    result: null,
  });

  useEffect(() => {
    if (!lastJsonMessage?.code) return;

    const code = lastJsonMessage.code;
    const data = lastJsonMessage.data;

    lastResultRef.current = { code, result: data };
    setLastMethod({ code, result: data }); // 同步 state

    const pendingRequest = pendingRequestsRef.current.get(code);
    if (pendingRequest) {
      clearTimeout(pendingRequest.timeoutId);
      pendingRequest.resolve(data);
      pendingRequestsRef.current.delete(code);
      setPendingCount(pendingRequestsRef.current.size);
    }
  }, [lastJsonMessage]);

  useEffect(() => {
    const pendingRequests = pendingRequestsRef.current;
    return () => {
      pendingRequests.forEach((request, code) => {
        clearTimeout(request.timeoutId);
        request.reject(new Error(`Request cancelled for code: ${code}`));
      });
      pendingRequests.clear();
    };
  }, []);

  const { trigger, isMutating } = useSWRMutation<
    TelegramApiResult,
    Error,
    string,
    TelegramApiArg
  >("/telegram/api", telegramApi);

  const executeMethod = useCallback(
    async (arg: TelegramApiArg): Promise<any> => {
      const result = await trigger(arg);
      const { code } = result;

      if (lastResultRef.current.code === code) {
        return lastResultRef.current.result;
      }

      return new Promise((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          pendingRequestsRef.current.delete(code);
          setPendingCount(pendingRequestsRef.current.size);
          reject(new Error(`Request timeout for code: ${code}`));
        }, 30000);

        pendingRequestsRef.current.set(code, {
          timeoutId,
          resolve,
          reject: (reason) => {
            reject(
              reason instanceof Error ? reason : new Error(String(reason)),
            );
          },
        });

        setPendingCount(pendingRequestsRef.current.size);
      });
    },
    [trigger],
  );

  const isMethodExecuting = isMutating || pendingCount > 0;

  return {
    executeMethod,
    triggerMethod: trigger,
    isMethodExecuting,
    lastMethodCode: lastMethod.code,
    lastMethodResult: lastMethod.result,
    pendingRequestsCount: pendingCount,
  };
}

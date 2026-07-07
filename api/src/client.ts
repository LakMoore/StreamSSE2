/**
 * 
 * HTTP2 Server-Sent Events (SSE) streaming utility.
 *
 * Originally based on @glamboyosa/ore's (https://github.com/glamboyosa/ore)
 * streamSSE implementation, but rewritten to use native Node.js HTTP2
 * and async generators internally as well as exposing them.
 *
 */

import http2 from "node:http2";
import util from "node:util";

export class SSEError extends Error {
  constructor(message: string, response?: Response) {
    super(message);
    this.name = "SSEError";
    this.response = response;
  }
  response?: Response;
}

export interface SSEOptions {
  headers?: HeadersInit;
  retries?: number;
  signal?: AbortSignal;
}

export interface SSEEvent {
  id: string | null;
  event: string | null;
  data: string;
  retry?: number;
}

export interface StreamOptions extends SSEOptions {
  decode?: boolean;
}

const RETRY_INTERVAL_DEFAULT = 1000; // Default retry interval in milliseconds

/**
 * Fetches a stream from a URL and yields raw chunks.
 * Handles connection retries.
 *
 * @param url - The URL to stream from
 * @param options - Configuration options
 */
export async function* stream(
  url: string,
  options?: StreamOptions,
): AsyncGenerator<string | Uint8Array, void, unknown> {
  const { headers, retries = 3, signal, decode = true } = options || {};

  let fetchHeaders = {
    ...headers,
  };

  let retryCount = 0;
  let lastError: unknown = null;

  const urlObj = new URL(url);

  while (retryCount <= retries) {
    const client = http2.connect(urlObj.origin);

    try {
      const req = client.request(
        {
          ...fetchHeaders,
          ":path": urlObj.pathname + urlObj.search,
        },
        { signal },
      );

      req.end();

      req.on("response", (headers: Record<string, string>) => {
        const status = Number.parseInt(headers[":status"]);
        const statusText = headers[":statusText"] ?? "";
        if (status) {
          if (status === 204) return; // No Content -> End of stream
          if (status >= 400 && status < 500) {
            throw new SSEError(
              `Stream Client Error`,
              new Response(null, {
                status: status,
                statusText: statusText,
                headers: headers,
              }),
            );
          }
        } else {
          throw new SSEError(
            `Stream Connection Error`,
            new Response(null, {
              status: status,
              statusText: statusText,
              headers: headers,
            }),
          );
        }
      });

      req.on("error", (err: unknown) => {
        req.close();
        throw new SSEError(`SSE Connection Error: ${util.inspect(err)}`);
      });

      const decoder = new TextDecoder();

      for await (const chunk of req) {
        if (decode) {
          yield decoder.decode(chunk, { stream: true });
        } else {
          yield chunk;
        }
      }
      return; // Stream completed successfully
    } catch (error: any) {
      if (signal?.aborted) {
        throw error;
      }
      lastError = error;
      console.error("Stream error:", error);
    } finally {
      retryCount++;
      // Ensure the client is closed to free resources
      if (client && !client.destroyed) {
        client.close();
      }
      // backoff before retrying
      await new Promise((resolve) =>
        setTimeout(resolve, RETRY_INTERVAL_DEFAULT * retryCount),
      );
    }
  }
  throw new SSEError(
    `Max retries (${retries}) exceeded. Last error: ${util.inspect(lastError)}`,
  );
}

/**
 * Fetches a stream and parses it as Server-Sent Events (SSE).
 * Handles 'data', 'event', 'id', 'retry' fields and automatic reconnection.
 *
 * @param url - The URL to stream from
 * @param options - Configuration options
 */
export async function* streamSSE(
  url: string,
  options?: SSEOptions,
): AsyncGenerator<SSEEvent, void, unknown> {
  const { headers, retries = 3, signal } = options || {};

  let lastEventId: string | null = null;
  let retryCount = 0;
  let lastError: unknown = null;

  const urlObj = new URL(url);

  while (retryCount <= retries) {
    const client = http2.connect(urlObj.origin);
    let retryInterval = RETRY_INTERVAL_DEFAULT * (retryCount + 1);
    try {
      let fetchHeaders = {
        ...headers,
      };

      if (lastEventId) {
        fetchHeaders = {
          ...fetchHeaders,
          "Last-Event-ID": lastEventId,
        };
      }

      const req = client.request(
        {
          ...fetchHeaders,
          ":path": urlObj.pathname + urlObj.search,
          Accept: "text/event-stream",
        },
        { signal },
      );

      req.setEncoding("utf8");
      req.end();

      req.on("response", (headers: Record<string, string>) => {
        const status = Number.parseInt(headers[":status"]);
        const statusText = headers[":statusText"] ?? "";
        if (status) {
          if (status === 204) return; // No Content -> End of stream
          if (status >= 400 && status < 500) {
            throw new SSEError(
              `SSE Client Error`,
              new Response(null, {
                status: status as number,
                statusText: statusText,
                headers: headers,
              }),
            );
          }
          retryCount = 0; // Reset retry count on successful connection
        } else {
          throw new SSEError(
            `SSE Connection Error`,
            new Response(null, {
              status: status as number,
              statusText: statusText,
              headers: headers,
            }),
          );
        }
      });

      req.on("error", (err: unknown) => {
        req.close();
        throw new SSEError(`SSE Connection Error: ${util.inspect(err)}`);
      });

      let buffer = "";

      // State machine for event parsing
      let currentEvent: Partial<SSEEvent> = { data: "", event: null, id: null };
      let hasData = false;

      for await (const chunk of req) {
        buffer += chunk;

        const lines = buffer.split(/\r\n|\r|\n/);
        buffer = lines.pop() || ""; // Keep last incomplete line

        for (const line of lines) {
          if (line === "") {
            if (hasData) {
              const data = currentEvent.data?.trimEnd();
              if (data) {
                const event: SSEEvent = {
                  id: currentEvent.id ?? lastEventId,
                  event: currentEvent.event ?? null,
                  data: data,
                  retry: currentEvent.retry,
                };

                if (event.id) lastEventId = event.id;

                yield event;
              }

              currentEvent = { data: "", event: null, id: null };
              hasData = false;
            }
            continue;
          }

          if (line.startsWith(":")) continue;

          const colonIndex = line.indexOf(":");
          let field: string;
          let valueStr = "";

          if (colonIndex === -1) {
            field = line;
          } else {
            field = line.slice(0, colonIndex);
            valueStr = line.slice(colonIndex + 1).trim();
          }

          switch (field) {
            case "data":
              currentEvent.data += valueStr + "\n";
              hasData = true;
              break;
            case "event":
              currentEvent.event = valueStr;
              break;
            case "id":
              if (!valueStr.includes("\0")) {
                currentEvent.id = valueStr;
              }
              break;
            case "retry": {
              const retry = Number.parseInt(valueStr, 10);
              if (!Number.isNaN(retry)) {
                retryInterval = retry;
                currentEvent.retry = retry;
              }
              break;
            }
          }
          // handle any final buffered event if needed
          if (hasData) {
            const data = currentEvent.data?.trimEnd();
            if (data) {
              const event: SSEEvent = {
                id: currentEvent.id ?? lastEventId,
                event: currentEvent.event ?? null,
                data: data,
                retry: currentEvent.retry,
              };
              if (event.id) lastEventId = event.id;
              yield event;
            }
          }
        }
      }

      console.debug("Stream closed by server, reconnecting...");
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      lastError = error;
      console.error("SSE error:", error);
    } finally {
      retryCount++;
      // Ensure the client is closed to free resources
      if (client && !client.destroyed) {
        client.close();
      }
      await new Promise((r) => setTimeout(r, retryInterval));
    }
  }
  throw new SSEError(
    `Max retries exceeded. Last error: ${util.inspect(lastError)}`,
  );
}

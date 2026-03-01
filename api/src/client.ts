
export interface OreOptions {
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

export interface StreamOptions extends OreOptions {
  decode?: boolean;
}

/**
 * Fetches a stream from a URL and yields raw chunks.
 * Handles connection retries.
 * 
 * @param url - The URL to stream from
 * @param options - Configuration options
 */
export async function* stream(
  url: string, 
  options?: StreamOptions
): AsyncGenerator<string | Uint8Array, void, unknown> {
  const { 
    headers, 
    retries = 3, 
    signal, 
    decode = true 
  } = options || {};

  const fetchHeaders = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    ...headers,
  };

  let retryCount = 0;

  while (retryCount <= retries) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: fetchHeaders,
        signal,
      });

      if (!response.ok) {
        if (response.status >= 400 && response.status < 500) {
          throw new Error(`Client Error: ${response.status} ${response.statusText}`);
        }
        throw new Error(`Server Error: ${response.status} ${response.statusText}`);
      }

      if (!response.body) {
        throw new Error("Response body is null");
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          
          if (decode) {
            yield decoder.decode(value, { stream: true });
          } else {
            yield value;
          }
        }
      } finally {
        reader.releaseLock();
      }

      return; // Stream completed successfully
    } catch (error: any) {
      if (signal?.aborted) {
        throw error;
      }
      
      console.error("Stream error:", error);
      retryCount++;
      
      if (retryCount > retries) {
        throw new Error(`Max retries (${retries}) exceeded. Last error: ${error.message || error}`);
      }
      
      await new Promise((resolve) => setTimeout(resolve, 1000 * retryCount));
    }
  }
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
  options?: OreOptions
): AsyncGenerator<SSEEvent, void, unknown> {
  const { 
    headers: customHeaders, 
    retries = 3, 
    signal 
  } = options || {};

  let lastEventId: string | null = null;
  let retryCount = 0;
  let retryInterval = 1000;

  while (retryCount <= retries) {
    try {
      const headers = { 
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        ...customHeaders 
      };
      
      if (lastEventId) {
        (headers as any)["Last-Event-ID"] = lastEventId;
      }

      const response = await fetch(url, {
        method: "GET",
        headers,
        signal,
      });

      if (!response.ok) {
         if (response.status === 204) return; // No Content -> End of stream
         if (response.status >= 400 && response.status < 500) {
           throw new Error(`Client Error: ${response.status} ${response.statusText}`);
         }
         throw new Error(`Failed to connect: ${response.status} ${response.statusText}`);
      }
      
      if (!response.body) throw new Error("Response body is null");

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      
      let buffer = "";
      
      // State machine for event parsing
      let currentEvent: Partial<SSEEvent> = { data: "", event: null, id: null };
      let hasData = false;

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          
          buffer += decoder.decode(value, { stream: true });
          
          const lines = buffer.split(/\r\n|\r|\n/);
          buffer = lines.pop() || ""; // Keep last incomplete line

          for (const line of lines) {
            if (line === "") {
              if (hasData) {
                 let data = currentEvent.data!;
                 if (data.endsWith("\n")) {
                    data = data.slice(0, -1);
                 }
                 
                 const event: SSEEvent = {
                   id: currentEvent.id ?? lastEventId,
                   event: currentEvent.event ?? null,
                   data: data,
                   retry: currentEvent.retry
                 };
                 
                 if (event.id) lastEventId = event.id;
                 
                 yield event;
                 
                 currentEvent = { data: "", event: null, id: null };
                 hasData = false;
              }
              continue;
            }

            if (line.startsWith(":")) continue;

            const colonIndex = line.indexOf(":");
            let field = "";
            let valueStr = "";

            if (colonIndex === -1) {
              field = line;
              valueStr = "";
            } else {
              field = line.slice(0, colonIndex);
              valueStr = line.slice(colonIndex + 1);
              if (valueStr.startsWith(" ")) valueStr = valueStr.slice(1);
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
                if (valueStr.indexOf("\0") === -1) {
                  currentEvent.id = valueStr;
                }
                break;
              case "retry":
                const retry = parseInt(valueStr, 10);
                if (!isNaN(retry)) {
                  retryInterval = retry;
                  currentEvent.retry = retry;
                }
                break;
            }
          }
        }
      } finally {
         reader.releaseLock();
      }

      console.log("Stream closed by server, reconnecting...");
      retryCount++;
      await new Promise(r => setTimeout(r, retryInterval));

    } catch (error: any) {
      if (signal?.aborted) throw error;
      
      console.error("SSE error:", error);
      retryCount++;
      if (retryCount > retries) {
         throw new Error(`Max retries exceeded. Last error: ${error.message}`);
      }
      await new Promise(r => setTimeout(r, retryInterval));
    }
  }
}

# Ore

Ore is a modern, lightweight JavaScript/TypeScript library for robust streaming and Server-Sent Events (SSE) consumption. It provides a simple, bulletproof API for handling streams with automatic retries and easy integration into modern frameworks like React, Next.js, and more.

## Features

- **Bulletproof Streaming:** Robust handling of connection drops with automatic retries.
- **Dual Mode:** 
  - `stream()`: For raw text/byte streaming (e.g. AI responses, logs).
  - `streamSSE()`: For spec-compliant Server-Sent Events parsing.
- **Modern API:** Uses Async Generators for clean, modern usage (`for await...of`).
- **Universal:** Works in Browser, Node.js, and Edge runtimes.
- **Zero Dependencies:** Tiny footprint.

## Install

```bash
npm install @glamboyosa/ore
```

## Usage

### Raw Streaming (Text/Bytes)

Perfect for AI chat streams, logs, or custom protocols.

```typescript
import { stream } from "@glamboyosa/ore";

// Basic usage
for await (const chunk of stream("http://api.example.com/chat")) {
  console.log(chunk); // "Hello", " world", "!"
}

// With options
const ac = new AbortController();
const dataStream = stream("http://api.example.com/chat", {
  headers: { "Authorization": "Bearer token" },
  retries: 3,
  signal: ac.signal,
  decode: true, // Set to false to get Uint8Array
});

for await (const chunk of dataStream) {
  // ...
}
```

### Server-Sent Events (SSE)

Parses standard SSE format (`data: ...`, `event: ...`, `id: ...`).

```typescript
import { streamSSE } from "@glamboyosa/ore";

for await (const event of streamSSE("http://api.example.com/events")) {
  console.log(event.id);
  console.log(event.event); // e.g., 'update'
  console.log(event.data);  // The message payload
}
```

### Usage with React

```tsx
import { useEffect, useState } from "react";
import { stream } from "@glamboyosa/ore";

function ChatComponent() {
  const [messages, setMessages] = useState("");

  useEffect(() => {
    const controller = new AbortController();

    (async () => {
      try {
        for await (const chunk of stream("/api/chat", { signal: controller.signal })) {
          setMessages(prev => prev + chunk);
        }
      } catch (err) {
        if (err.name !== 'AbortError') console.error(err);
      }
    })();

    return () => controller.abort();
  }, []);

  return <div>{messages}</div>;
}
```

### Usage with Next.js Server Components

Ore works great with React Server Components (RSC) and Suspense for streaming HTML.

```tsx
import { stream } from "@glamboyosa/ore";
import { Suspense } from "react";

// Recursive component to stream data
async function StreamViewer({ iterator }) {
  const { value, done } = await iterator.next();
  if (done) return null;
  
  return (
    <span>
      {value}
      <Suspense>
        <StreamViewer iterator={iterator} />
      </Suspense>
    </span>
  );
}

export default function Page() {
  const dataStream = stream("http://api.example.com/stream");
  const iterator = dataStream[Symbol.asyncIterator]();

  return (
    <Suspense fallback="Loading...">
      <StreamViewer iterator={iterator} />
    </Suspense>
  );
}
```

## API Reference

### `stream(url: string, options?: StreamOptions)`

Returns an `AsyncGenerator<string | Uint8Array>`.

**Options:**
- `headers`: `HeadersInit` - Custom headers.
- `retries`: `number` (default: 3) - Max retry attempts on failure.
- `signal`: `AbortSignal` - To cancel the request.
- `decode`: `boolean` (default: true) - If true, yields strings. If false, yields `Uint8Array`.

### `streamSSE(url: string, options?: OreOptions)`

Returns an `AsyncGenerator<SSEEvent>`.

**Options:**
- `headers`: `HeadersInit`
- `retries`: `number` (default: 3)
- `signal`: `AbortSignal`

**SSEEvent Interface:**
```typescript
interface SSEEvent {
  id: string | null;
  event: string | null;
  data: string;
  retry?: number;
}
```

## License

MIT

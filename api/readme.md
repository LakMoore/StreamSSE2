# SSE Events 2

Forked from Ore by Timothy Ogbemudia, SSE Events 2 is a modern, lightweight TypeScript library for robust streaming and Server-Sent Events (SSE) consumption. It provides a simple, bulletproof API for handling streams with automatic retries and easy integration into Node.js server-side code.

Unlike Ore, this is not for integration into client-side code.

Specific to my needs, this fork uses the node:http2 client to open streams.  There are other solutions published if you can use http1.

## Features

- **Bulletproof Streaming:** Robust handling of connection drops with automatic retries.
- **Dual Mode:** 
  - `stream()`: For raw text/byte streaming (e.g. AI responses, logs).
  - `streamSSE()`: For spec-compliant Server-Sent Events parsing.
- **Modern API:** Uses Async Generators for clean, modern usage (`for await...of`).
- **Build for Node.js:** Works in Node.js (not your browser).
- **Zero Dependencies:** Tiny footprint.

## Install

```bash
npm install sse-events-2
```

## Usage

### Raw Streaming (Text/Bytes)

Perfect for AI chat streams, logs, or custom protocols.

```typescript
import { stream } from "sse-events-2";

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
import { streamSSE } from "sse-events-2";

for await (const event of streamSSE("http://api.example.com/events")) {
  console.log(event.id);
  console.log(event.event); // e.g., 'update'
  console.log(event.data);  // The message payload
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

### `streamSSE(url: string, options?: SSEOptions)`

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

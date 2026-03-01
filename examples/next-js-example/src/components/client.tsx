"use client";
import { useEffect, useState, useRef } from "react";
import { stream } from "@glamboyosa/ore";

type ClientProps = {};

const Client: React.FC<ClientProps> = () => {
  const [chat, setCurrentChat] = useState("");
  const isFirstRun = useRef(true);

  useEffect(() => {
    if (isFirstRun.current) {
      isFirstRun.current = false;
      const controller = new AbortController();

      (async () => {
        try {
          console.log("Starting client stream...");
          for await (const chunk of stream("http://localhost:4000/", {
            headers: { "Cache-Control": "no-cache" },
            signal: controller.signal,
          })) {
            setCurrentChat((prev) => prev + (chunk as string));
          }
        } catch (err: any) {
           if (err.name !== 'AbortError') {
             console.error("Stream failed", err);
           }
        }
      })();

      return () => {
        controller.abort();
      };
    }
  }, []);

  return (
    <div
      suppressHydrationWarning
      className="flex w-full items-center justify-center gap-4"
    >
      <h4> Client Component:</h4>
      <p className="w-1/2">{chat}</p>
    </div>
  );
};

export default Client;

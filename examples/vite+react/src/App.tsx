import reactLogo from "./assets/react.svg";
import viteLogo from "/vite.svg";
import "./App.css";
import { stream } from "@glamboyosa/ore";
import { useEffect, useRef, useState } from "react";

function App() {
  const [chat, setCurrentChat] = useState("");
  const isFirstRun = useRef(true);
  
  useEffect(() => {
    if (isFirstRun.current) {
      isFirstRun.current = false;
      const controller = new AbortController();

      (async () => {
        try {
          console.log("Starting stream...");
          for await (const chunk of stream("http://localhost:4000/", {
            headers: { "Cache-Control": "no-cache" },
            signal: controller.signal,
          })) {
            // The server sends raw text chunks, so we just append them.
            // stream() yields decoded strings by default.
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
    <>
      <div>
        <a href="https://vitejs.dev" target="_blank">
          <img src={viteLogo} className="logo" alt="Vite logo" />
        </a>
        <a href="https://react.dev" target="_blank">
          <img src={reactLogo} className="logo react" alt="React logo" />
        </a>
      </div>
      <h1>Vite + React</h1>
      <div className="card">
        <p>Stream:</p>
        <p>{chat}</p>
      </div>
    </>
  );
}

export default App;

import { useEffect, useRef, useState, useCallback } from "react";

const RECONNECT_DELAY = 3000;
const DEFAULT_WS_URL = (process.env.REACT_APP_API_BASE || "http://localhost:8000")
  .replace(/^http/, "ws") + "/ws";

export default function useWebSocket(url = DEFAULT_WS_URL) {
  const [lastMessage, setLastMessage] = useState(null);
  const [isConnected, setIsConnected] = useState(false);
  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(url);

    ws.onopen = () => {
      setIsConnected(true);
      console.log("WS connected");
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setLastMessage(data);
      } catch (e) {
        console.error("WS parse error:", e);
      }
    };

    ws.onclose = () => {
      setIsConnected(false);
      console.log("WS disconnected, reconnecting...");
      reconnectTimer.current = setTimeout(connect, RECONNECT_DELAY);
    };

    ws.onerror = (err) => {
      console.error("WS error:", err);
      ws.close();
    };

    wsRef.current = ws;
  }, [url]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  // Send a ping to keep the connection alive
  useEffect(() => {
    const interval = setInterval(() => {
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        wsRef.current.send("ping");
      }
    }, 30000);
    return () => clearInterval(interval);
  }, []);

  return { lastMessage, isConnected };
}

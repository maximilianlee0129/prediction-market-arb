import { useState, useEffect, useCallback } from "react";

const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:8000";

export default function useApi(path, { pollInterval = 10000, params = {} } = {}) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const queryString = new URLSearchParams(params).toString();
  const url = `${API_BASE}${path}${queryString ? `?${queryString}` : ""}`;

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [url]);

  useEffect(() => {
    fetchData();
    if (pollInterval > 0) {
      const interval = setInterval(fetchData, pollInterval);
      return () => clearInterval(interval);
    }
  }, [fetchData, pollInterval]);

  return { data, loading, error, refetch: fetchData };
}

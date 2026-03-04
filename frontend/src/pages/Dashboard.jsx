import { useState, useEffect } from "react";
import useApi from "../hooks/useApi";
import useWebSocket from "../hooks/useWebSocket";
import ArbTable from "../components/ArbTable";

export default function Dashboard() {
  const { data: stats } = useApi("/api/stats", { pollInterval: 10000 });
  const { data: apiOpps, loading } = useApi("/api/opportunities", {
    pollInterval: 10000,
    params: { active_only: true, sort_by: "composite_score", limit: 50 },
  });
  const { lastMessage, isConnected } = useWebSocket();

  // Merge API data with WebSocket updates
  const [opportunities, setOpportunities] = useState([]);

  useEffect(() => {
    if (apiOpps) setOpportunities(apiOpps);
  }, [apiOpps]);

  useEffect(() => {
    if (lastMessage?.type === "opportunities" && lastMessage.data) {
      setOpportunities(lastMessage.data);
    }
  }, [lastMessage]);

  return (
    <div className="space-y-6">
      {/* Stats Bar */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <StatCard label="Kalshi Markets" value={stats?.kalshi_markets} />
        <StatCard label="Polymarket" value={stats?.polymarket_markets} />
        <StatCard label="Matched Pairs" value={stats?.matched_pairs} />
        <StatCard label="Active Arbs" value={stats?.active_opportunities} color="text-green-400" />
        <StatCard
          label="WebSocket"
          value={isConnected ? "Connected" : "Disconnected"}
          color={isConnected ? "text-green-400" : "text-red-400"}
        />
      </div>

      {/* Main Table */}
      <div className="bg-gray-900 rounded-lg border border-gray-800">
        <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between">
          <h2 className="font-medium">Live Arbitrage Opportunities</h2>
          <span className="text-xs text-gray-500">
            {opportunities.length} active | sorted by composite score
          </span>
        </div>
        {loading && !opportunities.length ? (
          <div className="py-12 text-center text-gray-500">Loading...</div>
        ) : (
          <ArbTable opportunities={opportunities} />
        )}
      </div>
    </div>
  );
}

function StatCard({ label, value, color = "text-white" }) {
  return (
    <div className="bg-gray-900 rounded-lg border border-gray-800 p-4">
      <div className="text-xs text-gray-400 mb-1">{label}</div>
      <div className={`text-xl font-bold font-mono ${color}`}>
        {value ?? "-"}
      </div>
    </div>
  );
}

import { useState } from "react";
import useApi from "../hooks/useApi";

export default function MarketExplorer() {
  const [platform, setPlatform] = useState("");
  const [search, setSearch] = useState("");

  const { data: markets, loading } = useApi("/api/markets", {
    pollInterval: 15000,
    params: {
      ...(platform && { platform }),
      ...(search && { search }),
      limit: 100,
    },
  });

  return (
    <div>
      {/* Filters */}
      <div className="flex gap-3 mb-4">
        <select
          value={platform}
          onChange={(e) => setPlatform(e.target.value)}
          className="bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm"
        >
          <option value="">All Platforms</option>
          <option value="kalshi">Kalshi</option>
          <option value="polymarket">Polymarket</option>
        </select>
        <input
          type="text"
          placeholder="Search markets..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="bg-gray-800 border border-gray-700 rounded px-3 py-1.5 text-sm flex-1 max-w-md"
        />
      </div>

      {loading && !markets ? (
        <div className="text-gray-500 py-8 text-center">Loading markets...</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-400 border-b border-gray-800">
                <th className="text-left py-3 px-4">Platform</th>
                <th className="text-left py-3 px-4">Title</th>
                <th className="text-right py-3 px-2">YES</th>
                <th className="text-right py-3 px-2">NO</th>
                <th className="text-right py-3 px-2">Bid</th>
                <th className="text-right py-3 px-2">Ask</th>
                <th className="text-right py-3 px-2">Volume 24h</th>
                <th className="text-right py-3 px-4">Liquidity</th>
              </tr>
            </thead>
            <tbody>
              {(markets || []).map((m) => (
                <tr key={`${m.platform}-${m.id}`} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                  <td className="py-2 px-4">
                    <span className={`text-xs font-medium px-2 py-0.5 rounded ${
                      m.platform === "kalshi"
                        ? "bg-blue-900/50 text-blue-300"
                        : "bg-purple-900/50 text-purple-300"
                    }`}>
                      {m.platform === "kalshi" ? "K" : "P"}
                    </span>
                  </td>
                  <td className="py-2 px-4 max-w-sm truncate">{m.title}</td>
                  <td className="text-right py-2 px-2 font-mono">{m.yes_price?.toFixed(2)}</td>
                  <td className="text-right py-2 px-2 font-mono">{m.no_price?.toFixed(2)}</td>
                  <td className="text-right py-2 px-2 font-mono text-gray-400">{m.yes_bid?.toFixed(2)}</td>
                  <td className="text-right py-2 px-2 font-mono text-gray-400">{m.yes_ask?.toFixed(2)}</td>
                  <td className="text-right py-2 px-2 font-mono">
                    {m.volume_24h ? `$${(m.volume_24h / 1000).toFixed(0)}k` : "-"}
                  </td>
                  <td className="text-right py-2 px-4 font-mono">
                    {m.liquidity ? `$${(m.liquidity / 1000).toFixed(0)}k` : "-"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {(!markets || markets.length === 0) && (
            <div className="text-center py-8 text-gray-500">No markets found</div>
          )}
        </div>
      )}
    </div>
  );
}

import MarketExplorer from "../components/MarketExplorer";
import useApi from "../hooks/useApi";

export default function Markets() {
  const { data: pairs } = useApi("/api/matched-pairs", {
    pollInterval: 30000,
    params: { limit: 50 },
  });

  return (
    <div className="space-y-6">
      {/* Matched Pairs Section */}
      <div className="bg-gray-900 rounded-lg border border-gray-800">
        <div className="px-4 py-3 border-b border-gray-800">
          <h2 className="font-medium">Matched Market Pairs</h2>
          <span className="text-xs text-gray-500">{pairs?.length || 0} active pairs</span>
        </div>
        <div className="overflow-x-auto">
          {pairs && pairs.length > 0 ? (
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-400 border-b border-gray-800">
                  <th className="text-left py-3 px-4">Kalshi Market</th>
                  <th className="text-left py-3 px-4">Polymarket</th>
                  <th className="text-right py-3 px-2">Confidence</th>
                  <th className="text-right py-3 px-4">Method</th>
                </tr>
              </thead>
              <tbody>
                {pairs.map((p) => (
                  <tr key={p.id} className="border-b border-gray-800/50 hover:bg-gray-800/30">
                    <td className="py-2 px-4 max-w-xs truncate">{p.kalshi_title}</td>
                    <td className="py-2 px-4 max-w-xs truncate">{p.poly_title}</td>
                    <td className="text-right py-2 px-2 font-mono">
                      {(p.confidence_score * 100).toFixed(0)}%
                    </td>
                    <td className="text-right py-2 px-4">
                      <span className={`text-xs px-2 py-0.5 rounded ${
                        p.match_method === "claude_api"
                          ? "bg-blue-900/50 text-blue-300"
                          : "bg-gray-800 text-gray-400"
                      }`}>
                        {p.match_method}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="py-8 text-center text-gray-500">
              No matched pairs yet. The matcher runs every 5 minutes.
            </div>
          )}
        </div>
      </div>

      {/* All Markets Browser */}
      <div className="bg-gray-900 rounded-lg border border-gray-800">
        <div className="px-4 py-3 border-b border-gray-800">
          <h2 className="font-medium">All Markets</h2>
        </div>
        <div className="p-4">
          <MarketExplorer />
        </div>
      </div>
    </div>
  );
}

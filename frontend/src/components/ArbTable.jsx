import ColorBadge from "./ColorBadge";

function rowColor(profitPct) {
  if (profitPct >= 3) return "bg-green-950/30 hover:bg-green-950/50";
  if (profitPct >= 1) return "bg-yellow-950/20 hover:bg-yellow-950/40";
  return "hover:bg-gray-800/50";
}

export default function ArbTable({ opportunities = [] }) {
  if (!opportunities.length) {
    return (
      <div className="text-center py-12 text-gray-500">
        No arbitrage opportunities detected yet. Waiting for data...
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-gray-400 border-b border-gray-800">
            <th className="text-left py-3 px-4">Market</th>
            <th className="text-right py-3 px-2">Direction</th>
            <th className="text-right py-3 px-2">Kalshi</th>
            <th className="text-right py-3 px-2">Poly</th>
            <th className="text-right py-3 px-2">Spread</th>
            <th className="text-right py-3 px-2">Net Profit</th>
            <th className="text-right py-3 px-2">Annual.</th>
            <th className="text-right py-3 px-2">Confidence</th>
            <th className="text-right py-3 px-4">Score</th>
          </tr>
        </thead>
        <tbody>
          {opportunities.map((opp, i) => (
            <tr key={opp.id || i} className={`border-b border-gray-800/50 ${rowColor(opp.net_profit_pct)}`}>
              <td className="py-2 px-4 max-w-xs truncate">
                <div className="font-medium">{opp.kalshi_title || `Pair #${opp.matched_pair_id}`}</div>
                {opp.poly_title && (
                  <div className="text-xs text-gray-500 truncate">{opp.poly_title}</div>
                )}
              </td>
              <td className="text-right py-2 px-2 text-xs text-gray-400">
                {opp.direction === "kalshi_yes_poly_no" ? "K-YES / P-NO" : "K-NO / P-YES"}
              </td>
              <td className="text-right py-2 px-2 font-mono">{opp.kalshi_price?.toFixed(2)}</td>
              <td className="text-right py-2 px-2 font-mono">{opp.poly_price?.toFixed(2)}</td>
              <td className="text-right py-2 px-2 font-mono">{(opp.raw_spread * 100)?.toFixed(1)}%</td>
              <td className="text-right py-2 px-2">
                <ColorBadge value={opp.net_profit_pct} />
              </td>
              <td className="text-right py-2 px-2 font-mono text-xs">
                {opp.annualized_return > 0 ? `${opp.annualized_return.toFixed(0)}%` : "-"}
              </td>
              <td className="text-right py-2 px-2 font-mono text-xs">
                {(opp.match_confidence * 100).toFixed(0)}%
              </td>
              <td className="text-right py-2 px-4">
                <span className="font-mono font-bold text-blue-400">
                  {opp.composite_score?.toFixed(3)}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

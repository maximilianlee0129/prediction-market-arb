import useApi from "../hooks/useApi";

export default function OpportunityLog() {
  const { data: logs, loading } = useApi("/api/opportunities/history/log", {
    pollInterval: 15000,
    params: { limit: 100 },
  });

  if (loading && !logs) {
    return <div className="text-gray-500 py-8 text-center">Loading history...</div>;
  }

  if (!logs || logs.length === 0) {
    return <div className="text-gray-500 py-8 text-center">No opportunity events logged yet</div>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-gray-400 border-b border-gray-800">
            <th className="text-left py-3 px-4">Time</th>
            <th className="text-left py-3 px-2">Event</th>
            <th className="text-right py-3 px-2">Opp. ID</th>
            <th className="text-right py-3 px-4">Net Profit</th>
          </tr>
        </thead>
        <tbody>
          {logs.map((log) => (
            <tr key={log.id} className="border-b border-gray-800/50 hover:bg-gray-800/30">
              <td className="py-2 px-4 text-gray-400 text-xs">
                {log.recorded_at ? new Date(log.recorded_at).toLocaleString() : "-"}
              </td>
              <td className="py-2 px-2">
                <span className={`text-xs font-medium px-2 py-0.5 rounded ${
                  log.event_type === "detected"
                    ? "bg-green-900/50 text-green-300"
                    : log.event_type === "closed"
                    ? "bg-red-900/50 text-red-300"
                    : "bg-gray-800 text-gray-300"
                }`}>
                  {log.event_type}
                </span>
              </td>
              <td className="text-right py-2 px-2 font-mono text-gray-400">#{log.opportunity_id}</td>
              <td className="text-right py-2 px-4 font-mono">
                {log.net_profit_pct > 0 ? `${log.net_profit_pct.toFixed(2)}%` : "-"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

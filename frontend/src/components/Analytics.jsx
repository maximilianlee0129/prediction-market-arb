import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import useApi from "../hooks/useApi";

export default function Analytics() {
  const { data: opps } = useApi("/api/opportunities", {
    pollInterval: 30000,
    params: { active_only: false, limit: 200 },
  });

  if (!opps || opps.length === 0) {
    return <div className="text-gray-500 py-8 text-center">No data for analytics yet</div>;
  }

  // Group opportunities by date for charts
  const byDate = {};
  for (const opp of opps) {
    const date = opp.detected_at?.split("T")[0] || "unknown";
    if (!byDate[date]) byDate[date] = { date, count: 0, totalProfit: 0, maxProfit: 0 };
    byDate[date].count += 1;
    byDate[date].totalProfit += opp.net_profit_pct;
    byDate[date].maxProfit = Math.max(byDate[date].maxProfit, opp.net_profit_pct);
  }
  const dailyData = Object.values(byDate)
    .sort((a, b) => a.date.localeCompare(b.date))
    .map((d) => ({
      ...d,
      avgProfit: d.count > 0 ? +(d.totalProfit / d.count).toFixed(2) : 0,
    }));

  // Spread distribution
  const spreadBuckets = [
    { range: "0-1%", count: 0 },
    { range: "1-3%", count: 0 },
    { range: "3-5%", count: 0 },
    { range: "5-10%", count: 0 },
    { range: "10%+", count: 0 },
  ];
  for (const opp of opps) {
    const p = opp.net_profit_pct;
    if (p < 1) spreadBuckets[0].count++;
    else if (p < 3) spreadBuckets[1].count++;
    else if (p < 5) spreadBuckets[2].count++;
    else if (p < 10) spreadBuckets[3].count++;
    else spreadBuckets[4].count++;
  }

  return (
    <div className="space-y-8">
      {/* Arbs per Day */}
      <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
        <h3 className="text-sm font-medium text-gray-400 mb-4">Opportunities Detected per Day</h3>
        <ResponsiveContainer width="100%" height={250}>
          <BarChart data={dailyData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="date" tick={{ fill: "#9ca3af", fontSize: 11 }} />
            <YAxis tick={{ fill: "#9ca3af", fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: "#1f2937", border: "1px solid #374151", borderRadius: 8 }}
              labelStyle={{ color: "#d1d5db" }}
            />
            <Bar dataKey="count" fill="#3b82f6" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Avg Spread over Time */}
      <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
        <h3 className="text-sm font-medium text-gray-400 mb-4">Average Net Profit % over Time</h3>
        <ResponsiveContainer width="100%" height={250}>
          <LineChart data={dailyData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="date" tick={{ fill: "#9ca3af", fontSize: 11 }} />
            <YAxis tick={{ fill: "#9ca3af", fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: "#1f2937", border: "1px solid #374151", borderRadius: 8 }}
              labelStyle={{ color: "#d1d5db" }}
            />
            <Line type="monotone" dataKey="avgProfit" stroke="#10b981" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="maxProfit" stroke="#f59e0b" strokeWidth={1} strokeDasharray="4 4" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Spread Distribution */}
      <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
        <h3 className="text-sm font-medium text-gray-400 mb-4">Profit Distribution</h3>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={spreadBuckets}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="range" tick={{ fill: "#9ca3af", fontSize: 11 }} />
            <YAxis tick={{ fill: "#9ca3af", fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: "#1f2937", border: "1px solid #374151", borderRadius: 8 }}
              labelStyle={{ color: "#d1d5db" }}
            />
            <Bar dataKey="count" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

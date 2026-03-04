import OpportunityLog from "../components/OpportunityLog";
import Analytics from "../components/Analytics";

export default function History() {
  return (
    <div className="space-y-6">
      {/* Analytics Charts */}
      <div className="bg-gray-900 rounded-lg border border-gray-800">
        <div className="px-4 py-3 border-b border-gray-800">
          <h2 className="font-medium">Analytics</h2>
        </div>
        <div className="p-4">
          <Analytics />
        </div>
      </div>

      {/* Event Log */}
      <div className="bg-gray-900 rounded-lg border border-gray-800">
        <div className="px-4 py-3 border-b border-gray-800">
          <h2 className="font-medium">Opportunity Event Log</h2>
        </div>
        <OpportunityLog />
      </div>
    </div>
  );
}

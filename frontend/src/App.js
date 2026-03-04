import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import Markets from "./pages/Markets";
import History from "./pages/History";

function NavItem({ to, children }) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        `px-3 py-1.5 rounded text-sm font-medium transition-colors ${
          isActive
            ? "bg-gray-800 text-white"
            : "text-gray-400 hover:text-gray-200"
        }`
      }
    >
      {children}
    </NavLink>
  );
}

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-950 text-gray-100">
        {/* Header */}
        <header className="border-b border-gray-800 bg-gray-900/50 backdrop-blur sticky top-0 z-10">
          <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <h1 className="text-lg font-bold">Arb Scanner</h1>
              <span className="text-xs text-gray-500 bg-gray-800 px-2 py-0.5 rounded">
                Kalshi / Polymarket
              </span>
            </div>
            <nav className="flex gap-1">
              <NavItem to="/">Dashboard</NavItem>
              <NavItem to="/markets">Markets</NavItem>
              <NavItem to="/history">History</NavItem>
            </nav>
          </div>
        </header>

        {/* Content */}
        <main className="max-w-7xl mx-auto px-4 py-6">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/markets" element={<Markets />} />
            <Route path="/history" element={<History />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;

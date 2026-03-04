export default function ColorBadge({ value, thresholds = [1, 3] }) {
  const [low, high] = thresholds;
  let bg, text;

  if (value >= high) {
    bg = "bg-green-900/50";
    text = "text-green-300";
  } else if (value >= low) {
    bg = "bg-yellow-900/50";
    text = "text-yellow-300";
  } else {
    bg = "bg-red-900/50";
    text = "text-red-300";
  }

  return (
    <span className={`px-2 py-0.5 rounded text-sm font-mono ${bg} ${text}`}>
      {value.toFixed(2)}%
    </span>
  );
}

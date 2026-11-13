export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#09090b",
        panel: "#18181b",
        panelAlt: "#1f1f23",
        border: "#27272a",
        accent: "#7c3aed",
        success: "#22c55e",
        warning: "#f59e0b",
        danger: "#ef4444",
      },
      fontFamily: {
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      boxShadow: {
        glow: "0 0 30px rgba(124,58,237,0.25)",
      },
    },
  },
  plugins: [],
};

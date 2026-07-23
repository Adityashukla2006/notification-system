import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Emits .next/standalone with a minimal server.js and only the node_modules
  // actually reached at runtime. That is what lets the Docker image ship
  // without installing dependencies at all — a few hundred MB smaller, and a
  // far smaller surface than shipping the whole dependency tree.
  output: "standalone",
};

export default nextConfig;

import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactCompiler: true,
  output: "standalone", // self-contained .next/standalone for the Docker image (Phase 7)
};

export default nextConfig;

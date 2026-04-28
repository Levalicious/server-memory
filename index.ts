#!/usr/bin/env node

// MUST be the first import: brings up the OTel SDK (when enabled via env)
// before any MCP modules construct anything that should be traced.
import "./src/tracing.js";

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createServer } from "./server.js";

// Workaround: Node 24 segfaults on exit when any N-API addon is loaded,
// even a bare no-op module. This is a confirmed Node bug, not ours.
// Force a clean exit to avoid the cosmetic segfault.
// process.on('exit', () => { process._exit(0); });

const server = createServer();
const transport = new StdioServerTransport();
server.connect(transport).catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});

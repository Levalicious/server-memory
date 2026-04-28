import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { promises as fs } from 'fs';
import path from 'path';
import os from 'os';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

// ESM-safe equivalent of CommonJS __dirname.
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * End-to-end stdio-purity check for the OpenTelemetry bootstrap.
 *
 * Stdout is reserved for MCP JSON-RPC; any stray byte corrupts the channel.
 * This test spawns the actual built binary with `OTEL_SDK_DISABLED=true`,
 * sends a real JSON-RPC `tools/call`, and asserts that every newline-delimited
 * chunk on stdout parses as valid JSON-RPC. No diag noise, no console-exporter
 * spillover, no random init banners.
 */
describe('tracing — stdio purity', () => {
  let testDir: string;
  let memoryFile: string;

  beforeEach(async () => {
    testDir = await fs.mkdtemp(path.join(os.tmpdir(), 'mcp-memory-trace-'));
    memoryFile = path.join(testDir, 'test-memory.json');
  });

  afterEach(async () => {
    try {
      await fs.rm(testDir, { recursive: true });
    } catch {
      // ignore
    }
  });

  it('emits only valid JSON-RPC on stdout when OTEL_SDK_DISABLED=true', async () => {
    const distPath = path.resolve(__dirname, '..', 'dist', 'index.js');

    const child = spawn(process.execPath, [distPath], {
      env: {
        ...process.env,
        OTEL_SDK_DISABLED: 'true',
        // Belt-and-braces: even if a user accidentally set =console while
        // disabled, our bootstrap should not start the SDK at all.
        OTEL_TRACES_EXPORTER: 'console',
        MEMORY_FILE_PATH: memoryFile,
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    let stdoutBuf = '';
    child.stdout.on('data', (chunk: Buffer) => { stdoutBuf += chunk.toString('utf8'); });
    let stderrBuf = '';
    child.stderr.on('data', (chunk: Buffer) => { stderrBuf += chunk.toString('utf8'); });

    // 1) initialize
    const initReq = {
      jsonrpc: '2.0',
      id: 1,
      method: 'initialize',
      params: {
        protocolVersion: '2024-11-05',
        capabilities: {},
        clientInfo: { name: 'trace-test', version: '0.0.0' },
      },
    };
    child.stdin.write(JSON.stringify(initReq) + '\n');

    // 2) tools/call search_nodes — exercises the wrapped dispatch path.
    const callReq = {
      jsonrpc: '2.0',
      id: 2,
      method: 'tools/call',
      params: {
        name: 'search_nodes',
        arguments: { query: 'anything' },
        _meta: {
          // Realistic carrier — should be silently dropped under disabled SDK.
          traceparent: '00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01',
        },
      },
    };

    // Wait for initialize response before sending the second request to
    // avoid races; rather than parsing partial stream, just delay briefly.
    await new Promise(r => setTimeout(r, 500));
    child.stdin.write(JSON.stringify(callReq) + '\n');

    // Give the server a moment, then close cleanly.
    await new Promise(r => setTimeout(r, 800));
    child.stdin.end();

    await new Promise<void>((resolve) => {
      let settled = false;
      const finish = (): void => { if (!settled) { settled = true; clearTimeout(killTimer); resolve(); } };
      child.on('exit', finish);
      // Force-kill backstop if it doesn't exit on its own. Cleared on normal exit.
      const killTimer = setTimeout(() => {
        try { child.kill('SIGKILL'); } catch { /* */ }
        finish();
      }, 4000);
    });

    // Every non-empty line on stdout MUST be a valid JSON-RPC envelope.
    const lines = stdoutBuf.split('\n').filter(l => l.trim().length > 0);
    expect(lines.length).toBeGreaterThan(0);
    for (const line of lines) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch {
        throw new Error(
          `Non-JSON line on stdout (would corrupt MCP): ${JSON.stringify(line)}\nstderr: ${stderrBuf}`
        );
      }
      expect(parsed).toMatchObject({ jsonrpc: '2.0' });
    }
  }, 15000);
});

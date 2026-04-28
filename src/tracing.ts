/**
 * OpenTelemetry bootstrap for the stdio MCP memory server.
 *
 * Stdio MCP is hostile to OTel's stdout-writing defaults: any stray byte on
 * stdout corrupts the JSON-RPC stream. This module:
 *   - never attaches the default DiagConsoleLogger (which writes to stdout);
 *     instead, if OTEL_LOG_LEVEL is set, attaches a tiny stderr-only logger.
 *   - rejects OTEL_*_EXPORTER=console at startup (would dump JSON to stdout).
 *   - disables resource auto-detection (some detectors emit diag warnings).
 *   - skips NodeSDK.start() entirely if OTEL_SDK_DISABLED=true or
 *     OTEL_EXPORTER_OTLP_ENDPOINT is unset, so users who don't opt in pay zero
 *     overhead and have zero stdout-corruption risk.
 *
 * When enabled, propagation defaults include W3C traceparent/tracestate, so
 * carriers extracted from MCP `params._meta` parent-link the resulting span.
 *
 * Transport: OTLP/gRPC. Default endpoint is grpc://localhost:4317 unless
 * overridden via OTEL_EXPORTER_OTLP_ENDPOINT (or the trace/metrics-specific
 * variants). The endpoint URL must be a gRPC-style URL (no /v1/traces path).
 */

import {
  diag,
  DiagLogLevel,
  metrics,
  trace,
  type DiagLogger,
  type Histogram,
  type Meter,
  type Tracer,
} from '@opentelemetry/api';

const SERVICE_NAME = process.env.OTEL_SERVICE_NAME ?? 'memory-server';
const SERVICE_VERSION = '0.0.20';

/**
 * Stderr-only diag logger. Avoids contaminating the JSON-RPC stdout channel.
 */
function createStderrDiagLogger(): DiagLogger {
  const write = (level: string, args: unknown[]): void => {
    try {
      const msg = args
        .map(a => (typeof a === 'string' ? a : JSON.stringify(a)))
        .join(' ');
      process.stderr.write(`[otel ${level}] ${msg}\n`);
    } catch {
      // never throw from a logger
    }
  };
  return {
    error: (...a) => write('error', a),
    warn:  (...a) => write('warn',  a),
    info:  (...a) => write('info',  a),
    debug: (...a) => write('debug', a),
    verbose: (...a) => write('verbose', a),
  };
}

function parseDiagLogLevel(level: string | undefined): DiagLogLevel | null {
  if (!level) return null;
  switch (level.toUpperCase()) {
    case 'NONE':    return DiagLogLevel.NONE;
    case 'ERROR':   return DiagLogLevel.ERROR;
    case 'WARN':    return DiagLogLevel.WARN;
    case 'INFO':    return DiagLogLevel.INFO;
    case 'DEBUG':   return DiagLogLevel.DEBUG;
    case 'VERBOSE': return DiagLogLevel.VERBOSE;
    case 'ALL':     return DiagLogLevel.ALL;
    default:        return null;
  }
}

/**
 * Reject =console exporters at startup. They write JSON to stdout and would
 * silently corrupt the MCP JSON-RPC stream.
 */
function rejectConsoleExporters(): void {
  const offenders: string[] = [];
  for (const v of ['OTEL_TRACES_EXPORTER', 'OTEL_METRICS_EXPORTER', 'OTEL_LOGS_EXPORTER']) {
    const val = process.env[v];
    if (val && val.split(',').map(s => s.trim()).includes('console')) {
      offenders.push(`${v}=${val}`);
    }
  }
  if (offenders.length > 0) {
    process.stderr.write(
      `[memory-server] Refusing to start: ${offenders.join(', ')} would write to stdout and corrupt the MCP JSON-RPC stream. Use OTLP exporters instead.\n`
    );
    process.exit(1);
  }
}

const userDisabled = process.env.OTEL_SDK_DISABLED === 'true';
const hasEndpoint =
  !!process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
  !!process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ||
  !!process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT;

const enabled = !userDisabled && hasEndpoint;

let sdkInstance: { shutdown(): Promise<void> } | null = null;

if (enabled) {
  rejectConsoleExporters();

  // Top-level dynamic imports — only paid when enabled. Under ESM Node16,
  // top-level await is supported in modules.
  const { NodeSDK } = await import('@opentelemetry/sdk-node');
  const { resourceFromAttributes } = await import('@opentelemetry/resources');
  const { PeriodicExportingMetricReader } = await import('@opentelemetry/sdk-metrics');
  const { OTLPTraceExporter } = await import('@opentelemetry/exporter-trace-otlp-grpc');
  const { OTLPMetricExporter } = await import('@opentelemetry/exporter-metrics-otlp-grpc');

  const resource = resourceFromAttributes({
    'service.name': SERVICE_NAME,
    'service.version': SERVICE_VERSION,
  });

  const sdk = new NodeSDK({
    resource,
    autoDetectResources: false,
    traceExporter: new OTLPTraceExporter(),
    metricReaders: [
      new PeriodicExportingMetricReader({
        exporter: new OTLPMetricExporter(),
      }),
    ],
    // Empty instrumentations list — no auto-instrumentation. Keep span output
    // limited to the explicit tool-call wrapper in server.ts.
    instrumentations: [],
  });

  sdk.start();
  sdkInstance = sdk;

  // Install stderr-only diag logger AFTER SDK start so we overwrite the
  // default DiagConsoleLogger that NodeSDK installs (which would write to
  // stdout via `console.log` and corrupt the JSON-RPC stream). Pass
  // `suppressOverrideMessage` to silence the API's "logger overwritten" warn.
  const logLevel = parseDiagLogLevel(process.env.OTEL_LOG_LEVEL) ?? DiagLogLevel.WARN;
  // Disable any logger NodeSDK may have installed (writes to stdout).
  diag.disable();
  if (logLevel !== DiagLogLevel.NONE) {
    diag.setLogger(createStderrDiagLogger(), {
      logLevel,
      suppressOverrideMessage: true,
    });
  }

  const flush = async (): Promise<void> => {
    try {
      await sdk.shutdown();
    } catch (err) {
      process.stderr.write(`[otel] shutdown error: ${(err as Error).message}\n`);
    }
  };
  process.once('beforeExit', () => { void flush(); });
  process.once('SIGTERM', () => { void flush(); });
  process.once('SIGINT', () => { void flush(); });
}

/**
 * Tracer for the memory server. Resolves to a real tracer when enabled, no-op
 * otherwise (the API package returns a no-op tracer when no provider is
 * registered).
 */
export const tracer: Tracer = trace.getTracer('memory-server', SERVICE_VERSION);

/**
 * Meter for the memory server. No-op when SDK is disabled.
 */
export const meter: Meter = metrics.getMeter('memory-server', SERVICE_VERSION);

/**
 * RPC server duration histogram. SemConv: rpc.server.duration in seconds.
 * Histograms emit `_count` and `_sum`, so a separate counter is unnecessary.
 * Attribute keys: `rpc.system`, `rpc.method`, optional `error.type`.
 */
export const toolDurationHistogram: Histogram = meter.createHistogram(
  'rpc.server.duration',
  {
    unit: 's',
    description: 'Duration of MCP tool calls (seconds).',
  }
);

/**
 * Whether the OTel SDK is initialized. Useful for tests.
 */
export const otelEnabled = enabled;

/**
 * Explicit shutdown — safe to call multiple times. Tests use this to flush
 * spans/metrics between cases.
 */
export async function shutdown(): Promise<void> {
  if (sdkInstance) {
    await sdkInstance.shutdown();
    sdkInstance = null;
  }
}

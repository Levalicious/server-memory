import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { type Server } from "@modelcontextprotocol/sdk/server/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { type Entity, type Relation, MAX_CHARS } from "../server.js";

export { MAX_CHARS };

export interface PaginatedResult<T> {
  items: T[];
  nextCursor: number | null;
  totalCount: number;
}

export interface PaginatedGraph {
  entities: PaginatedResult<Entity>;
  relations: PaginatedResult<Relation>;
}

/**
 * `find_path` returns a paginated relation list (the path) plus β-contract
 * status fields letting the caller distinguish:
 *   - `targetReached`: did BFS arrive at `toEntity`? When false, `items` is
 *     a best-effort exploration path to `farthestDiscovered`, not to
 *     `toEntity`.
 *   - `budgetExhausted`: did we stop due to the per-call byte budget (vs.
 *     hitting `maxDepth` or exhausting the reachable subgraph)?
 *   - `farthestDiscovered`: the deepest BFS-reached node, present whenever
 *     BFS expanded any edge at all. Anchor for a follow-up retry.
 *   - `note`: natural-language explanation when the target wasn't reached;
 *     intended for the LLM to read directly.
 */
export interface FindPathResult extends PaginatedResult<Relation> {
  targetReached: boolean;
  budgetExhausted: boolean;
  farthestDiscovered?: string;
  note?: string;
}

export async function createTestClient(server: Server): Promise<{
  client: Client;
  cleanup: () => Promise<void>;
}> {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  
  const client = new Client({
    name: "test-client",
    version: "1.0.0",
  }, {
    capabilities: {}
  });

  await server.connect(serverTransport);
  await client.connect(clientTransport);

  return {
    client,
    cleanup: async () => {
      await client.close();
      await server.close();
    }
  };
}

// Read operations that should be paginated
const PAGINATED_TOOLS = new Set([
  'search_nodes',
  'open_nodes',
  'open_nodes_filtered',
  'get_neighbors',
  'find_path',
  'get_entities_by_type',
  'get_orphaned_entities',
]);

/**
 * MAX_CHARS is a SOFT budget per page on the server. The forward-progress
 * invariant in `paginateItems` allows a single oversized lead item to blow
 * past the budget — that's by design (otherwise the cursor would never
 * advance past such an item and the model would loop). So the test helper's
 * cap is a sanity ceiling, not the production budget.
 */
const RESPONSE_SANITY_CAP = MAX_CHARS * 16;

export async function callTool(
  client: Client,
  name: string,
  args: Record<string, unknown>
): Promise<unknown> {
  // Verify tool is discoverable via ListTools before calling it
  const listing = await client.listTools();
  const toolNames = listing.tools.map((t: { name: string }) => t.name);
  if (!toolNames.includes(name)) {
    throw new Error(`Tool "${name}" is not listed in ListTools. Available: ${toolNames.join(', ')}`);
  }

  const result = await client.callTool({ name, arguments: args });

  const content = result.content as Array<{ type: string; text?: string }> | undefined;
  if (!content || content.length === 0) {
    return null;
  }

  const first = content[0];
  if (first.type === 'text' && first.text) {
    // The cap is a sanity ceiling — way larger than the server's per-page
    // budget — to catch a real algorithmic regression (e.g. paginateItems
    // emitting the entire list in one page). It must NOT be MAX_CHARS,
    // because the forward-progress invariant allows single oversized items.
    if (PAGINATED_TOOLS.has(name) && first.text.length > RESPONSE_SANITY_CAP) {
      throw new Error(`Response exceeds sanity cap (${RESPONSE_SANITY_CAP} chars): got ${first.text.length} chars`);
    }
    try {
      return JSON.parse(first.text);
    } catch {
      return first.text;
    }
  }

  return first;
}

/**
 * Like {@link callTool}, but returns the unparsed `CallToolResult` so tests can
 * assert on `isError`, multi-part content, or the literal text body. Use this
 * for any test that needs to verify a tool returned a tool-level error
 * (`isError: true`) — the parsed `callTool` helper drops that flag.
 */
export interface RawToolResult {
  content: Array<{ type: string; text?: string }>;
  isError?: boolean;
}

export async function callToolRaw(
  client: Client,
  name: string,
  args: Record<string, unknown>
): Promise<RawToolResult> {
  const listing = await client.listTools();
  const toolNames = listing.tools.map((t: { name: string }) => t.name);
  if (!toolNames.includes(name)) {
    throw new Error(`Tool "${name}" is not listed in ListTools. Available: ${toolNames.join(', ')}`);
  }

  const result = await client.callTool({ name, arguments: args });
  return {
    content: (result.content as Array<{ type: string; text?: string }>) ?? [],
    isError: (result as { isError?: boolean }).isError,
  };
}

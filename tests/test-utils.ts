import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { Entity, Relation, MAX_CHARS } from "../server.js";

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

export async function callTool(
  client: Client,
  name: string,
  args: Record<string, unknown>
): Promise<unknown> {
  const result = await client.callTool({ name, arguments: args });
  
  const content = result.content as Array<{ type: string; text?: string }> | undefined;
  if (!content || content.length === 0) {
    return null;
  }
  
  const first = content[0];
  if (first.type === 'text' && first.text) {
    // Only enforce char limit on paginated read operations
    if (PAGINATED_TOOLS.has(name) && first.text.length > MAX_CHARS) {
      throw new Error(`Response exceeds ${MAX_CHARS} char limit: got ${first.text.length} chars`);
    }
    try {
      return JSON.parse(first.text);
    } catch {
      return first.text;
    }
  }
  
  return first;
}

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";

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
    try {
      return JSON.parse(first.text);
    } catch {
      return first.text;
    }
  }
  
  return first;
}

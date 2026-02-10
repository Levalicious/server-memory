/**
 * Concurrency tests: two MCP server instances sharing the same binary files.
 *
 * Verifies that flock-based locking + mmap refresh works correctly when
 * one instance writes and the other reads.
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { createServer, Entity, Relation } from "../server.js";
import { createTestClient, callTool, PaginatedResult, PaginatedGraph } from "./test-utils.js";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";

interface Neighbor {
  name: string;
  mtime?: number;
  obsMtime?: number;
}

describe("Concurrency - dual server instances", () => {
  let tmpDir: string;
  let memoryFilePath: string;

  let clientA: Client;
  let clientB: Client;
  let cleanupA: () => Promise<void>;
  let cleanupB: () => Promise<void>;

  beforeEach(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "memory-concurrency-"));
    memoryFilePath = path.join(tmpDir, "memory.json");

    // Create two MCP server instances sharing the same files
    const serverA = createServer(memoryFilePath);
    const serverB = createServer(memoryFilePath);

    const setupA = await createTestClient(serverA);
    const setupB = await createTestClient(serverB);

    clientA = setupA.client;
    clientB = setupB.client;
    cleanupA = setupA.cleanup;
    cleanupB = setupB.cleanup;
  });

  afterEach(async () => {
    await cleanupA();
    await cleanupB();

    // Clean up temp files
    const files = fs.readdirSync(tmpDir);
    for (const f of files) {
      fs.unlinkSync(path.join(tmpDir, f));
    }
    fs.rmdirSync(tmpDir);
  });

  it("instance B sees entities created by instance A", async () => {
    // A creates entities
    await callTool(clientA, "create_entities", {
      entities: [
        { name: "Alpha", entityType: "Letter", observations: ["First letter"] },
        { name: "Beta", entityType: "Letter", observations: ["Second letter"] },
      ],
    });

    // B should see them
    const result = (await callTool(clientB, "search_nodes", {
      query: "Letter",
      sortBy: "name",
    })) as PaginatedGraph;

    const names = result.entities.items.map((e: Entity) => e.name);
    expect(names).toEqual(["Alpha", "Beta"]);
  });

  it("instance B sees relations created by instance A", async () => {
    // A creates entities
    await callTool(clientA, "create_entities", {
      entities: [
        { name: "Node1", entityType: "Test", observations: [] },
        { name: "Node2", entityType: "Test", observations: [] },
      ],
    });

    // A creates relation
    await callTool(clientA, "create_relations", {
      relations: [{ from: "Node1", to: "Node2", relationType: "links_to" }],
    });

    // B reads neighbors
    const result = (await callTool(clientB, "get_neighbors", {
      entityName: "Node1",
      sortBy: "name",
    })) as PaginatedResult<Neighbor>;

    expect(result.items).toHaveLength(1);
    expect(result.items[0].name).toBe("Node2");
  });

  it("instance A sees entities created by instance B", async () => {
    // B creates entities
    await callTool(clientB, "create_entities", {
      entities: [
        { name: "Gamma", entityType: "Letter", observations: ["Third letter"] },
      ],
    });

    // A should see them
    const result = (await callTool(clientA, "open_nodes", {
      names: ["Gamma"],
    })) as PaginatedGraph;

    expect(result.entities.items).toHaveLength(1);
    expect(result.entities.items[0].name).toBe("Gamma");
    expect(result.entities.items[0].observations).toEqual(["Third letter"]);
  });

  it("interleaved writes are visible to both instances", async () => {
    // A creates first entity
    await callTool(clientA, "create_entities", {
      entities: [{ name: "E1", entityType: "Interleaved", observations: [] }],
    });

    // B creates second entity
    await callTool(clientB, "create_entities", {
      entities: [{ name: "E2", entityType: "Interleaved", observations: [] }],
    });

    // A creates third entity
    await callTool(clientA, "create_entities", {
      entities: [{ name: "E3", entityType: "Interleaved", observations: [] }],
    });

    // Both should see all 3
    const resultA = (await callTool(clientA, "search_nodes", {
      query: "Interleaved",
      sortBy: "name",
    })) as PaginatedGraph;

    const resultB = (await callTool(clientB, "search_nodes", {
      query: "Interleaved",
      sortBy: "name",
    })) as PaginatedGraph;

    const namesA = resultA.entities.items.map((e: Entity) => e.name);
    const namesB = resultB.entities.items.map((e: Entity) => e.name);

    expect(namesA).toEqual(["E1", "E2", "E3"]);
    expect(namesB).toEqual(["E1", "E2", "E3"]);
  });

  it("deletions by A are visible to B", async () => {
    // A creates entities
    await callTool(clientA, "create_entities", {
      entities: [
        { name: "Keep", entityType: "Test", observations: [] },
        { name: "Remove", entityType: "Test", observations: [] },
      ],
    });

    // A deletes one
    await callTool(clientA, "delete_entities", {
      entityNames: ["Remove"],
    });

    // B should only see the remaining one
    const result = (await callTool(clientB, "search_nodes", {
      query: "Test",
      sortBy: "name",
    })) as PaginatedGraph;

    const names = result.entities.items.map((e: Entity) => e.name);
    expect(names).toEqual(["Keep"]);
  });

  it("observation changes by A are visible to B", async () => {
    // A creates entity
    await callTool(clientA, "create_entities", {
      entities: [{ name: "Observable", entityType: "Test", observations: ["Initial"] }],
    });

    // A adds observation
    await callTool(clientA, "add_observations", {
      observations: [{ entityName: "Observable", contents: ["Added by A"] }],
    });

    // B reads it
    const result = (await callTool(clientB, "open_nodes", {
      names: ["Observable"],
    })) as PaginatedGraph;

    expect(result.entities.items[0].observations).toContain("Initial");
    expect(result.entities.items[0].observations).toContain("Added by A");
  });

  it("stats are consistent across instances", async () => {
    // A creates entities and relations
    await callTool(clientA, "create_entities", {
      entities: [
        { name: "S1", entityType: "Stats", observations: [] },
        { name: "S2", entityType: "Stats", observations: [] },
      ],
    });
    await callTool(clientA, "create_relations", {
      relations: [{ from: "S1", to: "S2", relationType: "related" }],
    });

    // B checks stats
    const stats = (await callTool(clientB, "get_stats", {})) as {
      entityCount: number;
      relationCount: number;
    };

    expect(stats.entityCount).toBe(2);
    expect(stats.relationCount).toBe(1);
  });

  it("handles many entities created across instances (growth/remap)", async () => {
    // Create enough entities to trigger file growth
    const batchA: Entity[] = [];
    const batchB: Entity[] = [];
    for (let i = 0; i < 50; i++) {
      batchA.push({ name: `A_${i}`, entityType: "Bulk", observations: [`Obs ${i}`] });
      batchB.push({ name: `B_${i}`, entityType: "Bulk", observations: [`Obs ${i}`] });
    }

    // Create in parallel-ish: A first, then B
    await callTool(clientA, "create_entities", { entities: batchA });
    await callTool(clientB, "create_entities", { entities: batchB });

    // Both see all 100
    const statsA = (await callTool(clientA, "get_stats", {})) as { entityCount: number };
    const statsB = (await callTool(clientB, "get_stats", {})) as { entityCount: number };

    expect(statsA.entityCount).toBe(100);
    expect(statsB.entityCount).toBe(100);
  });
});

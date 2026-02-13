# Knowledge Graph Memory Server

A persistent knowledge graph with binary storage, PageRank-based ranking, and Maximum Entropy Random Walk (MERW) exploration. Designed as an MCP server for use with LLM agents.

## Storage Format

The knowledge graph is stored in two binary files using a custom mmap-backed arena allocator:

- **`<base>.graph`** — Entity records (72 bytes each), adjacency blocks, and node log
- **`<base>.strings`** — Interned, refcounted string table

This replaces the original JSONL format. The binary format supports O(1) entity lookup, POSIX file locking for concurrent access, and in-place mutation without rewriting the entire file.

> [!NOTE]
> **Migrating from JSONL:** If you have an existing `.json` knowledge graph, use the migration script:
> ```sh
> npx tsx scripts/migrate-jsonl.ts [path/to/memory.json]
> ```
> The original `.json` file is preserved. See also `scripts/verify-migration.ts` to validate the result. The `MEMORY_FILE_PATH` does not need to change.

> [!NOTE]
> **Automatic v1→v2 migration:** Graph files using the v1 format (64-byte entity records) are automatically migrated to v2 (72-byte records with MERW ψ field) on first open. The old file is preserved as `<name>.graph.v1`.

## Core Concepts

### Entities
Entities are the primary nodes in the knowledge graph. Each entity has:
- A unique name (identifier)
- An entity type (e.g., "person", "organization", "event")
- A list of observations (max 2, each max 140 characters)
- Modification timestamps (`mtime` for any change, `obsMtime` for observation changes)

Example:
```json
{
  "name": "John_Smith",
  "entityType": "person",
  "observations": ["Speaks fluent Spanish"],
  "mtime": 1733423456789,
  "obsMtime": 1733423456789
}
```

### Relations
Relations define directed connections between entities. They are always stored in active voice and describe how entities interact or relate to each other. Each relation has a modification timestamp (`mtime`).

Example:
```json
{
  "from": "John_Smith",
  "to": "Anthropic",
  "relationType": "works_at",
  "mtime": 1733423456789
}
```
### Observations
Observations are discrete pieces of information about an entity. They are:

- Stored as strings (max 140 characters each)
- Attached to specific entities (max 2 per entity)
- Can be added or removed independently
- Should be atomic (one fact per observation)

Example:
```json
{
  "entityName": "John_Smith",
  "observations": [
    "Speaks fluent Spanish",
    "Graduated in 2019"
  ]
}
```

### Ranking

Two ranking systems are maintained and updated after every graph mutation:

- **PageRank (`pagerank`)** — Structural importance via Monte Carlo random walks on graph topology (Avrachenkov et al. Algorithm 4). Each mutation triggers a full sampling pass.
- **LLM Rank (`llmrank`)** — Walker visit counts that track which nodes the LLM actually opens/searches. Primary sort for `llmrank` is walker visits, with PageRank as tiebreaker.

### Maximum Entropy Random Walk (MERW)

The `random_walk` tool uses MERW rather than a standard uniform random walk. MERW maximizes the global entropy rate by sampling uniformly among all paths in the graph, rather than locally maximizing entropy at each vertex.

Transition probabilities are computed from the dominant eigenvector ψ of the (damped) adjacency matrix:

```
S_ij = (A_ij / λ) · (ψ_j / ψ_i)
```

The eigenvector is computed via sparse power iteration with teleportation damping (α=0.85), warm-started from the previously stored ψ values. After a small graph mutation, convergence typically requires only 2–5 iterations rather than a full cold start.

**Practical effect:** Walks gravitate toward structurally rich regions of the graph rather than wandering down linear chains, making serendipitous exploration more productive.

## API

### Tools
- **create_entities**
  - Create multiple new entities in the knowledge graph
  - Input: `entities` (array of objects)
    - Each object contains:
      - `name` (string): Entity identifier
      - `entityType` (string): Type classification
      - `observations` (string[]): Associated observations (max 2, each max 140 chars)
  - Ignores entities with existing names

- **create_relations**
  - Create multiple new relations between entities
  - Input: `relations` (array of objects)
    - Each object contains:
      - `from` (string): Source entity name
      - `to` (string): Target entity name
      - `relationType` (string): Relationship type in active voice
  - Skips duplicate relations

- **add_observations**
  - Add new observations to existing entities
  - Input: `observations` (array of objects)
    - Each object contains:
      - `entityName` (string): Target entity
      - `contents` (string[]): New observations to add (each max 140 chars)
  - Returns added observations per entity
  - Fails if entity doesn't exist or would exceed 2 observations

- **delete_entities**
  - Remove entities and their relations
  - Input: `entityNames` (string[])
  - Cascading deletion of associated relations
  - Silent operation if entity doesn't exist

- **delete_observations**
  - Remove specific observations from entities
  - Input: `deletions` (array of objects)
    - Each object contains:
      - `entityName` (string): Target entity
      - `observations` (string[]): Observations to remove
  - Silent operation if observation doesn't exist

- **delete_relations**
  - Remove specific relations from the graph
  - Input: `relations` (array of objects)
    - Each object contains:
      - `from` (string): Source entity name
      - `to` (string): Target entity name
      - `relationType` (string): Relationship type
  - Silent operation if relation doesn't exist

- **search_nodes**
  - Search for nodes using a regex pattern
  - Input: 
    - `query` (string): Regex pattern to search
    - `sortBy` (string, optional): Sort field (`mtime`, `obsMtime`, `name`, `pagerank`, `llmrank`). Default: `llmrank`
    - `sortDir` (string, optional): Sort direction (`asc` or `desc`)
    - `direction` (string, optional): Edge direction filter (`forward`, `backward`, `any`). Default: `forward`
    - `entityCursor`, `relationCursor` (number, optional): Pagination cursors
  - Searches across entity names, types, and observation content
  - Returns matching entities and their relations (paginated)

- **open_nodes**
  - Retrieve specific nodes by name
  - Input:
    - `names` (string[]): Entity names to retrieve
    - `direction` (string, optional): Edge direction filter (`forward`, `backward`, `any`). Default: `forward`
    - `entityCursor`, `relationCursor` (number, optional): Pagination cursors
  - Returns requested entities and relations originating from them (paginated)
  - Silently skips non-existent nodes

- **get_neighbors**
  - Get names of neighboring entities connected to a specific entity within a given depth
  - Input: 
    - `entityName` (string): The entity to find neighbors for
    - `depth` (number, default: 1): Maximum traversal depth
    - `sortBy` (string, optional): Sort field (`mtime`, `obsMtime`, `name`, `pagerank`, `llmrank`). Default: `llmrank`
    - `sortDir` (string, optional): Sort direction (`asc` or `desc`)
    - `direction` (string, optional): Edge direction filter (`forward`, `backward`, `any`). Default: `forward`
    - `cursor` (number, optional): Pagination cursor
  - Returns neighbor names with timestamps (paginated)
  - Use `open_nodes` to get full entity data for neighbors

- **find_path**
  - Find a path between two entities in the knowledge graph
  - Input:
    - `fromEntity` (string): Starting entity
    - `toEntity` (string): Target entity
    - `maxDepth` (number, default: 5): Maximum search depth
    - `direction` (string, optional): Edge direction filter (`forward`, `backward`, `any`). Default: `forward`
    - `cursor` (number, optional): Pagination cursor
  - Returns path between entities if one exists (paginated)

- **get_entities_by_type**
  - Get all entities of a specific type
  - Input: 
    - `entityType` (string): Type to filter by
    - `sortBy` (string, optional): Sort field (`mtime`, `obsMtime`, `name`, `pagerank`, `llmrank`). Default: `llmrank`
    - `sortDir` (string, optional): Sort direction (`asc` or `desc`)
    - `cursor` (number, optional): Pagination cursor
  - Returns all entities matching the specified type (paginated)

- **get_entity_types**
  - Get all unique entity types in the knowledge graph
  - No input required
  - Returns list of all entity types

- **get_relation_types**
  - Get all unique relation types in the knowledge graph
  - No input required
  - Returns list of all relation types

- **get_stats**
  - Get statistics about the knowledge graph
  - No input required
  - Returns entity count, relation count, entity types count, relation types count

- **get_orphaned_entities**
  - Get entities that have no relations (orphaned entities)
  - Input: 
    - `strict` (boolean, default: false): If true, returns entities not connected to 'Self' entity
    - `sortBy` (string, optional): Sort field (`mtime`, `obsMtime`, `name`, `pagerank`, `llmrank`). Default: `llmrank`
    - `sortDir` (string, optional): Sort direction (`asc` or `desc`)
    - `cursor` (number, optional): Pagination cursor
  - Returns entities with no connections (paginated)

- **validate_graph**
  - Validate the knowledge graph
  - No input required
  - Returns missing entities referenced in relations and observation limit violations

- **decode_timestamp**
  - Decode a millisecond timestamp to human-readable UTC format
  - Input:
    - `timestamp` (number, optional): Millisecond timestamp to decode. If omitted, returns current time
    - `relative` (boolean, optional): If true, include relative time (e.g., "3 days ago")
  - Returns timestamp, ISO 8601 string, formatted UTC string, and optional relative time
  - Useful for interpreting `mtime`/`obsMtime` values from entities

- **random_walk**
  - Perform a MERW-weighted random walk from a starting entity
  - Input:
    - `start` (string): Name of the entity to start the walk from
    - `depth` (number, default: 3): Number of hops to take
    - `seed` (string, optional): Seed for reproducible walks
    - `direction` (string, optional): Edge direction filter (`forward`, `backward`, `any`). Default: `forward`
  - Neighbors are selected proportional to their MERW eigenvector component ψ
  - Falls back to uniform sampling if ψ has not been computed
  - Returns the terminal entity name and the path taken

- **sequentialthinking**
  - Record a thought in the knowledge graph
  - Input: `observations` (string[], max 2, each max 140 chars), `previousCtxId` (string, optional)
  - Creates a Thought entity and links it to the previous thought if provided
  - Returns the new thought's context ID for chaining

- **kb_load**
  - Load a plaintext document into the knowledge graph
  - Input:
    - `filePath` (string): Absolute path to a plaintext file (`.txt`, `.md`, `.tex`, source code, etc.)
    - `title` (string, optional): Document title. Defaults to filename without extension
    - `topK` (number, optional): Number of top TextRank sentences to highlight in the index. Default: 15
  - Creates a doubly-linked chain of TextChunk entities, a Document entity, and a DocumentIndex with TextRank-selected entry points
  - For PDFs, convert to text first (e.g., `pdftotext`)

# Usage with Claude Desktop

### Setup

Add this to your claude_desktop_config.json:

#### Docker

```json
{
  "mcpServers": {
    "memory": {
      "command": "docker",
      "args": ["run", "-i", "-v", "claude-memory:/app/dist", "--rm", "mcp/memory"]
    }
  }
}
```

#### NPX
```json
{
  "mcpServers": {
    "memory": {
      "command": "npx",
      "args": [
        "-y",
        "@levalicious/server-memory"
      ]
    }
  }
}
```

#### NPX with custom setting

The server can be configured using the following environment variables:

```json
{
  "mcpServers": {
    "memory": {
      "command": "npx",
      "args": [
        "-y",
        "@levalicious/server-memory"
      ],
      "env": {
        "MEMORY_FILE_PATH": "/path/to/custom/memory.json"
      }
    }
  }
}
```

- `MEMORY_FILE_PATH`: Path to the memory storage JSON file (default: `memory.json` in the server directory)

# VS Code Installation Instructions

For quick installation, use one of the one-click installation buttons below:

[![Install with NPX in VS Code](https://img.shields.io/badge/VS_Code-NPM-0098FF?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=memory&config=%7B%22command%22%3A%22npx%22%2C%22args%22%3A%5B%22-y%22%2C%22%40levalicious%2Fserver-memory%22%5D%7D) [![Install with NPX in VS Code Insiders](https://img.shields.io/badge/VS_Code_Insiders-NPM-24bfa5?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=memory&config=%7B%22command%22%3A%22npx%22%2C%22args%22%3A%5B%22-y%22%2C%22%40levalicious%2Fserver-memory%22%5D%7D&quality=insiders)

For manual installation, add the following JSON block to your User Settings (JSON) file in VS Code. You can do this by pressing `Ctrl + Shift + P` and typing `Preferences: Open Settings (JSON)`.

Optionally, you can add it to a file called `.vscode/mcp.json` in your workspace. This will allow you to share the configuration with others. 

> Note that the `mcp` key is not needed in the `.vscode/mcp.json` file.

#### NPX

```json
{
  "mcp": {
    "servers": {
      "memory": {
        "command": "npx",
        "args": [
          "-y",
          "@levalicious/server-memory"
        ]
      }
    }
  }
}
```

#### Docker

```json
{
  "mcp": {
    "servers": {
      "memory": {
        "command": "docker",
        "args": [
          "run",
          "-i",
          "-v",
          "claude-memory:/app/dist",
          "--rm",
          "mcp/memory"
        ]
      }
    }
  }
}
```

### System Prompt

The prompt for utilizing memory depends on the use case. Changing the prompt will help the model determine the frequency and types of memories created.

Here is an example prompt for chat personalization. You could use this prompt in the "Custom Instructions" field of a [Claude.ai Project](https://www.anthropic.com/news/projects). 

```
Follow these steps for each interaction:

1. User Identification:
   - You should assume that you are interacting with default_user
   - If you have not identified default_user, proactively try to do so.

2. Memory Retrieval:
   - Always begin your chat by saying only "Remembering..." and retrieve all relevant information from your knowledge graph
   - Always refer to your knowledge graph as your "memory"

3. Memory
   - While conversing with the user, be attentive to any new information that falls into these categories:
     a) Basic Identity (age, gender, location, job title, education level, etc.)
     b) Behaviors (interests, habits, etc.)
     c) Preferences (communication style, preferred language, etc.)
     d) Goals (goals, targets, aspirations, etc.)
     e) Relationships (personal and professional relationships up to 3 degrees of separation)

4. Memory Update:
   - If any new information was gathered during the interaction, update your memory as follows:
     a) Create entities for recurring organizations, people, and significant events
     b) Connect them to the current entities using relations
     c) Store facts about them as observations
```

## Building

Docker:

```sh
docker build -t mcp/memory -f src/memory/Dockerfile . 
```

## License

This MCP server is licensed under the MIT License. This means you are free to use, modify, and distribute the software, subject to the terms and conditions of the MIT License. For more details, please see the LICENSE file in the project repository.
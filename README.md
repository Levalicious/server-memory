# Knowledge Graph Memory Server

A basic implementation of persistent memory using a local knowledge graph. This lets Claude remember information about the user across chats.

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
  - Input: `query` (string), `entityCursor` (number, optional), `relationCursor` (number, optional)
  - Searches across:
    - Entity names
    - Entity types
    - Observation content
  - Returns matching entities and their relations (paginated)

- **open_nodes_filtered**
  - Retrieve specific nodes by name with filtered relations
  - Input: `names` (string[]), `entityCursor` (number, optional), `relationCursor` (number, optional)
  - Returns:
    - Requested entities
    - Only relations where both endpoints are in the requested set
  - Silently skips non-existent nodes (paginated)

- **open_nodes**
  - Retrieve specific nodes by name
  - Input: `names` (string[]), `entityCursor` (number, optional), `relationCursor` (number, optional)
  - Returns:
    - Requested entities
    - Relations originating from requested entities
  - Silently skips non-existent nodes (paginated)

- **get_neighbors**
  - Get neighboring entities connected to a specific entity within a given depth
  - Input: `entityName` (string), `depth` (number, default: 0), `withEntities` (boolean, default: false), `entityCursor` (number, optional), `relationCursor` (number, optional)
  - Returns relations (and optionally entities) connected within specified depth (paginated)

- **find_path**
  - Find a path between two entities in the knowledge graph
  - Input: `fromEntity` (string), `toEntity` (string), `maxDepth` (number, default: 5), `cursor` (number, optional)
  - Returns path between entities if one exists (paginated)

- **get_entities_by_type**
  - Get all entities of a specific type
  - Input: `entityType` (string), `cursor` (number, optional)
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
  - Input: `strict` (boolean, default: false), `cursor` (number, optional)
  - In strict mode, returns entities not connected to 'Self' entity (directly or indirectly)
  - Returns entities with no connections (paginated)

- **validate_graph**
  - Validate the knowledge graph
  - No input required
  - Returns missing entities referenced in relations and observation limit violations

- **evaluate_bcl**
  - Evaluate a Binary Combinatory Logic (BCL) program
  - Input: `program` (string), `maxSteps` (number, default: 1000000)
  - BCL syntax: T:=00|01|1TT where 00=K, 01=S, 1=application
  - Returns evaluation result with halt status

- **add_bcl_term**
  - Add a BCL term to the constructor, maintaining valid syntax
  - Input: `term` (string)
  - Valid values: '1' or 'App' (application), '00' or 'K' (K combinator), '01' or 'S' (S combinator)
  - Returns completion status

- **clear_bcl_term**
  - Clear the current BCL term being constructed and reset the constructor state
  - No input required
  - Resets BCL constructor

- **sequentialthinking**
  - Record a thought in the knowledge graph
  - Input: `observations` (string[], max 2, each max 140 chars), `previousCtxId` (string, optional)
  - Creates a Thought entity and links it to the previous thought if provided
  - Returns the new thought's context ID for chaining

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
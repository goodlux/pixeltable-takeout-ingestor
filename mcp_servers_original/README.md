# MCP Servers (Original from pixeltable-mcp-server)

This directory contains the original MCP servers from the pixeltable-mcp-server repo.

## Usage

Run individual servers:

```bash
# Document server (port 8083)
cd mcp_servers_original/doc-index
uv run python server.py --port 8083

# Audio server (port 8080) - requires OpenAI API key
cd mcp_servers_original/audio-index  
OPENAI_API_KEY=your_key uv run python server.py --port 8080

# Video server (port 8081) - requires OpenAI API key
cd mcp_servers_original/video-index
OPENAI_API_KEY=your_key uv run python server.py --port 8081

# Image server (port 8082) - requires OpenAI API key  
cd mcp_servers_original/image-index
OPENAI_API_KEY=your_key uv run python server.py --port 8082
```

## Claude Desktop Integration

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "pixeltable-documents": {
      "command": "npx",
      "args": ["mcp-remote", "http://localhost:8083/sse"]
    },
    "pixeltable-audio": {
      "command": "npx", 
      "args": ["mcp-remote", "http://localhost:8080/sse"]
    },
    "pixeltable-video": {
      "command": "npx",
      "args": ["mcp-remote", "http://localhost:8081/sse"] 
    },
    "pixeltable-images": {
      "command": "npx",
      "args": ["mcp-remote", "http://localhost:8082/sse"]
    }
  }
}
```

## Note

These are the exact original files from pixeltable-mcp-server. They work as-is and don't need the Docker isolation issues we solved.

# Pixeltable Takeout Ingestor

A focused tool for ingesting Google Takeout and other personal data exports into Pixeltable for multimedia AI processing.

## Quick Start

```bash
# Install with uv
uv pip install -e .

# Copy environment template
cp .env.example .env
# Edit .env with your settings

# Run ingestion
takeout-ingest --help
```

## Architecture

- **Pixeltable**: Core multimedia data storage and processing
- **MCP Server**: Provides Claude integration for querying data
- **Focused Scope**: Just ingestion and retrieval, not conversation management

## Data Storage

Configure `PIXELTABLE_HOME` in `.env` to point to external storage for large datasets.

## Planned Ingestors

- Google Takeout (Photos, Drive, Chat, etc.)
- Claude Conversations (full conversations as records)
- Artifacts (isolated from conversations)
- Email exports
- Document collections

## MCP Integration

Built-in MCP servers for each data type, compatible with Claude Desktop via mcp-remote bridge.

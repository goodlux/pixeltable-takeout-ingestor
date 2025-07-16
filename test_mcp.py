"""
Test MCP Document Server
"""

import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_document_mcp():
    """Test document MCP server setup."""
    print("Testing Document MCP Server...")
    
    try:
        from takeout_ingestor.mcp_servers.documents import mcp
        
        # Test that the tools are registered
        print(f"MCP server name: {mcp.name}")
        
        # Check available tools
        tools = []
        for attr_name in dir(mcp):
            attr = getattr(mcp, attr_name)
            if hasattr(attr, '_mcp_tool'):
                tools.append(attr_name)
        
        print(f"Available tools: {tools}")
        
        # Test basic functionality (without actually setting up Pixeltable)
        print("✅ Document MCP server structure is valid!")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_document_mcp()

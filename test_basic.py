"""
Basic test for Claude Conversation Ingestor
"""

import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_claude_ingestor():
    """Test basic functionality of Claude conversation ingestor."""
    print("Testing Claude Conversation Ingestor...")
    
    try:
        from takeout_ingestor.ingestors.claude_conversations import ClaudeConversationIngestor
        
        # Create sample conversation file
        sample_conversation = """Human: Hello Claude, how are you today?

Assistant: Hello! I'm doing well, thank you for asking. I'm excited to help you with whatever you're working on today. How can I assist you?

Human: I'd like to learn about data ingestion patterns.

Assistant: Great topic! Data ingestion is a crucial part of any data pipeline. There are several common patterns:

1. **Batch Processing**: Processing data in chunks at scheduled intervals
2. **Stream Processing**: Real-time processing of continuous data streams
3. **ETL (Extract, Transform, Load)**: Traditional approach with distinct phases
4. **ELT (Extract, Load, Transform)**: Modern approach where transformation happens in the target system

Would you like me to dive deeper into any of these patterns?"""
        
        # Write to temporary file
        temp_file = project_root / "temp_conversation.txt"
        temp_file.write_text(sample_conversation)
        
        # Test ingestor
        ingestor = ClaudeConversationIngestor()
        
        # Test validation
        is_valid, error = ingestor.validate_source(str(temp_file))
        print(f"Validation result: {is_valid}, Error: {error}")
        
        if is_valid:
            # Test parsing
            records = ingestor.parse_source(str(temp_file))
            print(f"Parsed {len(records)} conversation(s)")
            
            if records:
                # Test transformation
                transformed = ingestor.transform_record(records[0])
                print(f"Transformed record keys: {list(transformed.keys())}")
                print(f"Message count: {transformed['message_count']}")
                print(f"Conversation ID: {transformed['conversation_id']}")
        
        # Cleanup
        temp_file.unlink(missing_ok=True)
        print("✅ Basic test completed successfully!")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_claude_ingestor()
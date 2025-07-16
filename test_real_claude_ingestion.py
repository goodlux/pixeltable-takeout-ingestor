"""
Test Claude Conversation Ingestor with REAL conversations.json
"""

import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def test_real_claude_ingestion():
    """Test Claude conversation ingestion with real conversations.json."""
    print("🔄 Testing Claude Conversation Ingestor with REAL data...")
    
    conversations_file = "/Users/rob/repos/anthropic/data-2025-05-21-17-32-01/conversations.json"
    
    if not Path(conversations_file).exists():
        print(f"❌ File not found: {conversations_file}")
        return
    
    try:
        from takeout_ingestor.ingestors.claude_conversations import ClaudeConversationIngestor
        
        # Test ingestor with actual Pixeltable storage
        print("📝 Creating ingestor...")
        ingestor = ClaudeConversationIngestor(
            batch_size=10,
            pixeltable_home=os.getenv('PIXELTABLE_HOME')
        )
        
        # First, validate the file
        print("🔍 Validating conversations.json...")
        is_valid, error = ingestor.validate_source(conversations_file)
        print(f"   Validation result: {is_valid}")
        if error:
            print(f"   Error: {error}")
            return
        
        # Parse to see how many conversations we have
        print("📖 Parsing conversations...")
        records = ingestor.parse_source(conversations_file)
        print(f"   Found {len(records)} conversations")
        
        if len(records) > 0:
            # Show sample of first conversation
            first_conv = records[0]
            print(f"   Sample conversation:")
            print(f"     Format: {first_conv.get('format')}")
            print(f"     Messages: {first_conv.get('message_count', 0)}")
            print(f"     Source: {Path(first_conv.get('source_file', '')).name}")
        
        # Ask user if they want to proceed with full ingestion
        if len(records) > 5:
            proceed = input(f"\n🤔 Found {len(records)} conversations. Proceed with full ingestion? (y/N): ")
            if proceed.lower() != 'y':
                print("👍 Skipping full ingestion. Run again with 'y' to proceed.")
                return
        
        # Run full ingestion (this will actually store in Pixeltable)
        print("\n💾 Running full ingestion...")
        result = ingestor.ingest(conversations_file)
        
        print("\n📊 Ingestion Results:")
        print(f"  Total records: {result['total_records']}")
        print(f"  Processed: {result['processed_records']}")
        print(f"  Failed: {result['failed_records']}")
        print(f"  Success rate: {result['success_rate']:.1f}%")
        print(f"  Table: {result['table_name']}")
        
        print("\n✅ Real Claude conversations successfully ingested into Pixeltable!")
        print("\n🔍 You can now query this data via the MCP server!")
        print("Try asking: 'What conversations do I have about [topic]?'")
        
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_real_claude_ingestion()

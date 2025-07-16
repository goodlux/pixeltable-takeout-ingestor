"""
Test Claude Conversation Ingestor with actual Pixeltable storage
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

def test_full_claude_ingestion():
    """Test full Claude conversation ingestion into Pixeltable."""
    print("🔄 Testing Claude Conversation Ingestor with Pixeltable...")
    
    try:
        from takeout_ingestor.ingestors.claude_conversations import ClaudeConversationIngestor
        
        # Create a more interesting sample conversation
        sample_conversation = """Human: I'm working on a data ingestion project using Pixeltable. Can you help me understand the best practices?

Assistant: Absolutely! Here are some key best practices for data ingestion with Pixeltable:

1. **Start with clean schema design** - Define your tables with appropriate data types
2. **Use batch processing** - Process data in manageable chunks for better performance
3. **Leverage embeddings** - Add semantic search capabilities to your data
4. **Monitor progress** - Use logging and progress tracking for large ingestion jobs

What type of data are you planning to ingest?

Human: I'm focusing on Google Takeout data - photos, documents, chat history. The volume could be quite large.

Assistant: Great choice! Google Takeout can contain massive amounts of valuable personal data. Here's my recommended approach:

**For Photos:**
- Use Pixeltable's image processing capabilities
- Extract metadata (EXIF data, timestamps, locations)
- Generate image descriptions using vision models
- Create embedding indexes for semantic photo search

**For Documents:**
- Parse different formats (PDFs, docs, spreadsheets)
- Chunk large documents for better retrieval
- Use document splitters with appropriate token limits
- Index both content and metadata

**For Chat History:**
- Store conversations as structured data
- Extract participants, timestamps, attachments
- Enable search across conversation content
- Consider privacy implications and access controls

**Performance Tips:**
- Process in batches (100-1000 items per batch)
- Use external storage for large files
- Monitor memory usage during processing
- Implement resume functionality for long-running jobs

Would you like me to dive deeper into any of these areas?"""
        
        # Write to temporary file
        temp_file = project_root / "temp_claude_conversation.txt"
        temp_file.write_text(sample_conversation)
        
        # Test ingestor with actual Pixeltable storage
        print("📝 Creating ingestor...")
        ingestor = ClaudeConversationIngestor(
            batch_size=10,
            pixeltable_home=os.getenv('PIXELTABLE_HOME')
        )
        
        # Run full ingestion (this will actually store in Pixeltable)
        print("💾 Running ingestion...")
        result = ingestor.ingest(str(temp_file))
        
        print("📊 Ingestion Results:")
        print(f"  Total records: {result['total_records']}")
        print(f"  Processed: {result['processed_records']}")
        print(f"  Failed: {result['failed_records']}")
        print(f"  Success rate: {result['success_rate']:.1f}%")
        print(f"  Table: {result['table_name']}")
        
        # Cleanup
        temp_file.unlink(missing_ok=True)
        
        print("✅ Claude conversation successfully ingested into Pixeltable!")
        print("\n🔍 You can now query this data via the MCP server!")
        print("Try asking: 'What conversations do I have about data ingestion?'")
        
    except Exception as e:
        print(f"❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_full_claude_ingestion()
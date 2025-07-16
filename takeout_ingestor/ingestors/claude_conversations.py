"""
Claude Conversation Ingestor - Redesigned for Pixeltable Document Pipeline

Ingests Claude conversations as text documents into Pixeltable's document search.
Uses unified document table for consistent search experience.
"""

import json
import re
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime
import tempfile
import os

import pixeltable as pxt
from pixeltable.iterators import DocumentSplitter
from pixeltable.functions.huggingface import sentence_transformer

from .base import BaseIngestor


class ClaudeConversationIngestor(BaseIngestor):
    """
    Ingest Claude conversations into Pixeltable's document search pipeline.
    
    Converts conversations to text documents and uses Pixeltable's built-in
    document processing, chunking, and embedding capabilities.
    """
    
    def get_supported_formats(self) -> List[str]:
        """Return supported file formats."""
        return [".txt", ".json", ".md"]
    
    def get_table_name(self) -> str:
        """Return the Pixeltable table name."""
        return "doc_search.all_documents"
    
    def validate_source(self, source_path: str) -> Tuple[bool, Optional[str]]:
        """Validate Claude conversation source."""
        path = Path(source_path)
        
        if not path.exists():
            return False, f"Source path does not exist: {source_path}"
        
        if path.suffix.lower() not in self.get_supported_formats():
            return False, f"Unsupported file format: {path.suffix}"
        
        # Basic content validation
        try:
            content = path.read_text(encoding='utf-8')
            if len(content.strip()) == 0:
                return False, "File is empty"
            
            # Check if it's JSON with conversation structure
            if path.suffix.lower() == '.json':
                try:
                    data = json.loads(content)
                    if isinstance(data, (dict, list)):
                        return True, None
                except json.JSONDecodeError:
                    pass
            
            # Look for conversation markers in text files
            if any(marker in content.lower() for marker in ["human:", "assistant:", "claude:", "user:"]):
                return True, None
            
            # Accept any text file for now
            return True, None
            
        except Exception as e:
            return False, f"Error reading file: {e}"
    
    def parse_source(self, source_path: str) -> List[Dict[str, Any]]:
        """Parse Claude conversations and convert to document records."""
        path = Path(source_path)
        content = path.read_text(encoding='utf-8')
        
        if path.suffix.lower() == '.json':
            return self._parse_json_conversations(content, source_path)
        else:
            return self._parse_text_conversations(content, source_path)
    
    def _parse_json_conversations(self, content: str, source_path: str) -> List[Dict[str, Any]]:
        """Parse JSON format conversations."""
        try:
            data = json.loads(content)
            conversations = []
            
            if isinstance(data, list):
                # Multiple conversations
                for i, conv in enumerate(data):
                    conversations.append(self._extract_conversation_as_document(conv, source_path, i))
            elif isinstance(data, dict):
                # Single conversation
                conversations.append(self._extract_conversation_as_document(data, source_path, 0))
            
            return conversations
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {source_path}: {e}")
            return []
    
    def _parse_text_conversations(self, content: str, source_path: str) -> List[Dict[str, Any]]:
        """Parse text format conversations."""
        # Treat entire file as one conversation document
        conversation_data = {
            'raw_content': content,
            'source_file': source_path,
            'format': 'text',
            'parsed_at': datetime.now().isoformat(),
            'conversation_index': 0
        }
        
        return [self._extract_conversation_as_document(conversation_data, source_path, 0)]
    
    def _extract_conversation_as_document(self, conv_data: Dict, source_path: str, index: int) -> Dict[str, Any]:
        """Extract conversation and prepare as a document record."""
        # Create document text from conversation
        if 'messages' in conv_data:
            # JSON format with messages
            doc_text = self._format_messages_as_text(conv_data['messages'])
            title = conv_data.get('title', f'Claude Conversation {index + 1}')
            created_at = conv_data.get('created_at')
            updated_at = conv_data.get('updated_at')
        else:
            # Raw text or other format
            doc_text = conv_data.get('raw_content', str(conv_data))
            title = f'Claude Conversation {index + 1}'
            created_at = None
            updated_at = None
        
        # Generate metadata
        metadata = {
            'source_type': 'claude_conversation',
            'source_file': source_path,
            'title': title,
            'conversation_index': index,
            'created_at': created_at,
            'updated_at': updated_at,
            'parsed_at': datetime.now().isoformat(),
            'format': conv_data.get('format', 'json')
        }
        
        return {
            'document_text': doc_text,
            'metadata': metadata,
            'title': title
        }
    
    def _format_messages_as_text(self, messages: List[Dict]) -> str:
        """Format conversation messages as readable text document."""
        text_parts = []
        
        for msg in messages:
            role = msg.get('role', 'unknown').title()
            content = msg.get('content', '')
            
            # Normalize role names
            if role.lower() in ['human', 'user']:
                role = 'Human'
            elif role.lower() in ['assistant', 'claude']:
                role = 'Assistant'
            
            text_parts.append(f"{role}: {content}")
        
        return "\n\n".join(text_parts)
    
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform conversation record for Pixeltable document storage."""
        # Create a temporary text file for Pixeltable to process
        doc_text = record['document_text']
        metadata = record['metadata']
        title = record['title']
        
        # Create temporary file
        temp_file = self._create_temp_document(doc_text, title, metadata)
        
        # Return record formatted for Pixeltable document table
        transformed = {
            'pdf_file': temp_file,  # Pixeltable will process this text file
            'document_metadata': json.dumps(metadata),  # Store our metadata
            'ingested_at': datetime.now().isoformat()
        }
        
        # Store temp file path for cleanup later
        transformed['_temp_file_path'] = temp_file
        
        return transformed
    
    def _create_temp_document(self, text: str, title: str, metadata: Dict) -> str:
        """Create a temporary text file for Pixeltable to process."""
        # Create temp file with meaningful name
        safe_title = re.sub(r'[^\w\-_\.]', '_', title)[:50]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        temp_dir = Path(tempfile.gettempdir()) / "takeout_ingestor_docs"
        temp_dir.mkdir(exist_ok=True)
        
        temp_file = temp_dir / f"claude_conv_{timestamp}_{safe_title}.txt"
        
        # Write document with metadata header
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(f"Title: {title}\n")
            f.write(f"Source: {metadata.get('source_type', 'unknown')}\n")
            if metadata.get('created_at'):
                f.write(f"Created: {metadata['created_at']}\n")
            f.write(f"Parsed: {metadata['parsed_at']}\n")
            f.write("\n" + "="*50 + "\n\n")
            f.write(text)
        
        return str(temp_file)
    
    def _ensure_pixeltable_schema(self):
        """Ensure the unified documents table exists in Pixeltable."""
        try:
            # Create directory if not exists
            pxt.create_dir('doc_search', if_exists='ignore')
            
            # Define unified document table schema
            schema = {
                'pdf_file': pxt.Document,  # Pixeltable will process any document type
                'document_metadata': pxt.String,  # JSON metadata about source
                'ingested_at': pxt.String  # When we ingested it
            }
            
            # Create table
            table = pxt.create_table(
                'doc_search.all_documents',
                schema,
                if_exists='ignore'
            )
            
            # Create view for document chunks (Pixeltable's document processing)
            chunks_view_name = 'doc_search.all_documents_chunks'
            try:
                chunks_view = pxt.create_view(
                    chunks_view_name,
                    table,
                    iterator=DocumentSplitter.create(
                        document=table.pdf_file,
                        separators='token_limit',
                        limit=300  # Tokens per chunk
                    ),
                    if_exists='ignore'
                )
                
                # Add embedding index for semantic search
                embed_model = sentence_transformer.using(model_id='intfloat/e5-large-v2')
                chunks_view.add_embedding_index(
                    column='text',
                    string_embed=embed_model,
                    if_exists='ignore'
                )
                
                self.logger.info("Unified documents table schema ready")
                
            except Exception as e:
                self.logger.warning(f"Chunks view might already exist: {e}")
                
        except Exception as e:
            self.logger.error(f"Error setting up Pixeltable schema: {e}")
            raise
    
    def _store_pixeltable(self, record: Dict[str, Any]):
        """Store document record in Pixeltable."""
        try:
            table = self.pixeltable_client.get_table('doc_search.all_documents')
            
            # Remove temp file path before storing
            store_record = {k: v for k, v in record.items() if not k.startswith('_')}
            table.insert([store_record])
            
            # Clean up temp file after successful storage
            temp_file = record.get('_temp_file_path')
            if temp_file and Path(temp_file).exists():
                try:
                    Path(temp_file).unlink()
                except Exception as e:
                    self.logger.warning(f"Could not clean up temp file {temp_file}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error storing record in Pixeltable: {e}")
            raise
    
    def cleanup(self):
        """Clean up any remaining temporary files."""
        super().cleanup()
        
        # Clean up temp directory
        temp_dir = Path(tempfile.gettempdir()) / "takeout_ingestor_docs"
        if temp_dir.exists():
            try:
                import shutil
                shutil.rmtree(temp_dir)
                self.logger.info("Cleaned up temporary document files")
            except Exception as e:
                self.logger.warning(f"Could not clean up temp directory: {e}")

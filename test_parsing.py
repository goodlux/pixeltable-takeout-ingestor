"""
Basic test for Claude Conversation Ingestor (without Pixeltable)
"""

import sys
import os
from pathlib import Path
import json
import re
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_claude_parsing():
    """Test Claude conversation parsing logic without Pixeltable."""
    print("Testing Claude Conversation Parsing Logic...")
    
    try:
        # Create sample conversation file
        sample_conversation = """Human: Hello Claude, how are you today?
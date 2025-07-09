import sqlite3
import json
import time
from datetime import datetime
from typing import List, Dict, Optional
import logging

from bs4 import BeautifulSoup

from services.client import SejmAPIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Collector:

    def __init__(self):
        self.client = SejmAPIClient()

    def extract_statement_preview(self, html_content: str, max_length: int = 200) -> str:
        """Extract a preview of statement content from HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract statement content (paragraphs without special classes)
        content_parts = []
        for p in soup.find_all('p'):
            if not p.get('class') and p.text.strip():
                content_parts.append(p.text.strip())
        
        full_content = ' '.join(content_parts)
        
        # Return preview
        if len(full_content) > max_length:
            return full_content[:max_length] + "..."
        return full_content

    def extract_statement_content(self, html_content: str) -> str:
        """Extract statement content from HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract statement content (paragraphs without special classes)
        content_parts = []
        for p in soup.find_all('p'):
            if not p.get('class') and p.text.strip():
                content_parts.append(p.text.strip())
        
        full_content = ' '.join(content_parts)

        return full_content
        
    def print_statements(self, term: int = 10, limit_proceedings: Optional[int] = None, 
                    limit_statements_per_day: Optional[int] = None,
                    show_content_preview: bool = False):
        """
        Fetch and print statements from the Sejm API
        
        Args:
            term: Parliamentary term number
            limit_proceedings: Limit number of proceedings to process
            limit_statements_per_day: Limit statements per day to display
            show_content_preview: Whether to fetch and show statement content preview
        """
        
        print(f"Fetching proceedings for term {term}...")
        proceedings = self.client.get_proceedings(term)
        print(f"Found {len(proceedings)} proceedings\n")
        
        total_statements = 0
        
        for i, proceeding in enumerate(proceedings):
            if limit_proceedings and i >= limit_proceedings:
                print(f"\nReached proceeding limit of {limit_proceedings}")
                break
            
            proceeding_num = proceeding['number']
            print("=" * 80)
            print(f"proceeding {proceeding_num}: {proceeding['title']}")
            print(f"Dates: {', '.join(proceeding['dates'])}")
            print("=" * 80)
            
            for date in proceeding['dates']:
                print(f"\n  Date: {date}")
                print("  " + "-" * 76)
                
                try:
                    # Get list of statements
                    statements_data = self.client.get_statement_list(term, proceeding_num, date)
                    statements = statements_data.get('statements', [])
                    
                    print(f"  Found {len(statements)} statements")
                    
                    statements_shown = 0
                    for statement in statements:
                        # Skip procedural entries (num=0) unless they have a specific speaker
                        if statement['num'] == 0 and not statement.get('memberID'):
                            continue
                        
                        if limit_statements_per_day and statements_shown >= limit_statements_per_day:
                            remaining = len(statements) - statements_shown
                            if remaining > 0:
                                print(f"\n  ... and {remaining} more statements")
                            break
                        
                        # Print statement info
                        print(f"\n  statement #{statement['num']}:")
                        print(f"    Speaker: {statement['name']}")
                        
                        if statement.get('function'):
                            print(f"    Function: {statement['function']}")
                        
                        if statement.get('memberID', 0) > 0:
                            print(f"    MP ID: {statement['memberID']}")
                        
                        if statement.get('startDateTime'):
                            start = statement['startDateTime'].split('T')[1][:5]
                            end = statement.get('endDateTime', '').split('T')[1][:5] if statement.get('endDateTime') else '??:??'
                            print(f"    Time: {start} - {end}")
                        
                        if statement.get('unspoken'):
                            print(f"    [UNSPOKEN - submitted in writing]")
                        
                        # Fetch and show content preview if requested
                        if show_content_preview and statement['num'] > 0:
                            try:
                                content = self.client.get_statement_transcript(term, proceeding_num, date, statement['num'])
                                preview = self.extract_statement_preview(content)
                                if preview:
                                    print(f"    Preview: {preview}")
                                time.sleep(0.2)  # Be nice to the API
                            except Exception as e:
                                print(f"    [Error fetching content: {e}]")
                        
                        statements_shown += 1
                        total_statements += 1
                    
                except Exception as e:
                    print(f"  Error processing date {date}: {e}")
                    continue
        
        print(f"\n{'=' * 80}")
        print(f"SUMMARY: Total statements displayed: {total_statements}")
        print(f"{'=' * 80}")
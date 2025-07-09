import sqlite3
import json
import time
from datetime import datetime
from typing import List, Dict, Optional
import logging

from bs4 import BeautifulSoup

from services.client import SejmAPIClient
from services.collector import Collector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
    

def main():
    """Main function"""

    client = SejmAPIClient()
    collector = Collector()

    print("SEJM STATEMENTS PRINTER")
    print("=" * 80)
    
    # Configuration
    TERM = 10  # Current parliamentary term
    LIMIT_PROCEEDINGS = 3  # Only show first 3 proceedings
    LIMIT_STATEMENTS_PER_DAY = 10  # Only show first 10 statements per day
    SHOW_CONTENT_PREVIEW = True  # Set to True to fetch and show statement previews
    DATE = '2023-11-13'
    PROCEEDING = 1
    STATEMENT_NO = 1

    # # Print specific statement
    statement = client.get_statement_transcript(TERM, PROCEEDING, DATE, STATEMENT_NO)
    
    print(collector.extract_statement_content(statement))
    
    # # Print MPs
    mps = client.get_mps()
    # print(json.dumps(mps, indent=2, ensure_ascii=False))

    # Print statements
    # print_statements(
    #     term=TERM,
    #     limit_proceedings=LIMIT_PROCEEDINGS,
    #     limit_statements_per_day=LIMIT_STATEMENTS_PER_DAY,
    #     show_content_preview=SHOW_CONTENT_PREVIEW
    # )
    
    # Optional: Show all statements without limits (but without content)
    # print("\n\nShowing all statements (metadata only):")
    # collector.print_statements(term=TERM, show_content_preview=False)


if __name__ == "__main__":
    main()
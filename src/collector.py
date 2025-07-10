import json
import time
from typing import Dict, Optional, Any
import logging

from bs4 import BeautifulSoup

from src.config import Config
from src.models import MP, Statement
from src.client import SejmAPIClient
from src.storage import StorageBackend, JSONStorage, CSVStorage

logger = logging.getLogger(__name__)


class DataCollector:

    def __init__(self, config: Config, storage: StorageBackend):
        self.config = config
        self.client = SejmAPIClient()
        self.storage = storage
        self.processed_statements = self.storage.get_processed_statements()
        self.new_statement_entries = 0

    def collect_mps(self, term: Optional[int] = None) -> None:
        """Collect and save all MPs data"""
        term = term or self.config.parliamentary_term
        logger.info(f"Collecting MPs for term {term}")

        try:
            mps_data = self.client.get_mps(term)
            logger.info(f"Found {len(mps_data)} MPs")

            for mp_data in mps_data:
                mp = MP.from_api_response(mp_data, term)
                self.storage.save_mp(mp)
                # print(mp)

            # Flush remaining statements after completion
            self.storage.flush_all()

            logger.info("MPs collection completed")

        except Exception as e:
            logger.error(f"Error collecting MPs: {e}")
            raise

    def collect_statements(self, term: Optional[int] = None,
                           limit_proceedings: Optional[int] = None, update_existing: Optional[bool] = False) -> None:
        """Collect statements data"""
        term = term or self.config.parliamentary_term
        logger.info(f"Collecting statements for term {term}")

        try:
            proceedings = self.client.get_proceedings(term)
            logger.info(f"Found {len(proceedings)} sittings")

            for i, proceeding in enumerate(proceedings):
                if limit_proceedings and i >= limit_proceedings:
                    logger.info(f"Reached sitting limit: {limit_proceedings}")
                    break

                self._process_proceeding(term, proceeding, update_existing)

            # Flush remaining statements after completion
            self.storage.flush_all()

            stats = self.storage.get_statistics()
            logger.info("Statements collection completed")
            logger.info(
                f"{self.new_statement_entries} new statement entries created in this run.")
            logger.info(
                f"Current data stats: {json.dumps(stats, indent=2, ensure_ascii=False)}")

            self.new_statement_entries = 0

        except Exception as e:
            logger.error(f"Error collecting speeches: {e}")
            raise

    def _process_proceeding(self, term: int, proceeding: Dict[str, Any], update_existing: Optional[bool] = False) -> None:
        """Process a single proceeding"""
        proceeding_num = proceeding['number']
        logger.info(
            f"Processing proceeding {proceeding_num}: {proceeding['title']}")

        for date in proceeding['dates']:
            self._process_proceeding_date(
                term, proceeding_num, date, update_existing)

    def _process_proceeding_date(self, term: int, proceeding_num: int, date: str, update_existing: Optional[bool] = False):
        """Process statements for a specific proceeding date"""
        logger.info(
            f"Processing date: {date} of proceeding number {proceeding_num}")

        try:
            statements_data = self.client.get_statement_list(
                term, proceeding_num, date)
            statements = statements_data.get('statements', [])

            new_statements = 0
            for statement in statements:
                if self._should_process_statement(statement, proceeding_num, date, update_existing):
                    if self._process_statement(term, proceeding_num, date, statement):
                        new_statements += 1

                    # Respect API rate limits
                    time.sleep(self.config.api_delay_between_requests)

            logger.info(f"    Processed {new_statements} statements")

        except Exception as e:
            logger.error(f"    Error processing date {date}: {e}")

    def _should_process_statement(self, statement: Dict[str, Any], proceeding_num: int, date: str, update_existing: Optional[bool] = False) -> bool:
        """Check if statement should be processed"""
        # Skip procedural entries without speakers
        if statement['num'] == 0 and not statement.get('memberID'):
            return False

        if not update_existing:
            # Skip already processed
            unique_id = self._make_unique_id(
                term=self.config.parliamentary_term,
                proceeding_num=proceeding_num,
                proceeding_date=date,
                statement_num=statement['num']
            )

            if unique_id in self.processed_statements:
                logger.info(
                    f"Statement with id: {unique_id} is already processed. Skipping...")
                return False

        return True

    def _make_unique_id(self, term, proceeding_num, proceeding_date, statement_num) -> str:
        """Generate unique identifier for the speech"""
        return f"{term}_{proceeding_num}_{proceeding_date}_{statement_num}"

    def _process_statement(self, term: int, proceeding_num: int,
                           date: str, statement: Dict[str, Any]) -> bool:
        """Process a single statement"""
        try:
            # Fetch content
            content_html = self.client.get_statement_transcript(
                term, proceeding_num, date, statement['num']
            )

            # Extract text content
            content_text = self._extract_text_content(content_html)

            # Create statement object
            statement_obj = Statement(
                term=term,
                proceeding_num=proceeding_num,
                proceeding_date=date,
                statement_num=statement['num'],
                speaker_name=statement['name'],
                speaker_mp_id=statement.get('memberID') if statement.get(
                    'memberID', 0) > 0 else None,
                speaker_function=statement.get('function'),
                start_time=statement.get('startDateTime'),
                end_time=statement.get('endDateTime'),
                content_text=content_text,
                content_html=content_html,
                is_unspoken=statement.get('unspoken', False)
            )

            # Save to storage
            self.new_statement_entries += 1
            self.storage.save_statement(statement_obj)
            self.processed_statements.add(statement_obj.unique_id)

            return True

        except Exception as e:
            logger.error(
                f"      Error processing statement {statement['num']}: {e}")
            return False

    def _extract_text_content(self, html_content: str) -> str:
        """Extract plain text from HTML content"""
        soup = BeautifulSoup(html_content, 'html.parser')

        content_parts = []
        for p in soup.find_all('p'):
            if not p.get('class') and p.text.strip():
                content_parts.append(p.text.strip())

        return '\n'.join(content_parts)

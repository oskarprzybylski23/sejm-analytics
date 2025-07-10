"""Storage layer for the Sejm data pipeline"""
import json
import csv
from pathlib import Path
from typing import List, Optional, Dict, Any
from abc import ABC, abstractmethod
from datetime import datetime
import logging

import pandas as pd

from src.models import MP, Statement
from src.config import Config

logger = logging.getLogger(__name__)


class StorageBackend(ABC):
    """Abstract base class for storage backends"""

    @abstractmethod
    def save_mp(self, mp: MP) -> None:
        """Save MP data"""
        pass

    @abstractmethod
    def save_statement(self, statementsStatement) -> None:
        """Save statement data"""
        pass

    @abstractmethod
    def get_processed_statements(self) -> set:
        """Get set of already processed statements IDs"""
        pass


class CSVStorage(StorageBackend):
    """CSV file storage backend"""

    def __init__(self, data_dir: Path, batch_size: int):
        self.data_dir = data_dir
        self.mps_file = data_dir / "mps.csv"
        self.statements_file = data_dir / "statements.csv"
        self.processed_file = data_dir / "processed_statements.txt"
        self.batch_size = batch_size

        # Load existing data into memory for better performance
        self.mps_df = self._load_mps_df()
        self.statements_df = self._load_statements_df()

        # Batch processing placeholders
        self.pending_mps = []
        self.pending_statements = []

    def _load_mps_df(self) -> pd.DataFrame:
        """Load existing MPs data or create empty DataFrame"""
        if self.mps_file.exists():
            return pd.read_csv(self.mps_file, encoding='utf-8')
        else:
            return pd.DataFrame(columns=[
                'id', 'term', 'first_name', 'last_name', 'club',
                'district_name', 'district_num', 'voivodeship',
                'profession', 'education_level', 'email', 'photo_url', 'active'
            ])

    def _load_statements_df(self) -> pd.DataFrame:
        """Load existing speeches data or create empty DataFrame"""
        if self.statements_file.exists():

            dtypes = {
                'unique_id': str,
                'term': 'int16',
                'proceeding_num': 'int16',
                'proceeding_date': str,
                'statement_num': 'int32',
                'speaker_name': str,
                'speaker_mp_id': 'Int32',
                'speaker_function': str,
                'start_time': str,
                'end_time': str,
                'content_text': str,
                'is_unspoken': bool,
                'collected_at': str
            }
            return pd.read_csv(self.statements_file, encoding='utf-8', dtype=dtypes)
        else:
            return pd.DataFrame(columns=[
                'unique_id',
                'term',
                'proceeding_num',
                'proceeding_date',
                'statement_num',
                'speaker_name',
                'speaker_mp_id',
                'speaker_function',
                'start_time',
                'end_time',
                'content_text',
                'is_unspoken',
                'collected_at'
            ])

    def save_mp(self, mp: MP) -> None:
        """Add MP to pending batch"""
        self.pending_mps.append({
            'id': mp.id,
            'term': mp.term,
            'first_name': mp.first_name,
            'last_name': mp.last_name,
            'club': mp.club,
            'district_name': mp.district_name,
            'district_num': mp.district_num,
            'voivodeship': mp.voivodeship,
            'profession': mp.profession,
            'education_level': mp.education_level,
            'email': mp.email,
            'photo_url': mp.photo_url,
            'active': mp.active
        })

        # Flush if batch is full
        if len(self.pending_mps) >= self.batch_size:
            self.flush_mps()

    def save_statement(self, statement: Statement) -> None:
        """Add speech to pending batch"""
        # Skip if already processed
        if statement.unique_id in self.get_processed_statements():
            logger.debug(
                f"Statement with id: {statement.unique_id} already exists. Skipping saving...")
            return

        logger.debug(
            f"Adding statement with id: {statement.unique_id} to batch")

        self.pending_statements.append({
            'unique_id': statement.unique_id,
            'term': statement.term,
            'proceeding_num': statement.proceeding_num,
            'proceeding_date': statement.proceeding_date,
            'statement_num': statement.statement_num,
            'speaker_mp_id': statement.speaker_mp_id,
            'speaker_name': statement.speaker_name,
            'speaker_function': statement.speaker_function,
            'start_time': statement.start_time,
            'end_time': statement.end_time,
            'content_text': statement.content_text,
            'is_unspoken': statement.is_unspoken,
            'collected_at': datetime.now().isoformat()
        })

        # Flush if batch is full
        if len(self.pending_statements) >= self.batch_size:
            self.flush_statements()

    def flush_mps(self):
        """Write pending MPs to CSV"""
        if not self.pending_mps:
            return

        new_mps_df = pd.DataFrame(self.pending_mps)

        # Remove duplicates based on id and term
        existing_keys = set(zip(self.mps_df['id'], self.mps_df['term']))
        new_mps_df = new_mps_df[
            ~new_mps_df.apply(lambda x: (
                x['id'], x['term']) in existing_keys, axis=1)
        ]

        if not new_mps_df.empty:
            # Append to in-memory DataFrame
            self.mps_df = pd.concat(
                [self.mps_df, new_mps_df], ignore_index=True)

            # Save to CSV
            self.mps_df.to_csv(self.mps_file, index=False, encoding='utf-8')
            logger.info(f"Saved {len(new_mps_df)} new MPs to CSV")

        logger.info(f"MPs already exist in the CSV. Skipping.")

        self.pending_mps = []

    def flush_statements(self):
        """Write pending statements to CSV"""
        if not self.pending_statements:
            return

        new_statements_df = pd.DataFrame(self.pending_statements)

        # Append to in-memory DataFrame
        self.statements_df = pd.concat(
            [self.statements_df, new_statements_df], ignore_index=True)

        # Save to CSV
        if self.statements_file.exists():
            new_statements_df.to_csv(
                self.statements_file,
                mode='a',
                header=False,
                index=False,
                encoding='utf-8'
            )
        else:
            new_statements_df.to_csv(
                self.statements_file,
                index=False,
                encoding='utf-8'
            )

        logger.info(f"Saved {len(new_statements_df)} new statements to CSV")
        self.pending_statements = []

    def get_processed_statements(self) -> set:
        """Get set of processed speech IDs from in-memory DataFrame"""
        return set(self.statements_df['unique_id'].unique())

    def flush_all(self):
        """Flush all pending data"""
        self.flush_mps()
        self.flush_statements()

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about stored data"""
        return {
            'total_mps': len(self.mps_df),
            'total_statements': len(self.statements_df),
            'unique_speakers': self.statements_df['speaker_name'].nunique(),
            'date_range': {
                'from': self.statements_df['proceeding_date'].min(),
                'to': self.statements_df['proceeding_date'].max()
            },
            'statements_by_club': (
                self.statements_df
                .merge(self.mps_df, left_on='speaker_mp_id', right_on='id')
                ['club']
                .value_counts()
                .to_dict()
            )
        }


class JSONStorage(StorageBackend):
    """JSON file storage backend"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.mps_file = data_dir / "mps.json"
        self.statements_dir = data_dir / "statements"
        self.statements_dir.mkdir(exist_ok=True)
        self.processed_file = data_dir / "processed_statements.json"

    def save_mp(self, mp: MP) -> None:
        """Save MP to JSON file"""
        mps = self._load_mps()
        mps[f"{mp.term}_{mp.id}"] = mp.to_dict()

        with open(self.mps_file, 'w', encoding='utf-8') as f:
            json.dump(mps, f, ensure_ascii=False, indent=2)

    def save_statement(self, statement: Statement) -> None:
        """Save statement to JSON file.
        Creates a file for each proceeding date and dumps individual statement data in it."""
        date_file = self.statements_dir / f"{statement.proceeding_date}.json"

        statements = {}
        if date_file.exists():
            with open(date_file, 'r', encoding='utf-8') as f:
                statements = json.load(f)

        statements[statement.unique_id] = statement.to_dict()

        with open(date_file, 'w', encoding='utf-8') as f:
            json.dump(statements, f, ensure_ascii=False, indent=2)

        # Mark as processed
        self._mark_processed(statement.unique_id)

    def get_processed_statements(self) -> set:
        """Get set of processed statements IDs"""
        if self.processed_file.exists():
            with open(self.processed_file, 'r') as f:
                return set(json.load(f))
        return set()

    def _load_mps(self) -> Dict[str, Any]:
        """Load MPs from file"""
        if self.mps_file.exists():
            with open(self.mps_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _mark_processed(self, statements_id: str) -> None:
        """Mark statements as processed"""
        processed = self.get_processed_statements()
        processed.add(statements_id)

        with open(self.processed_file, 'w') as f:
            json.dump(list(processed), f)

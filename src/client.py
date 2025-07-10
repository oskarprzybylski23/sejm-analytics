from typing import List, Dict

import requests


class SejmAPIClient:
    """Client for interacting with the Sejm API"""

    BASE_URL = "https://api.sejm.gov.pl"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Sejm-Analysis-Tool/1.0'
        })

    def get_mps(self, term: int = 10) -> List[Dict]:
        """Get all MPs for a given term"""
        url = f"{self.BASE_URL}/sejm/term{term}/MP"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_proceedings(self, term: int = 10) -> List[Dict]:
        """Get list of parliamentary proceedings"""
        url = f"{self.BASE_URL}/sejm/term{term}/proceedings"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_statement_list(self, term: int, proceeding_num: int, date: str) -> Dict:
        """Get list of statements for a specific proceeding day"""
        url = f"{self.BASE_URL}/sejm/term{term}/proceedings/{proceeding_num}/{date}/transcripts"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_statement_transcript(self, term: int, proceeding_num: int, date: str, statement_num: int) -> str:
        """Get content of a specific statement in HTML format"""
        url = f"{self.BASE_URL}/sejm/term{term}/proceedings/{proceeding_num}/{date}/transcripts/{statement_num}"
        response = self.session.get(url, headers={'Accept': 'text/html'})
        response.raise_for_status()
        return response.text

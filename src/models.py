"""Data models for the Sejm data pipeline"""
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, List, Dict, Any
import json


@dataclass
class MP:
    """Member of Parliament model"""
    id: int
    term: int
    first_name: str
    last_name: str
    club: Optional[str] = None
    district_name: Optional[str] = None
    district_num: Optional[int] = None
    voivodeship: Optional[str] = None
    profession: Optional[str] = None
    education_level: Optional[str] = None
    email: Optional[str] = None
    photo_url: Optional[str] = None
    active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_api_response(cls, data: Dict[str, Any], term: int) -> 'MP':
        """Create MP instance from API response"""
        return cls(
            id=data['id'],
            term=term,
            first_name=data.get('firstName', ''),
            last_name=data.get('lastName', ''),
            club=data.get('club'),
            district_name=data.get('districtName'),
            district_num=data.get('districtNum'),
            voivodeship=data.get('voivodeship'),
            profession=data.get('profession'),
            education_level=data.get('educationLevel'),
            email=data.get('email'),
            photo_url=f"https://api.sejm.gov.pl/sejm/term{term}/MP/{data['id']}/photo",
            active=data.get('active', True)
        )


@dataclass
class Statement:
    """Parliamentary statement model"""
    term: int
    proceeding_num: int
    proceeding_date: str
    statement_num: int
    speaker_name: str
    speaker_mp_id: Optional[int] = None
    speaker_function: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    content_text: str = ""
    content_html: str = ""
    is_unspoken: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def unique_id(self) -> str:
        """Generate unique identifier for the speech"""
        return f"{self.term}_{self.proceeding_num}_{self.proceeding_date}_{self.statement_num}"

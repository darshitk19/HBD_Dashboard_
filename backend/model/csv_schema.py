"""
Pydantic validation models for CSV business records.
Validates data AFTER normalization, BEFORE database insert.
"""
from pydantic import BaseModel, validator
from typing import Optional


class BusinessRecord(BaseModel):
    """Schema for a single row from Google Maps CSV data."""
    
    name: str
    address: Optional[str] = ""
    website: Optional[str] = ""
    phone_number: Optional[str] = ""
    reviews_count: Optional[int] = 0
    reviews_average: Optional[float] = None
    category: Optional[str] = ""
    subcategory: Optional[str] = ""
    city: Optional[str] = ""
    state: Optional[str] = ""
    area: Optional[str] = ""
    drive_file_id: str
    drive_file_name: str
    drive_file_path: str
    drive_uploaded_time: Optional[str] = None
    # Drive metadata (not validated, just passed through)
    drive_folder_id: Optional[str] = None
    drive_folder_name: Optional[str] = None

    @validator('name')
    def validate_name(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('Name is required and cannot be empty')
        return v.strip()

    @validator('reviews_average')
    def validate_rating(cls, v):
        if v is not None:
            try:
                v = float(v)
            except (ValueError, TypeError):
                return None
            if v < 0 or v > 5:
                raise ValueError('Rating must be between 0 and 5')
        return v

    @validator('reviews_count', pre=True)
    def coerce_reviews_count(cls, v):
        if v is None or v == '':
            return 0
        try:
            val = int(v)
            if val < 0:
                return 0
            return val
        except (ValueError, TypeError):
            return 0

    @validator('phone_number')
    def validate_phone(cls, v):
        if v and len(v) < 10:
            raise ValueError(f'Phone number too short: {v}')
        return v

    class Config:
        # Allow extra fields to pass through without error
        extra = 'ignore'

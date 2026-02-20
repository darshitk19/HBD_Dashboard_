import pytest
from model.normalizer import UniversalNormalizer
from model.csv_schema import BusinessRecord

def test_normalizer_and_validation():
    # Valid row
    row = {
        'name': 'Test Place',
        'address': '123 Main St',
        'website': 'http://example.com',
        'phone_number': '1234567890',
        'reviews_count': 10,
        'reviews_average': 4.5,
        'category': 'Cafe',
        'subcategory': 'Coffee',
        'city': 'Testville',
        'state': 'TestState',
        'area': 'Downtown',
        'drive_file_id': 'abc123',
        'drive_file_name': 'test.csv',
        'drive_folder_id': 'folder1',
        'drive_folder_name': 'TestFolder',
        'drive_file_path': '/TestFolder',
        'drive_uploaded_time': '2024-01-01T00:00:00Z'
    }
    norm = UniversalNormalizer.normalize_row(row)
    validated = BusinessRecord(**norm)
    assert validated.name == 'Test Place'
    assert validated.reviews_average == 4.5

    # Invalid row (bad rating)
    bad_row = dict(row)
    bad_row['reviews_average'] = 7.0
    norm = UniversalNormalizer.normalize_row(bad_row)
    with pytest.raises(Exception):
        BusinessRecord(**norm)

    # Missing required field
    bad_row2 = dict(row)
    del bad_row2['name']
    norm = UniversalNormalizer.normalize_row(bad_row2)
    with pytest.raises(Exception):
        BusinessRecord(**norm)

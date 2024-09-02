import pandas as pd
import pytest
from python_hddb.helpers import generate_field_metadata

def test_generate_field_metadata():
    # Create a sample DataFrame
    df = pd.DataFrame({
        'Name': ['John', 'Jane'],
        'Age': [30, 25],
        'City of Birth': ['New York', 'London']
    })

    # Generate field metadata
    metadata = generate_field_metadata(df)

    # Assertions
    assert len(metadata) == 3
    
    for item in metadata:
        assert set(item.keys()) == {'fld___id', 'label', 'id'}
        assert isinstance(item['fld___id'], str)
        assert len(item['fld___id']) == 36  # UUID length
        assert item['label'] in df.columns
        
    assert metadata[0]['label'] == 'Name'
    assert metadata[0]['id'] == 'name'
    
    assert metadata[1]['label'] == 'Age'
    assert metadata[1]['id'] == 'age'
    
    assert metadata[2]['label'] == 'City of Birth'
    assert metadata[2]['id'] == 'city_of_birth'

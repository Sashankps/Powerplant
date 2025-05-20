import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import pandas as pd
import io
from app.main import app

client = TestClient(app)

# Mock S3 client and data
@pytest.fixture
def mock_s3_client():
    mock_client = MagicMock()
    return mock_client

@pytest.fixture
def mock_dataframe():
    # Create a mock dataframe with sample data
    data = {
        "GENID": ["GEN1", "GEN2", "GEN3", "GEN4", "GEN5"],
        "PNAME": ["Plant A", "Plant B", "Plant C", "Plant D", "Plant E"],
        "PSTATEABB": ["CA", "CA", "NY", "NY", "TX"],
        "ORISPL": [1001, 1002, 1003, 1004, 1005],
        "GENNTAN": [15000.0, 25000.0, 10000.0, 30000.0, 20000.0]
    }
    return pd.DataFrame(data)

def test_root_endpoint():
    """Test that the root endpoint returns a welcome message"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the Power Plant API"}

@patch('app.main.get_data_from_s3')
def test_get_states(mock_get_data, mock_dataframe):
    """Test getting states endpoint"""
    mock_get_data.return_value = mock_dataframe
    
    response = client.get("/states")
    assert response.status_code == 200
    assert sorted(response.json()) == ["CA", "NY", "TX"]

@patch('app.main.get_data_from_s3')
def test_get_plants(mock_get_data, mock_dataframe):
    """Test getting plants endpoint with state filter"""
    mock_get_data.return_value = mock_dataframe
    
    # Test CA state
    response = client.get("/plants?state=CA&limit=5")
    assert response.status_code == 200
    plants = response.json()
    assert len(plants) == 2
    
    # Verify the order (by net generation, descending)
    assert plants[0]["name"] == "Plant B"
    assert plants[0]["netGeneration"] == 25000.0
    assert plants[1]["name"] == "Plant A"
    assert plants[1]["netGeneration"] == 15000.0
    
    # Test NY state with limit
    response = client.get("/plants?state=NY&limit=1")
    assert response.status_code == 200
    plants = response.json()
    assert len(plants) == 1
    assert plants[0]["name"] == "Plant D"
    assert plants[0]["netGeneration"] == 30000.0

@patch('app.main.get_data_from_s3')
def test_get_plants_nonexistent_state(mock_get_data, mock_dataframe):
    """Test getting plants for a state that doesn't exist in the data"""
    mock_get_data.return_value = mock_dataframe
    
    response = client.get("/plants?state=ZZ&limit=5")
    assert response.status_code == 200
    assert response.json() == [] 
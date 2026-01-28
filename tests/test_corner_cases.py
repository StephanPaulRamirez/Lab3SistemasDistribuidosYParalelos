
import pytest
from unittest.mock import MagicMock, patch, mock_open
from validator.app import _load_schemas
# aggregator has is_duplicate
from aggregator.app import is_duplicate
from audit.app import init_db

@patch("validator.app.SCHEMA_PATH")
@patch("pathlib.Path.open", new_callable=mock_open, read_data='{"type": "object"}')
@patch("validator.app.INPUT_STREAMS", ["stream1"])
def test_validator_load_schemas_success(mock_file, mock_path):
    # Mock path.open to return valid json
    # SCHEMA_PATH / filename -> path
    # We need to mock Path object returned by SCHEMA_PATH / filename
    
    # Simpler: mock json.load
    with patch("json.load") as mock_json:
        mock_json.return_value = {"type": "object"}
        _load_schemas()
        assert mock_json.call_count == 1

@patch("validator.app.SCHEMA_PATH")
@patch("validator.app.INPUT_STREAMS", ["stream1"])
def test_validator_load_schemas_not_found(mock_path):
    # Mock open to raise FileNotFoundError
    # The code calls path.open()
    # Path is created via SCHEMA_PATH / filename
    
    mock_file_path = MagicMock()
    mock_path.__truediv__.return_value = mock_file_path
    mock_file_path.open.side_effect = FileNotFoundError 
    
    # Should catch and log error, not crash
    _load_schemas()

@patch("validator.app.SCHEMA_PATH")
@patch("validator.app.INPUT_STREAMS", ["stream1"])
def test_validator_load_schemas_invalid_json(mock_path):
    mock_file_path = MagicMock()
    mock_path.__truediv__.return_value = mock_file_path
    mock_file_path.open.return_value.__enter__.return_value = MagicMock()
    
    with patch("json.load", side_effect=ValueError("Bad JSON")):
        with pytest.raises(ValueError):
            _load_schemas()

def test_aggregator_is_duplicate():
    client = MagicMock()
    # First time: sadd returns 1 (new)
    client.sadd.return_value = 1
    assert not is_duplicate(client, "e1")
    
    # Second time: sadd returns 0 (duplicate)
    client.sadd.return_value = 0
    assert is_duplicate(client, "e1")
    
    assert client.expire.call_count == 2

@patch("audit.app.sqlite3.connect")
def test_audit_init_db(mock_connect):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur
    
    init_db()
    
    # Checks that tables are created
    assert mock_cur.execute.call_count == 3
    assert "CREATE TABLE" in mock_cur.execute.call_args_list[0][0][0]

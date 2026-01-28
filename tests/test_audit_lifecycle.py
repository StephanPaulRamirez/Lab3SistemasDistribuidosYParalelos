
import pytest
from unittest.mock import MagicMock, patch
from audit.app import main, process_events, process_metrics
import redis

# Removed test_run_audit_cycle as the function does not exist anymore.


@patch("audit.app.process_metrics")
@patch("audit.app.process_events")
@patch("audit.app.init_db")
@patch("audit.app.ensure_groups")
@patch("redis.Redis")
def test_main_loop(mock_redis_cls, mock_ensure, mock_init, mock_events, mock_metrics):
    # Break loop
    mock_events.side_effect = Exception("Break")
    
    try:
        main()
    except Exception as e:
        assert str(e) == "Break"
        
    assert mock_init.call_count == 1
    assert mock_init.call_count == 1
    assert mock_ensure.call_count == 1
    assert mock_events.call_count == 1

@patch("audit.app.process_metrics")
@patch("audit.app.process_events")
@patch("audit.app.init_db")
@patch("audit.app.ensure_groups")
@patch("redis.Redis")
def test_main_redis_error_retry(mock_redis_cls, mock_ensure, mock_init, mock_events, mock_metrics):
    # First call raises connection error, second call works, third call breaks loop
    mock_events.side_effect = [
        redis.ConnectionError("Connection lost"),
        None,
        Exception("Break")
    ]
    
    with patch("time.sleep") as mock_sleep:
        try:
            main()
        except Exception:
            pass
        
        assert mock_sleep.call_count == 1
        # ensure_groups called once at start
        assert mock_ensure.call_count == 1
        
@patch("audit.app.process_metrics")
@patch("audit.app.process_events")
@patch("audit.app.init_db")
@patch("audit.app.ensure_groups")
@patch("redis.Redis")
def test_runtime_metrics_logging(mock_redis_cls, mock_ensure, mock_init, mock_events, mock_metrics, capsys):
    # We want to trigger the log_json for runtime metrics
    # Logic is: if now - last_metrics_log >= AUDIT_METRICS_LOG_INTERVAL
    
    # We set side_effect to run once, then break
    mock_events.side_effect = [None, Exception("Break")]
    
    with patch("audit.app.AUDIT_METRICS_LOG_INTERVAL", 0.001):
        with patch("time.time", side_effect=[100, 100.1, 100.2]):
            # Mock DB counts for metrics
            # run_audit_cycle uses "with db_connect() ... select count"
            with patch("audit.app.db_connect") as mock_db:
                mock_conn = MagicMock()
                mock_db.return_value.__enter__.return_value = mock_conn
                mock_conn.cursor.return_value.execute.return_value.fetchone.return_value = [5]
                
                with patch("audit.app.log_json") as mock_log:
                    try:
                        main()
                    except Exception:
                        pass
                    
                    # Should see "audit_runtime_metrics"
                    found = False
                    for call_args in mock_log.call_args_list:
                        if call_args[0][1] == "audit_runtime_metrics":
                            found = True
                            break
                    assert found

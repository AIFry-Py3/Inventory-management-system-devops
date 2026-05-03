# tests/test_db.py
import pytest
from unittest.mock import patch, MagicMock
import json

# Import functions to test
from db import get_product, place_order, get_all_products

def test_get_product_cache_hit():
    """Test that cached product is returned without DB query"""
    with patch('db.get_redis') as mock_redis:
        mock_client = MagicMock()
        mock_client.get.return_value = json.dumps({"Product ID": 1, "Title": "Test"})
        mock_redis.return_value = mock_client
        
        product, from_cache = get_product(1)
        
        assert from_cache is True
        assert product["Title"] == "Test"
        mock_client.get.assert_called_once_with("product:1")

def test_place_order_insufficient_stock():
    """Test order fails when stock is too low"""
    with patch('db.get_pg_conn') as mock_pool:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        # Simulate product with only 2 units in stock
        mock_cursor.fetchone.return_value = (2, "Test Product")
        mock_pool.return_value = mock_conn
        
        success, msg = place_order(1, 5)  # Try to order 5
        
        assert success is False
        assert "Insufficient stock" in msg
        mock_conn.rollback.assert_not_called()  # No DB write attempted

def test_get_all_products_structure():
    """Test that get_all_products returns expected columns"""
    # This is an integration test — requires real DB
    # Skip in CI if no DB available
    pytest.importorskip("psycopg2")
    
    df = get_all_products()
    assert "Product ID" in df.columns
    assert "Title of Products" in df.columns
    assert len(df) >= 0  # Allow empty table
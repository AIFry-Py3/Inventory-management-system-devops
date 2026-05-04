import psycopg2
import psycopg2.pool
import redis
import json
import pandas as pd
import os
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


# ── Logging Setup ──────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Configuration from Environment Variables ───────────────────────────────
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'mydatabase'),
    'user': os.getenv('DB_USER', 'myuser'),
    'password': os.getenv('DB_PASSWORD', 'mysecretpassword'),
    'port': int(os.getenv('DB_PORT', 5433))
}

REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', '127.0.0.1'),
    'port': int(os.getenv('REDIS_PORT', 6379)),
    'db': int(os.getenv('REDIS_DB', 0)),
    'decode_responses': True
}

TABLE = '"Dataset_for_NGD"'
CACHE_TTL = int(os.getenv('CACHE_TTL', 600))
HIT_THRESHOLD = int(os.getenv('HIT_THRESHOLD', 3))

# ── Connection Pool Initialization (ONLY ONCE) ─────────────────────────────
_pg_pool = None
_redis_client = None

def _init_db_pool():
    global _pg_pool
    if _pg_pool is None:
        try:
            _pg_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1, maxconn=10, **DB_CONFIG
            )
            logger.info("✅ PostgreSQL connection pool initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize PostgreSQL pool: {e}")
            raise

def _init_redis():
    global _redis_client
    if _redis_client is None:
        try:
            _redis_client = redis.Redis(**REDIS_CONFIG)
            _redis_client.ping()  # Test connection
            logger.info("✅ Redis client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis: {e}")
            raise

# Lazy initialization on first use
_init_db_pool()
_init_redis()

# ── Connection Helpers ─────────────────────────────────────────────────────

# ─── CONNECTION HELPERS ───────────────────────────────────────────────────────
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def get_pg_conn():
    """Get a connection from the pool (auto-retries on transient failures)"""
    return _pg_pool.getconn()

def release_pg_conn(conn):
    """Return a connection to the pool"""
    if conn:
        _pg_pool.putconn(conn)

def get_redis():
    """Get the Redis client"""
    return _redis_client

# ── Single Product Lookup with Caching ─────────────────────────────────────
def get_product(product_id):
    """Fetch product by ID with Redis caching after HIT_THRESHOLD accesses"""
    r = get_redis()
    cache_key = f"product:{product_id}"

    # Try cache first
    cached = r.get(cache_key)
    if cached:
        r.zincrby("product:hits", 1, cache_key)
        logger.debug(f"Cache HIT for product {product_id}")
        return json.loads(cached), True

    # Cache miss → query PostgreSQL
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM {TABLE} WHERE "Product ID" = %s', (product_id,))
        row = cur.fetchone()
        if not row:
            logger.warning(f"Product {product_id} not found")
            return None, False
        
        # Convert row to dict using column names
        cols = [desc[0] for desc in cur.description]
        product = dict(zip(cols, row))
    finally:
        release_pg_conn(conn)

    # Track access count for caching decision
    hits = r.zincrby("product:hits", 1, cache_key)
    if hits >= HIT_THRESHOLD:
        r.setex(cache_key, CACHE_TTL, json.dumps(product, default=str))
        logger.debug(f"Cached product {product_id} after {int(hits)} hits")

    return product, False

# ── Bulk Product Queries ───────────────────────────────────────────────────
def get_all_products():
    """Fetch all products for dashboard/inventory"""
    conn = get_pg_conn()
    try:
        return pd.read_sql(f'SELECT * FROM {TABLE} ORDER BY "Product ID"', conn)
    finally:
        release_pg_conn(conn)

def get_low_stock(threshold=10):
    """Get products with stock <= threshold but > 0"""
    conn = get_pg_conn()
    try:
        return pd.read_sql(
            f'SELECT * FROM {TABLE} WHERE "Products in Store" <= %s AND "Products in Store" > 0',
            conn, params=[threshold]
        )
    finally:
        release_pg_conn(conn)

def get_out_of_stock():
    """Get products with zero stock"""
    conn = get_pg_conn()
    try:
        return pd.read_sql(
            f'SELECT * FROM {TABLE} WHERE "Products in Store" = 0',
            conn
        )
    finally:
        release_pg_conn(conn)

# ── Analytics: Top Accessed Products ───────────────────────────────────────
def get_top_products(n=10):
    """Return top-N most accessed products from Redis cache"""
    r = get_redis()
    top_keys = r.zrevrange("product:hits", 0, n - 1, withscores=True)
    results = []
    
    for cache_key, score in top_keys:
        cached = r.get(cache_key)
        if cached:
            product = json.loads(cached)
            product["Access Count"] = int(score)
            results.append(product)
    
    return results

# ── Cache Invalidation ─────────────────────────────────────────────────────
def invalidate_product(product_id):
    """Remove product from Redis cache on update/delete"""
    get_redis().delete(f"product:{product_id}")
    logger.debug(f"Invalidated cache for product {product_id}")

# ── Order Operations ───────────────────────────────────────────────────────
def place_order(product_id: int, quantity: int):
    """Place an order: reduce stock, increment sold count, invalidate cache"""
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        
        # Check stock availability
        cur.execute(
            'SELECT "Products in Store", "Title of Products" FROM "Dataset_for_NGD" WHERE "Product ID" = %s',
            (product_id,)
        )
        row = cur.fetchone()
        if not row:
            return False, "Product not found."
        
        current_stock, title = row
        if current_stock < quantity:
            return False, f"Insufficient stock. Only {current_stock} units available."

        # Update stock and sold count
        cur.execute(
            'UPDATE "Dataset_for_NGD" SET "Products in Store" = "Products in Store" - %s, "Products Sold" = "Products Sold" + %s WHERE "Product ID" = %s',
            (quantity, quantity, product_id)
        )
        conn.commit()
        
        invalidate_product(product_id)
        logger.info(f"Order placed: {quantity}x {title} (ID: {product_id})")
        return True, f"Order placed for {quantity} units of '{title}'."
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Order failed: {e}")
        return False, str(e)
    finally:
        release_pg_conn(conn)

def restock_product(product_id: int, quantity: int):
    """Add stock to a product"""
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        
        cur.execute('SELECT "Title of Products" FROM "Dataset_for_NGD" WHERE "Product ID" = %s', (product_id,))
        row = cur.fetchone()
        if not row:
            return False, "Product not found."

        cur.execute(
            'UPDATE "Dataset_for_NGD" SET "Products in Store" = "Products in Store" + %s WHERE "Product ID" = %s',
            (quantity, product_id)
        )
        conn.commit()
        
        invalidate_product(product_id)
        logger.info(f"Restocked: {quantity} units added to {row[0]} (ID: {product_id})")
        return True, f"Restocked {quantity} units for '{row[0]}'."
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Restock failed: {e}")
        return False, str(e)
    finally:
        release_pg_conn(conn)

# ── Product CRUD ───────────────────────────────────────────────────────────
def add_product(title, price, discount, in_store, sold):
    """Add a new product to the database"""
    # Use the connection pool, not a new direct connection
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        
        # Get next available Product ID
        cur.execute('SELECT COALESCE(MAX("Product ID"), 0) FROM "Dataset_for_NGD"')
        max_id = cur.fetchone()[0]
        new_id = max_id + 1
        
        cur.execute('''
            INSERT INTO "Dataset_for_NGD" 
            ("Product ID", "Title of Products", "Price ($)", "Discount (%)", "Products in Store", "Products Sold")
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (new_id, title, price, discount, in_store, sold))
        
        conn.commit()
        logger.info(f"Added product: {title} (ID: {new_id})")
        return True, f"Product added with ID {new_id}."
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Add product failed: {e}")
        return False, str(e)
    finally:
        release_pg_conn(conn)

def remove_product(product_id):
    """Delete a product from the database"""
    conn = get_pg_conn()
    try:
        cur = conn.cursor()
        cur.execute('DELETE FROM "Dataset_for_NGD" WHERE "Product ID" = %s', (product_id,))
        
        if cur.rowcount == 0:
            return False, "Product not found."
        
        conn.commit()
        invalidate_product(product_id)
        logger.info(f"Removed product ID: {product_id}")
        return True, "Product removed successfully."
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Remove product failed: {e}")
        return False, str(e)
    finally:
        release_pg_conn(conn)
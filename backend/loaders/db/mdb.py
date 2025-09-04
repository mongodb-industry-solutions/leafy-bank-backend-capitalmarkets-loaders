import os
import time
import logging
import threading
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from abc import abstractmethod
from dotenv import load_dotenv
from functools import wraps
from typing import Optional, Callable, Any

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def retry_on_connection_error(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry operations on connection errors.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Exponential backoff multiplier
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_error = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Connection error in {func.__name__} (attempt {attempt + 1}/{max_attempts}): {e}. "
                            f"Retrying in {current_delay} seconds..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                        
                        # Force reconnection on the instance if it has a db attribute
                        if hasattr(args[0], '_db'):
                            args[0]._db = None
                    else:
                        logger.error(f"Max retry attempts reached for {func.__name__}. Last error: {e}")
                except Exception as e:
                    # Non-connection errors should not be retried
                    raise
            
            raise last_error
        return wrapper
    return decorator


class MongoDBConnectionFactory:
    """
    Factory for creating MongoDB connections with auto-recovery capabilities.
    Maintains a cached client to avoid creating multiple connection pools.
    Thread-safe implementation with locking.
    """
    _cached_client = None
    _client_creation_time = None
    _client_lifetime = 3600  # Recreate client every hour to ensure freshness
    _lock = threading.Lock()  # Thread safety for client creation
    
    @staticmethod
    def create_client(max_retry_time: int = 300) -> MongoClient:
        """
        Get or create a MongoDB client with retry logic.
        Reuses existing client if it's healthy and not too old.
        Thread-safe implementation.
        
        Args:
            max_retry_time: Maximum time to retry connection in seconds (default: 5 minutes)
            
        Returns:
            MongoClient: A connected MongoDB client
            
        Raises:
            ConnectionFailure: If unable to connect within retry window
        """
        # First check without lock for performance (double-checked locking pattern)
        if MongoDBConnectionFactory._cached_client is not None:
            age = time.time() - MongoDBConnectionFactory._client_creation_time
            if age < MongoDBConnectionFactory._client_lifetime:
                try:
                    # Verify client is still healthy
                    MongoDBConnectionFactory._cached_client.admin.command('ping')
                    return MongoDBConnectionFactory._cached_client
                except Exception as e:
                    # Client is unhealthy, need to recreate
                    logger.info(f"Cached MongoDB client is unhealthy: {e}. Will create new client")
        
        # Acquire lock for client creation/replacement
        with MongoDBConnectionFactory._lock:
            # Check again inside the lock (another thread might have created it)
            if MongoDBConnectionFactory._cached_client is not None:
                age = time.time() - MongoDBConnectionFactory._client_creation_time
                if age < MongoDBConnectionFactory._client_lifetime:
                    try:
                        MongoDBConnectionFactory._cached_client.admin.command('ping')
                        return MongoDBConnectionFactory._cached_client
                    except:
                        pass
            
            # Close old client if exists
            if MongoDBConnectionFactory._cached_client is not None:
                try:
                    MongoDBConnectionFactory._cached_client.close()
                except:
                    pass
                MongoDBConnectionFactory._cached_client = None
            
            # Create new client inside the lock
            uri = os.getenv("MONGODB_URI")
            appname = os.getenv("APP_NAME", "capitalmarkets-loaders")
            
            start_time = time.time()
            last_error = None
            attempt = 0
            
            while time.time() - start_time < max_retry_time:
                try:
                    client = MongoClient(
                        uri,
                        appname=appname,
                        serverSelectionTimeoutMS=5000,  # 5 second timeout for server selection
                        connectTimeoutMS=5000,           # 5 second connection timeout
                        socketTimeoutMS=5000,            # 5 second socket timeout
                        maxPoolSize=50,                  # Maximum connections in pool
                        minPoolSize=0,                   # Allow pool to shrink to 0
                        maxIdleTimeMS=300000,            # 5 minute idle timeout
                        waitQueueTimeoutMS=5000,         # 5 second wait for connection from pool
                        retryWrites=True,                # Automatic retry for write operations
                        retryReads=True                  # Automatic retry for read operations
                    )
                    
                    # Verify connection with ping
                    client.admin.command('ping')
                    logger.info("MongoDB connection established successfully")
                    
                    # Cache the client for reuse
                    MongoDBConnectionFactory._cached_client = client
                    MongoDBConnectionFactory._client_creation_time = time.time()
                    
                    return client
                    
                except Exception as e:
                    last_error = e
                    attempt += 1
                    wait_time = min(30, 2 ** attempt)  # Exponential backoff, max 30 seconds
                    
                    if time.time() - start_time + wait_time < max_retry_time:
                        logger.warning(
                            f"Failed to connect to MongoDB (attempt {attempt}): {e}. "
                            f"Retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                    else:
                        break
            
            error_msg = f"Failed to connect to MongoDB after {max_retry_time} seconds. Last error: {last_error}"
            logger.error(error_msg)
            raise ConnectionFailure(error_msg)
    
    @staticmethod
    def get_database(database_name: Optional[str] = None, max_retry_time: int = 30):
        """
        Get a database instance with a fresh connection.
        
        Args:
            database_name: Name of the database (uses env var if not provided)
            max_retry_time: Maximum time to retry connection in seconds
            
        Returns:
            Database instance
        """
        client = MongoDBConnectionFactory.create_client(max_retry_time)
        db_name = database_name or os.getenv("DATABASE_NAME")
        return client[db_name]
    
    @staticmethod
    def close_cached_client():
        """
        Close the cached MongoDB client if it exists.
        Useful for cleanup during shutdown or testing.
        """
        if MongoDBConnectionFactory._cached_client is not None:
            try:
                MongoDBConnectionFactory._cached_client.close()
                logger.info("Closed cached MongoDB client")
            except Exception as e:
                logger.warning(f"Error closing cached MongoDB client: {e}")
            finally:
                MongoDBConnectionFactory._cached_client = None
                MongoDBConnectionFactory._client_creation_time = None


class MongoDBConnector:
    """
    Base class for MongoDB operations with resilient connection handling.
    Uses connection factory for auto-recovery from connection failures.
    """
    
    def __init__(self, uri=None, database_name=None, collection_name=None, appname=None):
        """
        Initialize MongoDB connector with lazy connection.
        Connection is established on first use, not during initialization.
        """
        # Store configuration
        self.database_name = database_name or os.getenv("DATABASE_NAME")
        self.collection_name = collection_name
        self.uri = uri or os.getenv("MONGODB_URI")
        self.appname = appname or os.getenv("APP_NAME")
        
        # Lazy initialization - connection created on first access
        self._db = None
        self._client = None
    
    @property
    def db(self):
        """
        Get database instance with automatic connection recovery.
        Creates a new connection if current one is unhealthy.
        """
        if self._db is None or not self._is_connection_healthy():
            try:
                self._db = MongoDBConnectionFactory.get_database(self.database_name)
                self._client = self._db.client
            except Exception as e:
                logger.error(f"Failed to establish database connection: {e}")
                raise
        return self._db
    
    @property
    def client(self):
        """
        Get client instance with automatic connection recovery.
        """
        if self._client is None or not self._is_connection_healthy():
            # Accessing self.db will create the connection
            _ = self.db
        return self._client
    
    def _is_connection_healthy(self) -> bool:
        """
        Check if the current connection is healthy.
        
        Returns:
            bool: True if connection is healthy, False otherwise
        """
        if self._client is None:
            return False
            
        try:
            # Ping with short timeout to check health
            self._client.admin.command('ping', serverSelectionTimeoutMS=2000)
            return True
        except Exception:
            return False
    
    @abstractmethod
    def run(self, **kwargs):
        """
        Abstract method interface defining common run method.
        """
        pass
    
    @retry_on_connection_error()
    def get_collection(self, collection_name=None):
        """Retrieve a collection with retry logic."""
        collection_name = collection_name or self.collection_name
        return self.db[collection_name]
    
    @retry_on_connection_error()
    def insert_one(self, collection_name, document):
        """Insert a single document into a collection with retry logic."""
        collection = self.get_collection(collection_name)
        result = collection.insert_one(document)
        return result.inserted_id
    
    @retry_on_connection_error()
    def insert_many(self, collection_name, documents):
        """Insert multiple documents into a collection with retry logic."""
        collection = self.get_collection(collection_name)
        result = collection.insert_many(documents)
        return result.inserted_ids
    
    @retry_on_connection_error()
    def find(self, collection_name, query={}, projection=None):
        """Retrieve documents from a collection with retry logic."""
        collection = self.get_collection(collection_name)
        return list(collection.find(query, projection))
    
    @retry_on_connection_error()
    def update_one(self, collection_name, query, update, upsert=False):
        """Update a single document in a collection with retry logic."""
        collection = self.get_collection(collection_name)
        result = collection.update_one(query, update, upsert=upsert)
        return result.modified_count
    
    @retry_on_connection_error()
    def update_many(self, collection_name, query, update, upsert=False):
        """Update multiple documents in a collection with retry logic."""
        collection = self.get_collection(collection_name)
        result = collection.update_many(query, update, upsert=upsert)
        return result.modified_count
    
    @retry_on_connection_error()
    def delete_one(self, collection_name, query):
        """Delete a single document from a collection with retry logic."""
        collection = self.get_collection(collection_name)
        result = collection.delete_one(query)
        return result.deleted_count
    
    @retry_on_connection_error()
    def delete_many(self, collection_name, query):
        """Delete multiple documents from a collection with retry logic."""
        collection = self.get_collection(collection_name)
        result = collection.delete_many(query)
        return result.deleted_count
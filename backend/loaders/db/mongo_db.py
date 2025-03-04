import os
from pymongo import MongoClient
from abc import abstractmethod
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class MongoDBConnector:
    _instance = None

    def __new__(cls, uri=None, database_name=None, appname=None):

        if not cls._instance:
            cls._instance = super(MongoDBConnector, cls).__new__(cls)
            cls._instance.uri = uri or os.getenv("MONGODB_URI")
            cls._instance.database_name = database_name or os.getenv("DATABASE_NAME")
            cls._instance.appname = appname or os.getenv("APP_NAME")
            cls._instance.client = MongoClient(cls._instance.uri, appname=cls._instance.appname)
            cls._instance.db = cls._instance.client[cls._instance.database_name]
            cls._instance._initialized = True
        return cls._instance

    def __init__(self, uri=None, database_name=None, appname=None):
        """ Prevent reinitialization in the singleton. """
        pass

    @abstractmethod
    def run(self, **kwargs):
        """
        Abstract method interface defining common run method.
        """
        pass

    def get_collection(self, collection_name=None):
        """Retrieve a collection."""
        collection_name = collection_name
        return self.db[collection_name]

    def insert_one(self, collection_name, document):
        """Insert a single document into a collection."""
        collection = self.get_collection(collection_name)
        result = collection.insert_one(document)
        return result.inserted_id

    def insert_many(self, collection_name, documents):
        """Insert multiple documents into a collection."""
        collection = self.get_collection(collection_name)
        result = collection.insert_many(documents)
        return result.inserted_ids

    def find(self, collection_name, query={}, projection=None):
        """Retrieve documents from a collection."""
        collection = self.get_collection(collection_name)
        return list(collection.find(query, projection))

    def update_one(self, collection_name, query, update, upsert=False):
        """Update a single document in a collection."""
        collection = self.get_collection(collection_name)
        result = collection.update_one(query, update, upsert=upsert)
        return result.modified_count

    def update_many(self, collection_name, query, update, upsert=False):
        """Update multiple documents in a collection."""
        collection = self.get_collection(collection_name)
        result = collection.update_many(query, update, upsert=upsert)
        return result.modified_count

    def delete_one(self, collection_name, query):
        """Delete a single document from a collection."""
        collection = self.get_collection(collection_name)
        result = collection.delete_one(query)
        return result.deleted_count

    def delete_many(self, collection_name, query):
        """Delete multiple documents from a collection."""
        collection = self.get_collection(collection_name)
        result = collection.delete_many(query)
        return result.deleted_count
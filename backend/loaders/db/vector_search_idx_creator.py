from mongo_db import MongoDBConnector
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_vector_search_index(
    cluster_uri: str,
    database_name: str,
    collection_name: str,
    index_name: str = None,
    vector_field: str = "embedding",
    # https://docs.cohere.com/v2/docs/cohere-embed
    dimensions: int = 1024,  # For cohere.embed-english-v3
    similarity_metric: str = "cosine"  # Options: "cosine", "euclidean", "dotProduct"
):
    """
    Creates a vector search index on a MongoDB Atlas collection.

    :param cluster_uri: MongoDB connection URI.
    :param database_name: Name of the database.
    :param collection_name: Name of the collection.
    :param index_name: Name of the index (default: "vector_search_index").
    :param vector_field: The field containing vector embeddings.
    :param dimensions: The dimension of vector embeddings (default: 1024).
    :param similarity_metric: Similarity function (cosine, euclidean, dotProduct).
    """

    if index_name is None:
        index_name = f"{collection_name}_VS_IDX"

    logging.info(f"Creating vector search index...")
    logging.info(f"Cluster URI: {cluster_uri}")
    logging.info(f"Database: {database_name}")
    logging.info(f"Collection: {collection_name}")
    logging.info(f"Vector Field: {vector_field}")
    logging.info(f"Dimensions: {dimensions}")
    logging.info(f"Similarity Metric: {similarity_metric}")

    # Connect to MongoDB
    mongo_client = MongoDBConnector(uri=cluster_uri, database_name=database_name)

    db = mongo_client.db
    collection = db[collection_name]

    # Define the vector search index configuration
    index_config = {
        "name": index_name,
        "type": "vectorSearch",
        "definition": {
            "fields": [
                {
                    "path": vector_field,
                    "type": "vector",
                    "numDimensions": dimensions,
                    "similarity": similarity_metric
                }
            ]
        }
    }

    try:
        # Create the index
        collection.create_search_index(index_config)
        logging.info(f"Vector search index '{index_name}' created successfully on {database_name}.{collection_name}.")
    
    except Exception as e:
        logging.error(f"Error creating vector search index: {e}")

    finally:
        mongo_client.client.close()


# Example usage
if __name__ == "__main__":

    create_vector_search_index(
        cluster_uri=os.getenv("MONGODB_URI"),
        database_name=os.getenv("DATABASE_NAME"),
        collection_name=os.getenv("NEWS_COLLECTION"),
        vector_field="article_embedding",
        dimensions=1024,  # Adjust based on your model
        similarity_metric="cosine"  # Options: "cosine", "euclidean", "dotProduct"
    )
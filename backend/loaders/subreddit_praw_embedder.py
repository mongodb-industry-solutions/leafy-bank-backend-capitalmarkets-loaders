import os
from dotenv import load_dotenv
import logging
from pymongo import UpdateOne
from tqdm import tqdm
from db.mdb import MongoDBConnector
from embeddings.vogayeai.vogaye_ai_embeddings import VogayeAIEmbeddings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SubredditPrawEmbedder(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, 
                 submissions_collection_name: str = os.getenv("SUBREDDIT_SUBMISSIONS_COLLECTION", "subredditSubmissions")):
        """
        Reddit Subreddit data embedder for MongoDB.
        Generates embeddings for submissions and stores them back in MongoDB.
        """
        # Load environment variables
        load_dotenv()
        
        # Initialize MongoDB connection
        super().__init__(uri, database_name, appname)
        self.submissions_collection_name = submissions_collection_name
        logger.info(f"Using MongoDB collection '{self.submissions_collection_name}' for subreddit submissions")
        
        # Initialize Voyage AI embeddings client
        api_key = os.getenv("VOYAGE_API_KEY")
        if not api_key:
            raise ValueError("VOYAGE_API_KEY environment variable is not set")
        
        self.voyage_embeddings = VogayeAIEmbeddings(api_key=api_key)
        self.voyage_model_id = "voyage-finance-2"
        logger.info(f"Initialized VogayeAIEmbeddings with model: {self.voyage_model_id}")
        
        # Counters for tracking
        self.tokens_processed = 0
        self.api_requests_made = 0
        
    def _prepare_text_from_submission_dict(self, submission_dict):
        """
        Convert submission_dict to a text representation for embedding.
        Handles different formats and ensures compatibility.
        """
        if not submission_dict:
            return ""
        
        # For structured dict, serialize to ensure consistency
        text_parts = [
            f"Title: {submission_dict.get('title', '')}",
            f"Content: {submission_dict.get('selftext', '')}",
            f"Subreddit: {submission_dict.get('subreddit', '')}",
            f"Asset: {submission_dict.get('asset_id', '')}"
        ]
        
        # Add comments if available
        comments = submission_dict.get('comments', [])
        if comments:
            comment_texts = []
            for i, comment in enumerate(comments):
                if i >= 3:  # Only include up to 3 comments
                    break
                comment_text = f"Comment {i+1} by {comment.get('author', '[deleted]')}: {comment.get('body', '')}"
                comment_texts.append(comment_text)
            
            if comment_texts:
                text_parts.append("Comments: " + " | ".join(comment_texts))
        
        # Join all parts with newlines
        return "\n".join(text_parts)
    
    def drop_submission_embeddings(self):
        """
        Drops the submission_embeddings attribute from all documents in the collection.
        Useful for regenerating all embeddings.
        """
        logger.info(f"Dropping submission_embeddings attribute from all documents in {self.submissions_collection_name}...")
        result = self.db[self.submissions_collection_name].update_many(
            {"submission_embeddings": {"$exists": True}},
            {"$unset": {"submission_embeddings": ""}}
        )
        logger.info(f"Dropped submission_embeddings from {result.modified_count} documents.")
        return result.modified_count
    
    def count_documents_without_embeddings(self):
        """
        Count how many documents need embeddings to be generated.
        """
        count = self.db[self.submissions_collection_name].count_documents(
            {"submission_embeddings": {"$exists": False}}
        )
        logger.info(f"Found {count} documents without embeddings.")
        return count
    
    def _process_batch(self, docs_batch, operations):
        """
        Process a batch of documents and generate their embeddings.
        
        Args:
            docs_batch (list): List of documents to process
            operations (list): List to add update operations to
        
        Returns:
            dict: Statistics about the batch processing
        """
        batch_stats = {'processed': 0, 'successful': 0, 'failed': 0}
        
        for doc in docs_batch:
            try:
                # Extract submission_dict
                submission_dict = doc.get('submission_dict', {})
                
                # Convert submission_dict to text
                text = self._prepare_text_from_submission_dict(submission_dict)
                
                # Skip if no text to embed
                if not text:
                    logger.warning(f"Skipping document {doc['_id']} - no text to embed")
                    batch_stats['failed'] += 1
                    batch_stats['processed'] += 1
                    continue
                
                # Better token estimation (closer to how tokenizers work)
                # Average English word is ~5 chars, and tokenizers often split into subwords
                estimated_tokens = len(text.split()) + len(text) // 20  # Words + extra for subword tokens
                self.tokens_processed += estimated_tokens
                
                # Generate embedding
                embedding_result = self.voyage_embeddings.get_embeddings(
                    model_id=self.voyage_model_id, 
                    text=text
                )
                self.api_requests_made += 1
                
                # Check if embedding generation was successful
                if embedding_result is None:
                    logger.error(f"Failed to generate embedding for document {doc['_id']}")
                    batch_stats['failed'] += 1
                    batch_stats['processed'] += 1
                    continue
                
                # Create update operation
                operations.append(
                    UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {"submission_embeddings": embedding_result}}
                    )
                )
                
                batch_stats['successful'] += 1
                batch_stats['processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing document {doc.get('_id', 'unknown')}: {str(e)}")
                batch_stats['failed'] += 1
                batch_stats['processed'] += 1
        
        # Execute bulk update if there are operations
        if operations:
            try:
                result = self.db[self.submissions_collection_name].bulk_write(operations)
                logger.debug(f"Bulk update result: {result.modified_count} documents modified")
            except Exception as e:
                logger.error(f"Error performing bulk update: {str(e)}")
                batch_stats['successful'] = 0
                batch_stats['failed'] = batch_stats['processed']
        
        return batch_stats

    def generate_embeddings(self, batch_size=100, max_documents=None, progress_update_frequency=5):
        """
        Generate embeddings for all documents that don't have them yet.
        
        Args:
            batch_size (int): Number of documents to process in each MongoDB batch
            max_documents (int): Maximum number of documents to process (None for all)
            progress_update_frequency (int): How often to update the progress bar (e.g., every 5 documents)
        
        Returns:
            dict: Statistics about the operation
        """
        # Find all documents without embeddings
        query = {"submission_embeddings": {"$exists": False}}
        
        # Add limit if specified
        if max_documents:
            cursor = self.db[self.submissions_collection_name].find(query).limit(max_documents)
            total_docs = min(max_documents, self.count_documents_without_embeddings())
        else:
            cursor = self.db[self.submissions_collection_name].find(query)
            total_docs = self.count_documents_without_embeddings()
        
        logger.info(f"Starting to generate embeddings for {total_docs} documents in batches of {batch_size}")
        logger.info(f"Progress bar will update every {progress_update_frequency} documents")
        
        # Initialize counters
        processed = 0
        successful = 0
        failed = 0
        batches = 0
        self.tokens_processed = 0
        self.api_requests_made = 0
        
        # Create progress bar with fewer steps for more visible progress
        progress_steps = (total_docs + progress_update_frequency - 1) // progress_update_frequency
        with tqdm(total=progress_steps, desc="Generating embeddings") as pbar:
            # Process documents in batches
            docs_batch = []
            operations = []
            progress_counter = 0
            
            for doc in cursor:
                docs_batch.append(doc)
                progress_counter += 1
                
                # Update progress bar when we reach the update frequency
                if progress_counter >= progress_update_frequency:
                    pbar.update(1)
                    pbar.set_postfix({
                        'docs': processed + len(docs_batch),
                        'successful': successful,
                        'requests': self.api_requests_made,
                        'tokens': self.tokens_processed
                    })
                    progress_counter = 0
                
                # Process batch when it reaches the batch size
                if len(docs_batch) >= batch_size:
                    batch_stats = self._process_batch(docs_batch, operations)
                    successful += batch_stats['successful']
                    failed += batch_stats['failed']
                    processed += batch_stats['processed']
                    batches += 1
                    
                    # Update progress bar postfix without incrementing it
                    pbar.set_postfix({
                        'docs': processed,
                        'successful': successful,
                        'requests': self.api_requests_made,
                        'tokens': self.tokens_processed
                    })
                    
                    # Reset batch
                    docs_batch = []
                    operations = []
            
            # Process remaining documents
            if docs_batch:
                batch_stats = self._process_batch(docs_batch, operations)
                successful += batch_stats['successful']
                failed += batch_stats['failed']
                processed += batch_stats['processed']
                batches += 1
                
                # Update progress bar one last time
                if progress_counter > 0:
                    pbar.update(1)
                pbar.set_postfix({
                    'docs': processed,
                    'successful': successful,
                    'requests': self.api_requests_made,
                    'tokens': self.tokens_processed
                })
        
        # Final statistics
        stats = {
            'total_processed': processed,
            'successful': successful,
            'failed': failed,
            'api_requests': self.api_requests_made,
            'estimated_tokens': self.tokens_processed
        }
        
        logger.info(f"Embedding generation complete: {successful} successful, {failed} failed")
        logger.info(f"API requests made: {self.api_requests_made}")
        logger.info(f"Estimated tokens processed: {self.tokens_processed}")
        
        return stats
    
    def run(self, regenerate_all=False, batch_size=100, max_documents=None, progress_update_frequency=5):
        """
        Run the embeddings generation process.
        
        Args:
            regenerate_all (bool): Whether to regenerate all embeddings or just missing ones
            batch_size (int): Number of documents to process in each batch
            max_documents (int): Maximum number of documents to process (None for all)
            progress_update_frequency (int): How often to update the progress bar (every X documents)
        
        Returns:
            dict: Statistics about the operation
        """
        logger.info("Starting Reddit submission embeddings generation")
        
        # Drop existing embeddings if regenerating all
        if regenerate_all:
            self.drop_submission_embeddings()
        
        # Generate embeddings
        stats = self.generate_embeddings(batch_size, max_documents, progress_update_frequency)
        
        logger.info("Reddit submission embeddings generation complete")
        return stats


if __name__ == "__main__":
    # Initialize the embedder
    embedder = SubredditPrawEmbedder()
    
    # Run the embedder
    stats = embedder.run()
    
    # Print statistics
    print("\nEmbedding Generation Statistics:")
    print(f"Total documents processed: {stats['total_processed']}")
    print(f"Successfully embedded: {stats['successful']}")
    print(f"Failed to embed: {stats['failed']}")
    print(f"API requests made: {stats['api_requests']}")
    print(f"Estimated tokens processed: {stats['estimated_tokens']}")
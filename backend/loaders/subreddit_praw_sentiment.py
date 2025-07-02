import os
from dotenv import load_dotenv
import logging
from tqdm import tqdm
from pymongo import UpdateOne
from loaders.db.mdb import MongoDBConnector
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SubredditPrawSentiment(MongoDBConnector):
    def __init__(self, uri=None, database_name: str = None, appname: str = None, 
                 submissions_collection_name: str = os.getenv("SUBREDDIT_SUBMISSIONS_COLLECTION", "subredditSubmissions")):
        """
        Reddit Subreddit sentiment analysis class.
        Generates sentiment scores for subreddit submissions and stores them in MongoDB.
        """
        # Load environment variables
        load_dotenv()
        
        # Initialize MongoDB connection
        super().__init__(uri, database_name, appname)
        self.submissions_collection_name = submissions_collection_name
        logger.info(f"Using MongoDB collection '{self.submissions_collection_name}' for subreddit submissions")
        
        # Initialize FinBERT model for sentiment analysis
        logger.info("Loading FinBERT model for sentiment analysis...")
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        self.sentiment_pipeline = pipeline("sentiment-analysis", model=self.model, tokenizer=self.tokenizer, top_k=None)
        logger.info("FinBERT model loaded successfully")
        
        # Counters for tracking
        self.api_requests_made = 0
    
    def _prepare_text_from_submission_dict(self, submission_dict):
        """
        Convert submission_dict to a text representation for sentiment analysis.
        Properly annotated to improve model accuracy.
        """
        if not submission_dict:
            return ""
        
        # Format title and selftext with proper annotations
        title = submission_dict.get('title', '')
        selftext = submission_dict.get('selftext', '')
        # Limit selftext to 500 characters as requested
        if len(selftext) > 500:
            selftext = selftext[:500] + "..."
        
        subreddit = submission_dict.get('subreddit', '')
        asset_id = submission_dict.get('asset_id', '')
        
        # Create a well-structured text with clear sections
        text_parts = [
            f"TITLE: {title}",
            f"CONTENT: {selftext}",
            f"SUBREDDIT: {subreddit}",
            f"ASSET: {asset_id}"
        ]
        
        # Add comments if available
        comments = submission_dict.get('comments', [])
        if comments:
            comment_texts = []
            for i, comment in enumerate(comments):
                if i >= 3:  # Only include up to 3 comments
                    break
                author = comment.get('author', '[deleted]')
                # Limit comment body to 150 characters as requested
                body = comment.get('body', '')[:150]
                if len(comment.get('body', '')) > 150:
                    body = body + "..."
                
                # Annotate comments clearly
                comment_text = f"COMMENT {i+1} BY {author}: {body}"
                comment_texts.append(comment_text)
            
            if comment_texts:
                text_parts.append("COMMENTS SECTION:")
                text_parts.extend(comment_texts)
        
        # Join all parts with double newlines for clear separation
        return "\n\n".join(text_parts)
    
    def count_documents_without_sentiment(self):
        """
        Count how many documents need sentiment scores to be generated.
        """
        count = self.db[self.submissions_collection_name].count_documents(
            {"sentiment_score": {"$exists": False}}
        )
        logger.info(f"Found {count} documents without sentiment scores.")
        return count
    
    def drop_sentiment_scores(self):
        """
        Drops the sentiment_score attribute from all documents in the collection.
        Useful for regenerating all sentiment scores.
        """
        logger.info(f"Dropping sentiment_score attribute from all documents in {self.submissions_collection_name}...")
        result = self.db[self.submissions_collection_name].update_many(
            {"sentiment_score": {"$exists": True}},
            {"$unset": {"sentiment_score": ""}}
        )
        logger.info(f"Dropped sentiment_score from {result.modified_count} documents.")
        return result.modified_count
    
    def get_sentiment_scores(self, text):
        """
        Computes the sentiment scores for the given text using FinBERT.
        Returns a dictionary with positive, negative, and neutral scores.
        Handles long context windows by progressively reducing text size if needed.
        """
        if not text or not isinstance(text, str):
            logger.error("Invalid input text for sentiment analysis.")
            return None
        
        original_text = text
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Increment API request counter
                self.api_requests_made += 1
                
                # Get sentiment scores from FinBERT
                results = self.sentiment_pipeline(text)[0]
                
                # Format scores into a dictionary
                scores = {result['label'].lower(): result['score'] for result in results}
                return scores
                
            except Exception as e:
                error_message = str(e).lower()
                
                # Check if error is related to context length
                if "context" in error_message and "length" in error_message:
                    # Reduce text size for next attempt
                    if attempt < max_retries - 1:
                        # For first retry, keep title and reduce content significantly
                        if attempt == 0:
                            # Extract title if possible
                            title_match = None
                            for line in original_text.split('\n'):
                                if line.startswith("TITLE:"):
                                    title_match = line
                                    break
                            
                            # Cut content by 50% but preserve title
                            if title_match:
                                text = title_match + "\n\n" + original_text[:len(original_text)//2]
                            else:
                                text = original_text[:len(original_text)//2]
                            
                            logger.warning(f"Text too long for model context window. Reducing to 50% length and retrying.")
                        
                        # For second retry, be more aggressive
                        elif attempt == 1:
                            # Extract just title and maybe first paragraph
                            text_parts = original_text.split('\n\n')[:2]  # Just first two paragraphs
                            text = '\n\n'.join(text_parts)
                            logger.warning(f"Text still too long. Reducing to just title and first section.")
                        
                        continue  # Try again with shortened text
                
                # For other errors or if we've exhausted retries
                logger.error(f"Error computing sentiment scores (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
        
        # If we reach here, all attempts failed
        return None
    
    def _process_batch(self, docs_batch, operations):
        """
        Process a batch of documents and generate their sentiment scores.
        
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
                
                # Skip if no text to analyze
                if not text:
                    logger.warning(f"Skipping document {doc['_id']} - no text to analyze for sentiment")
                    batch_stats['failed'] += 1
                    batch_stats['processed'] += 1
                    continue
                
                # Get sentiment scores
                sentiment_scores = self.get_sentiment_scores(text)
                
                # Check if sentiment analysis was successful
                if sentiment_scores is None:
                    logger.error(f"Failed to generate sentiment scores for document {doc['_id']}")
                    batch_stats['failed'] += 1
                    batch_stats['processed'] += 1
                    continue
                
                # Create update operation
                operations.append(
                    UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {"sentiment_score": sentiment_scores}}
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
    
    def generate_sentiment_scores(self, batch_size=100, max_documents=None, progress_update_frequency=5):
        """
        Generate sentiment scores for all documents that don't have them yet.
        
        Args:
            batch_size (int): Number of documents to process in each MongoDB batch
            max_documents (int): Maximum number of documents to process (None for all)
            progress_update_frequency (int): How often to update the progress bar (e.g., every 5 documents)
        
        Returns:
            dict: Statistics about the operation
        """
        # Find all documents without sentiment scores
        query = {"sentiment_score": {"$exists": False}}
        
        # Add limit if specified
        if max_documents:
            cursor = self.db[self.submissions_collection_name].find(query).limit(max_documents)
            total_docs = min(max_documents, self.count_documents_without_sentiment())
        else:
            cursor = self.db[self.submissions_collection_name].find(query)
            total_docs = self.count_documents_without_sentiment()
        
        logger.info(f"Starting to generate sentiment scores for {total_docs} documents in batches of {batch_size}")
        logger.info(f"Progress bar will update every {progress_update_frequency} documents")
        
        # Initialize counters
        processed = 0
        successful = 0
        failed = 0
        batches = 0
        self.api_requests_made = 0
        
        # Create progress bar with fewer steps for more visible progress
        progress_steps = (total_docs + progress_update_frequency - 1) // progress_update_frequency
        with tqdm(total=progress_steps, desc="Generating sentiment scores") as pbar:
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
                        'requests': self.api_requests_made
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
                        'requests': self.api_requests_made
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
                    'requests': self.api_requests_made
                })
        
        # Final statistics
        stats = {
            'total_processed': processed,
            'successful': successful,
            'failed': failed,
            'api_requests': self.api_requests_made
        }
        
        logger.info(f"Sentiment analysis complete: {successful} successful, {failed} failed")
        logger.info(f"API requests made: {self.api_requests_made}")
        
        return stats
    
    def run(self, regenerate_all=False, batch_size=100, max_documents=None, progress_update_frequency=5):
        """
        Run the sentiment analysis process.
        
        Args:
            regenerate_all (bool): Whether to regenerate all sentiment scores or just missing ones
            batch_size (int): Number of documents to process in each batch
            max_documents (int): Maximum number of documents to process (None for all)
            progress_update_frequency (int): How often to update the progress bar (every X documents)
        
        Returns:
            dict: Statistics about the operation
        """
        logger.info("Starting Reddit submission sentiment analysis")
        
        # Drop existing sentiment scores if regenerating all
        if regenerate_all:
            self.drop_sentiment_scores()
        
        # Generate sentiment scores
        stats = self.generate_sentiment_scores(batch_size, max_documents, progress_update_frequency)

        logger.info("\nSentiment Analysis Statistics:")
        logger.info(f"Total documents processed: {stats['total_processed']}")
        logger.info(f"Successfully analyzed: {stats['successful']}")
        logger.info(f"Failed to analyze: {stats['failed']}")
        logger.info(f"API requests made: {stats['api_requests']}") 
        
        logger.info("Reddit submission sentiment analysis complete")


if __name__ == "__main__":
    # Initialize the sentiment analyzer
    sentiment_analyzer = SubredditPrawSentiment()
    
    # Run the sentiment analyzer
    sentiment_analyzer.run()
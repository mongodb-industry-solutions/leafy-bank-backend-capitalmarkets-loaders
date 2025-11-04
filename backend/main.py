import logging
from fastapi import FastAPI, Request, APIRouter, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from loader_scheduler import LoaderScheduler
from loader_service import LoaderService
from pydantic import BaseModel, field_validator
from datetime import datetime, timezone
import threading
import asyncio
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

# Global service instances - initialized on startup
loader_service: Optional[LoaderService] = None
scheduler: Optional[LoaderScheduler] = None
scheduler_thread: Optional[threading.Thread] = None
services_initialized = False

@app.get("/")
async def read_root(request: Request):
    return {"message": "Server is running", "services_initialized": services_initialized}

@app.get("/health")
async def health_check():
    """
    Health check endpoint that verifies service status and MongoDB connectivity.
    """
    health_status = {
        "status": "healthy",
        "services_initialized": services_initialized,
        "mongodb_connected": False,
        "scheduler_running": False,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    try:
        # Check if services are initialized
        if not services_initialized or loader_service is None:
            health_status["status"] = "initializing"
            return health_status
        
        # Check MongoDB connectivity
        from loaders.db.mdb import MongoDBConnectionFactory
        try:
            db = MongoDBConnectionFactory.get_database(max_retry_time=5)
            db.client.admin.command('ping')
            health_status["mongodb_connected"] = True
        except Exception as e:
            health_status["status"] = "unhealthy"
            health_status["mongodb_error"] = str(e)
        
        # Check scheduler status
        if scheduler_thread and scheduler_thread.is_alive():
            health_status["scheduler_running"] = True
        else:
            health_status["status"] = "degraded"
            
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["error"] = str(e)
    
    # Set appropriate HTTP status code
    status_code = 200 if health_status["status"] == "healthy" else 503
    
    return health_status

async def initialize_services():
    """
    Initialize services with retry logic. Runs in background to not block startup.
    """
    global loader_service, scheduler, scheduler_thread, services_initialized
    
    max_retries = 60  # Retry for up to 10 minutes (60 * 10 seconds)
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info("Attempting to initialize services...")
            
            # Initialize loader service
            loader_service = LoaderService()
            logger.info("LoaderService initialized successfully")
            
            # Initialize and start scheduler
            scheduler = LoaderScheduler()
            scheduler_thread = threading.Thread(target=scheduler.start, daemon=True)
            scheduler_thread.start()
            logger.info("LoaderScheduler started successfully")
            
            services_initialized = True
            logger.info("All services initialized successfully")
            return
            
        except Exception as e:
            retry_count += 1
            logger.error(f"Failed to initialize services (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                await asyncio.sleep(10)  # Wait 10 seconds before retry
            else:
                logger.critical("Failed to initialize services after maximum retries")
                # Services will remain uninitialized, but app continues running

@app.on_event("startup")
async def startup_event():
    """
    Startup event that initializes services in the background.
    """
    logger.info("Starting application...")
    asyncio.create_task(initialize_services())

@app.on_event("shutdown")
async def shutdown_event():
    """
    Shutdown event that cleans up resources.
    """
    logger.info("Shutting down application...")
    
    # Close MongoDB connections
    try:
        from loaders.db.mdb import MongoDBConnectionFactory
        MongoDBConnectionFactory.close_cached_client()
    except Exception as e:
        logger.error(f"Error during MongoDB cleanup: {e}")
    
    logger.info("Application shutdown complete")

def get_loader_service() -> LoaderService:
    """
    Get the loader service instance with initialization check.
    """
    if not services_initialized or loader_service is None:
        raise HTTPException(
            status_code=503, 
            detail="Service is still initializing. Please try again in a few moments."
        )
    return loader_service

############################
### -- LOADER SERVICE -- ###
############################

class DateRequest(BaseModel):
    date_str: str

    @field_validator('date_str')
    def validate_date_str(cls, value):
        if not value.isdigit() or len(value) != 8:
            raise ValueError("date_str must be in '%Y%m%d' format")

        date = datetime.strptime(value, "%Y%m%d")
        current_date = datetime.now(timezone.utc).strftime("%Y%m%d")

        if value >= current_date:
            raise ValueError(
                "date_str cannot be the current date or a future date")

        return value

class BackfillRequest(BaseModel):
    start_date: str
    end_date: str

    @field_validator('start_date', 'end_date')
    def validate_date(cls, value):
        if not value.isdigit() or len(value) != 8:
            raise ValueError("Dates must be in '%Y%m%d' format")
        return value

class SymbolRequest(DateRequest):
    symbol: str

class SeriesRequest(DateRequest):
    series_id: str

class BackfillSymbolRequest(BackfillRequest):
    symbol: str

class BackfillSeriesRequest(BackfillRequest):
    series_id: str

####################################
# YFINANCE
####################################

@app.post("/load-yfinance-market-data")
async def load_yfinance_market_data(date_str: DateRequest):
    try:
        service = get_loader_service()
        service.load_yfinance_market_data(date_str.date_str)
        return {"message": f"Yahoo Finance market data loading process completed for date {date_str.date_str}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading Yahoo Finance market data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-yfinance-market-data-by-symbol")
async def load_yfinance_market_data_by_symbol(request: SymbolRequest):
    try:
        service = get_loader_service()
        service.load_yfinance_market_data_by_symbol(request.date_str, request.symbol)
        return {"message": f"Yahoo Finance market data loading process completed for symbol {request.symbol} on date {request.date_str}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading Yahoo Finance market data by symbol: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

####################################
# BINANCE API
####################################

@app.post("/load-binance-api-crypto-data")
async def load_binance_api_crypto_data(date_str: DateRequest):
    try:
        service = get_loader_service()
        service.load_binance_api_crypto_data(date_str.date_str)
        return {"message": f"Binance API crypto data loading process completed for date {date_str.date_str}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading Binance API crypto data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-binance-api-crypto-data-by-symbol")
async def load_binance_api_crypto_data_by_symbol(request: SymbolRequest):
    try:
        service = get_loader_service()
        service.load_binance_api_crypto_data_by_symbol(request.date_str, request.symbol)
        return {"message": f"Binance API crypto data loading process completed for symbol {request.symbol} on date {request.date_str}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading Binance API crypto data by symbol: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
####################################
# PYFREDAPI
####################################

@app.post("/load-pyfredapi-macroeconomic-data")
async def load_pyfredapi_macroeconomic_data(date_str: DateRequest):
    try:
        service = get_loader_service()
        service.load_pyfredapi_macroeconomic_data(date_str.date_str)
        return {"message": f"PyFredAPI macroeconomic data loading process completed for date {date_str.date_str}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading PyFredAPI macroeconomic data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-pyfredapi-macroeconomic-data-by-series")
async def load_pyfredapi_macroeconomic_data_by_series(request: SeriesRequest):
    try:
        service = get_loader_service()
        service.load_pyfredapi_macroeconomic_data_by_series(request.date_str, request.series_id)
        return {"message": f"PyFredAPI macroeconomic data loading process completed for series ID {request.series_id} on date {request.date_str}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading PyFredAPI macroeconomic data by series: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
####################################
# COINGECKO STABLECOIN MARKET CAP
####################################

@app.post("/load-coingecko-stablecoin-market-cap-data")
async def load_coingecko_stablecoin_market_cap_data():
    """
    Loads Coingecko stablecoin market cap data for today.
    """
    try:
        msg = loader_service.load_coingecko_stablecoin_market_cap_data()
        return {"message": msg}
    except Exception as e:
        logging.error(f"Error loading Coingecko stablecoin market cap data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
####################################
# PORTFOLIO PERFORMANCE
####################################
    
@app.post("/insert-portfolio-performance-yesterday-data")
async def insert_portfolio_performance_yesterday_data():
    """
    Loads portfolio performance data for yesterday.
    This endpoint doesn't require any input parameters as it works with yesterday's date.
    """
    try:
        result = loader_service.insert_portfolio_performance_yesterday_data()
        if result["status"] == "exists":
            return {"message": "Portfolio performance data for yesterday already exists, no action taken"}
        else:
            return {"message": "Portfolio performance data for yesterday successfully loaded", "data": result}
    except Exception as e:
        logging.error(f"Error loading portfolio performance data for yesterday: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/insert-portfolio-performance-data-for-date")
async def insert_portfolio_performance_data_for_date(date_str: DateRequest):
    """
    Loads portfolio performance data for a specific date.
    """
    try:
        result = loader_service.insert_portfolio_performance_data_for_date(date_str.date_str)
        if result["status"] == "error":
            raise ValueError(result["message"])
        elif result["status"] == "exists":
            return {"message": f"Portfolio performance data for {date_str.date_str} already exists, no action taken"}
        else:
            return {"message": f"Portfolio performance data for {date_str.date_str} successfully loaded", "data": result}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading portfolio performance data for date: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backfill-portfolio-performance-data")
async def backfill_portfolio_performance_data(request: BackfillRequest):
    """
    Backfills portfolio performance data for a specific date range.
    """
    try:
        result = loader_service.backfill_portfolio_performance_data(request.start_date, request.end_date)
        if result["status"] == "error":
            raise ValueError(result["message"])
        return {
            "message": f"Backfill for portfolio performance data completed from {request.start_date} to {request.end_date}",
            "inserted_count": result.get("inserted_count", 0),
            "skipped_count": result.get("skipped_count", 0)
        }
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling portfolio performance data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

####################################
# BACKFILLS
####################################

####################################
# YFINANCE
####################################

@app.post("/backfill-yfinance-market-data")
async def backfill_yfinance_market_data(request: BackfillRequest):
    try:
        service = get_loader_service()
        service.backfill_yfinance_market_data(request.start_date, request.end_date)
        return {"message": f"Backfill for Yahoo Finance market data completed from {request.start_date} to {request.end_date}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling Yahoo Finance market data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backfill-yfinance-market-data-by-symbol")
async def backfill_yfinance_market_data_by_symbol(request: BackfillSymbolRequest):
    try:
        service = get_loader_service()
        service.backfill_yfinance_market_data_by_symbol(request.start_date, request.end_date, request.symbol)
        return {"message": f"Backfill for Yahoo Finance market data for symbol {request.symbol} completed from {request.start_date} to {request.end_date}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling Yahoo Finance market data by symbol: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

####################################
# BINANCE API
####################################

@app.post("/backfill-binance-api-crypto-data")
async def backfill_binance_api_crypto_data(request: BackfillRequest):
    try:
        service = get_loader_service()
        service.backfill_binance_api_crypto_data(request.start_date, request.end_date)
        return {"message": f"Backfill for Binance API crypto data completed from {request.start_date} to {request.end_date}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling Binance API crypto data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/backfill-binance-api-crypto-data-by-symbol")
async def backfill_binance_api_crypto_data_by_symbol(request: BackfillSymbolRequest):
    try:
        service = get_loader_service()
        service.backfill_binance_api_crypto_data_by_symbol(request.start_date, request.end_date, request.symbol)
        return {"message": f"Backfill for Binance API crypto data for symbol {request.symbol} completed from {request.start_date} to {request.end_date}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling Binance API crypto data by symbol: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

####################################
# PYFREDAPI
####################################

@app.post("/backfill-pyfredapi-macroeconomic-data")
async def backfill_pyfredapi_macroeconomic_data(request: BackfillRequest):
    try:
        service = get_loader_service()
        service.backfill_pyfredapi_macroeconomic_data(request.start_date, request.end_date)
        return {"message": f"Backfill for PyFredAPI macroeconomic data completed from {request.start_date} to {request.end_date}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling PyFredAPI macroeconomic data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backfill-pyfredapi-macroeconomic-data-by-series")
async def backfill_pyfredapi_macroeconomic_data_by_series(request: BackfillSeriesRequest):
    try:
        service = get_loader_service()
        service.backfill_pyfredapi_macroeconomic_data_by_series(request.start_date, request.end_date, request.series_id)
        return {"message": f"Backfill for PyFredAPI macroeconomic data for series ID {request.series_id} completed from {request.start_date} to {request.end_date}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling PyFredAPI macroeconomic data by series: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

####################################
# FINANCIAL NEWS
####################################

@app.post("/load-recent-financial-news")
async def load_recent_financial_news():
    try:
        service = get_loader_service()
        service.load_recent_financial_news()
        return {"message": "Financial News processing completed"}
    except Exception as e:
        logging.error(f"Error loading recent financial news: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
####################################
# PRAW WRAPPER PROCESSING
####################################

@app.post("/load-recent-subreddit-praw-data")
async def load_recent_subreddit_praw_data():
    try:
        service = get_loader_service()
        service.load_recent_subreddit_praw_data()
        return {"message": "Subreddit PRAW data processing completed!"}
    except Exception as e:
        logging.error(f"Error loading Subreddit PRAW data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/subreddit-praw-embedder-only")
async def subreddit_praw_embedder_only():
    try:
        service = get_loader_service()
        service.subreddit_praw_embedder_only()
        return {"message": "Subreddit PRAW embedder only process completed!"}
    except Exception as e:
        logging.error(f"Error performing Subreddit PRAW embedder only: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/subreddit-praw-sentiment-only")
async def subreddit_praw_sentiment_only():
    try:
        service = get_loader_service()
        service.subreddit_praw_sentiment_only()
        return {"message": "Subreddit PRAW sentiment analysis process completed!"}
    except Exception as e:
        logging.error(f"Error performing Subreddit PRAW sentiment analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/subreddit-praw-cleaner-only")
async def subreddit_praw_cleaner_only():
    try:
        service = get_loader_service()
        service.subreddit_praw_cleaner_only()
        return {"message": "Subreddit PRAW cleaner only process completed!"}
    except Exception as e:
        logging.error(f"Error performing Subreddit PRAW cleaner only: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

############################
## -- LOADER SCHEDULER -- ##
############################

@app.get("/scheduler-overview")
async def scheduler_overview():
    try:
        if not services_initialized or scheduler is None:
            raise HTTPException(
                status_code=503,
                detail="Scheduler is still initializing. Please try again in a few moments."
            )
        
        # Check NODE_ENV - return early if not production
        import os
        node_env = os.getenv("NODE_ENV", "").lower()
        if node_env != "prod":
            return {
                "overview": {
                    "max_exec": "N/A",
                    "tzinfo": "UTC",
                    "priority_function": "N/A",
                    "jobs": [],
                    "message": f"No jobs scheduled (NODE_ENV={node_env}, jobs only run in 'prod')"
                }
            }
        
        overview = str(scheduler.scheduler)
        overview_lines = overview.split("\n")
        overview_dict = {
            "max_exec": overview_lines[0].split(",")[0].split("=")[1].strip(),
            "tzinfo": overview_lines[0].split(",")[1].split("=")[1].strip(),
            "priority_function": overview_lines[0].split(",")[2].split("=")[1].strip(),
            "jobs": []
        }
        for line in overview_lines[3:]:
            if line.strip() and not line.startswith("--------"):
                parts = line.split()
                if len(parts) >= 8:
                    job = {
                        "type": parts[0],
                        "function": parts[1],
                        "due_at": f"{parts[2]} {parts[3]}",
                        "tzinfo": parts[4],
                        "due_in": parts[5]
                    }
                    # Replace function names
                    if job["function"] == "#et_data_etl(..)":
                        job["function"] = "run_yfinance_market_data_etl"
                    elif job["function"] == "#ic_data_etl(..)":
                        job["function"] = "run_pyfredapi_macroeconomic_data_etl"
                    elif job["function"] == "#_processing(..)":
                        job["function"] = "run_subreddit_praw_data_processing"
                    elif job["function"] == "#terday_data(..)":
                        job["function"] = "run_insert_portfolio_performance_yesterday_data"
                    if job["function"] == "#to_data_etl(..)":
                        job["function"] = "run_binance_api_crypto_data_etl"
                    if job["function"] == "#bedder_only(..)":
                        job["function"] = "run_subreddit_praw_data_embedder_only"
                    if job["function"] == "#timent_only(..)":
                        job["function"] = "run_subreddit_praw_data_sentiment_only"
                    if job["function"] == "#leaner_only(..)":
                        job["function"] = "run_subreddit_praw_data_cleaner_only"
                    if job["function"] == "#_extraction(..)":
                        job["function"] = "run_financial_news_extraction"
                    if job["function"] == "#et_cap_data(..)":
                        job["function"] = "run_coingecko_stablecoin_market_cap_data"

                    # Add "d" to single digit due_in values
                    if job["due_in"].isdigit():
                        job["due_in"] += "d"
                    
                    overview_dict["jobs"].append(job)
                else:
                    job = {
                        "type": parts[0] if len(parts) > 0 else "",
                        "function": parts[1] if len(parts) > 1 else "",
                        "due_at": f"{parts[2]} {parts[3]}" if len(parts) > 3 else "",
                        "tzinfo": parts[4] if len(parts) > 4 else "",
                        "due_in": parts[5] if len(parts) > 5 else ""
                    }
                    # Replace function names
                    if job["function"] == "#et_data_etl(..)":
                        job["function"] = "run_yfinance_market_data_etl"
                    elif job["function"] == "#ic_data_etl(..)":
                        job["function"] = "run_pyfredapi_macroeconomic_data_etl"
                    elif job["function"] == "#_processing(..)":
                        job["function"] = "run_subreddit_praw_data_processing"
                    elif job["function"] == "#terday_data(..)":
                        job["function"] = "run_insert_portfolio_performance_yesterday_data"
                    if job["function"] == "#to_data_etl(..)":
                        job["function"] = "run_binance_api_crypto_data_etl"
                    if job["function"] == "#bedder_only(..)":
                        job["function"] = "run_subreddit_praw_data_embedder_only"
                    if job["function"] == "#timent_only(..)":
                        job["function"] = "run_subreddit_praw_data_sentiment_only"
                    if job["function"] == "#leaner_only(..)":
                        job["function"] = "run_subreddit_praw_data_cleaner_only"
                    if job["function"] == "#_extraction(..)":
                        job["function"] = "run_financial_news_extraction"
                    if job["function"] == "#et_cap_data(..)":
                        job["function"] = "run_coingecko_stablecoin_market_cap_data"
                    
                    # Add "d" to single digit due_in values
                    if job["due_in"].isdigit():
                        job["due_in"] += "d"
                    
                    overview_dict["jobs"].append(job)
        return {"overview": overview_dict}
    except Exception as e:
        logger.error(f"Error generating scheduler overview: {e}")
        return {"error": "Failed to generate scheduler overview"}

# Scheduler initialization moved to startup_event

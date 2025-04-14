import logging
from fastapi import FastAPI, Request, APIRouter, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from loader_scheduler import LoaderScheduler
from loader_service import LoaderService
from pydantic import BaseModel, field_validator
from datetime import datetime, timezone
import threading

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

@app.get("/")
async def read_root(request: Request):
    return {"message": "Server is running"}

############################
### -- LOADER SERVICE -- ###
############################

# Initialize service
loader_service = LoaderService()

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

@app.post("/load-yfinance-market-data")
async def load_yfinance_market_data(date_str: DateRequest):
    try:
        loader_service.load_yfinance_market_data(date_str.date_str)
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
        loader_service.load_yfinance_market_data_by_symbol(request.date_str, request.symbol)
        return {"message": f"Yahoo Finance market data loading process completed for symbol {request.symbol} on date {request.date_str}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading Yahoo Finance market data by symbol: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-pyfredapi-macroeconomic-data")
async def load_pyfredapi_macroeconomic_data(date_str: DateRequest):
    try:
        loader_service.load_pyfredapi_macroeconomic_data(date_str.date_str)
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
        loader_service.load_pyfredapi_macroeconomic_data_by_series(request.date_str, request.series_id)
        return {"message": f"PyFredAPI macroeconomic data loading process completed for series ID {request.series_id} on date {request.date_str}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error loading PyFredAPI macroeconomic data by series: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
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

@app.post("/backfill-yfinance-market-data")
async def backfill_yfinance_market_data(request: BackfillRequest):
    try:
        loader_service.backfill_yfinance_market_data(request.start_date, request.end_date)
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
        loader_service.backfill_yfinance_market_data_by_symbol(request.start_date, request.end_date, request.symbol)
        return {"message": f"Backfill for Yahoo Finance market data for symbol {request.symbol} completed from {request.start_date} to {request.end_date}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling Yahoo Finance market data by symbol: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backfill-pyfredapi-macroeconomic-data")
async def backfill_pyfredapi_macroeconomic_data(request: BackfillRequest):
    try:
        loader_service.backfill_pyfredapi_macroeconomic_data(request.start_date, request.end_date)
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
        loader_service.backfill_pyfredapi_macroeconomic_data_by_series(request.start_date, request.end_date, request.series_id)
        return {"message": f"Backfill for PyFredAPI macroeconomic data for series ID {request.series_id} completed from {request.start_date} to {request.end_date}"}
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logging.error(f"Error backfilling PyFredAPI macroeconomic data by series: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-recent-financial-news")
async def load_recent_financial_news():
    try:
        loader_service.load_recent_financial_news()
        return {"message": "Financial News processing completed"}
    except Exception as e:
        logging.error(f"Error loading recent financial news: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

############################
## -- LOADER SCHEDULER -- ##
############################

@app.post("/scheduler-overview")
async def scheduler_overview():
    try:
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
                        job["function"] = "run_financial_news_processing"
                    if job["function"] == "#terday_data(..)":
                        job["function"] = "run_insert_portfolio_performance_yesterday_data"
                    
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
                        job["function"] = "run_financial_news_processing"
                    if job["function"] == "#terday_data(..)":
                        job["function"] = "run_insert_portfolio_performance_yesterday_data"
                    
                    # Add "d" to single digit due_in values
                    if job["due_in"].isdigit():
                        job["due_in"] += "d"
                    
                    overview_dict["jobs"].append(job)
        return {"overview": overview_dict}
    except Exception as e:
        logger.error(f"Error generating scheduler overview: {e}")
        return {"error": "Failed to generate scheduler overview"}

def start_scheduler():
    scheduler.start()

scheduler = LoaderScheduler()
scheduler_thread = threading.Thread(target=start_scheduler)
scheduler_thread.start()

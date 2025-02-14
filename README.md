# Agentic AI - Capital Markets

This repository hosts the backend for the **Agentic AI - Investment Fund Risk Management** demo. It showcases an AI-powered risk management solution for investment fund managers, integrating **AI Agents with a human-in-the-loop approach**. The AI Agent continuously monitors market risks in real-time and suggests risk mitigation strategies, such as asset rebalancing, based on macroeconomic changes, sentiment analysis, and market volatility. **MongoDB Atlas Vector Search** plays a key role in analyzing historical patterns to generate risk mitigation recommendations.

## What is Agentic AI in Capital Markets?

Agentic AI refers to autonomous AI-powered systems that **continuously analyze market conditions, generate insights, and provide actionable recommendations**. In this use case, the AI Agent operates within an investment fund, ensuring risk-aware portfolio management by assessing:

- **Macroeconomic indicators** (GDP, inflation, unemployment rate)
- **Market sentiment** (X/Twitter, news, social media)
- **Market volatility** (VIX index)
- **Investment mix risks** based on real-time market fluctuations

## Where Does MongoDB Shine?

MongoDB provides a **flexible and scalable data store** to handle the diverse datasets required for investment risk management:

- **Atlas Vector Search** enables similarity searches on historical risk conditions to suggest optimal responses to new market events.
- **Flexible Schema** accommodates structured and unstructured data, including financial reports, social media sentiment, and historical macroeconomic trends.
- **Aggregation Pipelines** facilitate complex queries, allowing efficient calculation of risk exposure, asset allocation changes, and performance analytics.
- **Time Series Collections** help efficiently store and retrieve market data over time.

## High-Level Architecture

[High level architecture diagram here use [google slides](https://docs.google.com/presentation/d/1vo8Y8mBrocJtzvZc_tkVHZTsVW_jGueyUl-BExmVUtI/edit#slide=id.g30c066974c7_0_3536)]

## Key Features

### AI-Driven Risk Management

- Real-time monitoring of **macroeconomic conditions, asset performance, and sentiment data**.
- AI-generated **risk mitigation recommendations** based on historical and real-time analysis.

## Tech Stack

- [MongoDB Atlas](https://www.mongodb.com/atlas/database) for the database
- [FastAPI](https://fastapi.tiangolo.com/) for the backend framework
- [Pydantic](https://pydantic-docs.helpmanual.io/) for documenting FastAPI Swagger schemas
- [SlowApi](https://slowapi.readthedocs.io/en/latest/) for rate limiting
- [Uvicorn](https://www.uvicorn.org/) for ASGI server
- [Poetry](https://python-poetry.org/) for dependency management
- [Docker](https://www.docker.com/) for containerization

## Prerequisites

Before you begin, ensure you have met the following requirements:

- **MongoDB Atlas** account (free tier is sufficient) - [Register Here](https://account.mongodb.com/account/register)
- **Python 3.10 or higher**
- **Poetry** (install via [Poetry's official documentation](https://python-poetry.org/docs/#installation))

## Setup Instructions

### Step 1: Set Up MongoDB Database and Collections

1. Log in to **MongoDB Atlas** and create a database named `agentic_capital_markets`. Ensure the name is reflected in the environment variables.
2. Create the following collections:
   - `market_data`
   - `macroeconomic_data`
   - ...

### Step 2: Add MongoDB User

Follow [MongoDB's guide](https://www.mongodb.com/docs/atlas/security-add-mongodb-users/) to create a user with **readWrite** access to the `agentic_capital_markets` database.

## Configure Environment Variables

Create a `.env` file in the `/backend` directory with the following content:

```bash
MONGODB_URI = "mongodb+srv://<REPLACE_USERNAME>:<REPLACE_PASSWORD>@<REPLACE_CLUSTER_NAME>.mongodb.net/"
ORIGINS=http://localhost:3000
```

## Running the Backend

### Virtual Environment Setup with Poetry

1. Open a terminal in the project root directory.
2. Run the following commands:
   ```bash
   make poetry_start
   make poetry_install
   ```
3. Verify that the `.venv` folder has been generated within the `/backend` directory.

### Start the Backend

To start the backend service, run:

```bash
poetry run uvicorn main:app --host 0.0.0.0 --port 8004
```

> Default port is `8004`, modify the `--port` flag if needed.

## Running with Docker

Run the following command in the root directory:

```bash
make build
```

To remove the container and image:

```bash
make clean
```

## API Documentation

You can access the API documentation by visiting the following URL:

```
http://localhost:<PORT_NUMBER>/docs
```
E.g. `http://localhost:8004/docs`

> **_Note:_** Make sure to replace `<PORT_NUMBER>` with the port number you are using and ensure the backend is running.

## Common errors

- Check that you've created an `.env` file that contains the required environment variables.

## Future tasks

- [ ] Add tests
- [ ] Evaluate SonarQube for code quality
- [ ] Automate the deployment process using GitHub Actions or CodePipeline

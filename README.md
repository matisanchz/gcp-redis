# JABA ETL Redis Function

The project function covers 2 functionalities:

## 1) ETL

An ETL feature that processes all MongoDB collections related to JABA athletes, loading them into the Redis vector database. This Redis database will serve as the medium through which JABA's chatbots (Roster and MediaKit) can perform semantic search operations using RAG.

### The function is divided into three parts:

1. **Data Extraction:** Retrieves data of athlete users and their subtasks, campaigns, and the tasks within them.
2. **Transformation:** Converts this non-relational data into persistable documents.
3. **Load:** Generates embeddings for these documents using GEMINI and loads them into the Redis vector database.

## 2) Change Streams

A feature that processes all Mongo Change Streams event detected in order to maintain the Redis vectorDB updated.

### The function listen 2 Databases:

1. **Users database:** When detect an operation of UPDATE/DELETE/INSERT into 'users' or 'useridentities' collection, it triggers an update into the Redis database.
2. **Campaigns database:** When detect an operation of UPDATE/DELETE/INSERT into 'campaigns', 'tasks' or 'subtasks' collection, it triggers an update into the Redis database.

## Prerequisites

- Python 3.10
- Google Cloud Platform account
- Gemini API key
- Redis URL
- MongoDB URI

## Setup

1. Clone the repository:
```bash
git clone https://github.com/JabaAI/jaba-redis-etl.git
cd jaba-redis-etl
```

2. Create and activate a virtual environment:
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Unix or MacOS:
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:

```bash
# On Windows Powershell:
$env:GEMINI_API_KEY="your_gemini_api_key"
$env:MONGO_URI="your_mongo_uri"
$env:REDIS_URL="your_redis_url"

# On Unix or MacOS:
export GEMINI_API_KEY="your_gemini_api_key"
export MONGO_URI="your_mongo_uri"
export REDIS_URL="your_redis_url"

# Or you can use an .env file. Follow .env.example template
```

## Local Development

To run the service locally:

```bash
functions-framework --target main --port 8080
```

## Endpoints

Once the function is running, 2 endpoints will be available:

1. **/redis-etl**: To invoke the ETL process (Function 1). This process will be invoked once.
2. **/change-streams**: To invoke the permanent Mongo Change Stream function (Function 2). This process will be invoked once too, but it will continue listening permanently.

## Deployment

The service is configured to deploy automatically using Google Cloud Build. The deployment process:

1. Builds a Docker container
2. Pushes the container to Google Container Registry
3. Deploys the Cloud Function with the following configuration:
   - Runtime: Python 3.10
   - Memory: 1GB
   - Timeout: 3600 seconds
   - Region: us-central1
# Token API

A FastAPI application that connects to Neo4j to retrieve token data.

## Setup

1. Ensure you have Python 3.8+ installed
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Configure your `.env` file with Neo4j credentials:
   ```
   NEO4J_URI=neo4j+s://example.databases.neo4j.io:7687
   NEO4J_USERNAME=neo4j
   NEO4J_PASSWORD=yourpassword
   # NEO4J_DATABASE is optional and defaults to None (uses the default database)
   ```

## Running the API

Start the API with:

```
python main.py
```

Or using Uvicorn directly:

```
uvicorn main:app --reload
```

The server will start on http://localhost:8000

## API Endpoints

### Root endpoint

- **URL**: `/`
- **Method**: `GET`
- **Response**: Confirms the API is running

### Get Token Data (Multiple Tokens)

- **URL**: `/tokens`
- **Method**: `POST`
- **Request Body**:
  ```json
  {
    "token_addresses": ["0x123...", "0x456...", "0x789..."]
  }
  ```
- **Response**:
  ```json
  {
    "tokens_data": {
      "0x123...": { "address": "0x123...", "name": "Token1", ... },
      "0x456...": { "address": "0x456...", "name": "Token2", ... },
      ...
    },
    "count": 3
  }
  ```
- **Constraints**: Maximum 25 token addresses allowed

### Get Token Data (Single Token - Legacy Endpoint)

- **URL**: `/token`
- **Method**: `POST`
- **Request Body**:
  ```json
  {
    "token_addresses": ["0x123..."]
  }
  ```
- **Response**: Same as the `/tokens` endpoint

## API Documentation

Interactive API documentation is available at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc 
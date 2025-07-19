# /app/config.py
"""
Configuration settings for the application.
Loads environment variables and provides them throughout the app.
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Neo4j settings
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = None  # Default database

# PostgreSQL settings
POSTGRES_CONNECTION_STRING = os.getenv("POSTGRES_CONNECTION_STRING")

# API Keys
CLANK_PASS = os.getenv("CLANK_PASS")
FARSTORE_PASS = os.getenv("FARSTORE_PASS")
REPUTATION_PASS = os.getenv("REPUTATION_PASS")
FART_PASS = os.getenv("FART_PASS")
NEYNAR_API_KEY = os.getenv("NEYNAR_API_KEY")
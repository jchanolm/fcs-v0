"""
Utility functions for the API.
"""
import logging
import os
import json
from datetime import datetime

# Set up logging
logger = logging.getLogger(__name__)

def clean_query_for_lucene(user_query):
    """
    Clean and escape a user query for Lucene/Atlas search
    
    Args:
        user_query: Raw query from user
        
    Returns:
        Cleaned query string safe for Lucene search
    """
    if not user_query:
        return ""
        
    special_chars = ['/', '\\', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '~', '*', '?', ':', '"']
    cleaned_query = user_query
    
    for char in special_chars:
        cleaned_query = cleaned_query.replace(char, ' ')

    cleaned_query = ' '.join(cleaned_query.split())
    
    return cleaned_query

def save_search_results_to_json(query, results, mongo_count=0):
    """
    Save search results to JSON file for debugging
    
    Args:
        query: Original search query 
        results: Search results to save
        mongo_count: Count of results from MongoDB
    """
    try:
        os.makedirs("data/query_results", exist_ok=True)
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_filename = clean_query_for_lucene(query) or "empty_query"
        json_filename = f"data/query_results/{clean_filename}_{timestamp_str}.json"
        
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump({
                "query": query,
                "timestamp": datetime.now().isoformat(),
                "mongo_count": mongo_count,
                "enriched_mongo_count": len([c for c in results if c.get("source") == "mongo_enriched"]),
                "total_count": len(results),
                "casts": results
            }, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved search results to {json_filename}")
    except Exception as e:
        logger.error(f"Error saving JSON: {str(e)}")
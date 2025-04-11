#!/usr/bin/env python3
"""
FarTrader: High-Intelligence Trading Bot using FCS API
======================================================

This bot analyzes Farcaster data to identify trading opportunities by:
1. Tracking token mentions and sentiment in Farcaster casts
2. Analyzing believer scores for tokens to identify conviction
3. Monitoring frame activity to detect early trends
4. Making trading decisions based on correlation of these signals
"""

import os
import json
import time
import logging
import asyncio
import httpx
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("trading_bot.log")
    ]
)
logger = logging.getLogger("fartrader")

# Load environment variables
load_dotenv()

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")
API_KEY = os.getenv("FARSTORE_API_KEY", "password.lol")
NEYNAR_API_KEY = os.getenv("NEYNAR_API_KEY")
DEX_API_KEY = os.getenv("DEX_API_KEY")  # For DEX trading (like 1inch, 0x, etc.)
WALLET_PRIVATE_KEY = os.getenv("WALLET_PRIVATE_KEY")

# Trading settings
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"
MAX_TRADE_AMOUNT_USD = float(os.getenv("MAX_TRADE_AMOUNT_USD", "100"))
MIN_BELIEVER_SCORE = float(os.getenv("MIN_BELIEVER_SCORE", "50"))
MIN_MENTIONS = int(os.getenv("MIN_MENTIONS", "10"))
SENTIMENT_THRESHOLD = float(os.getenv("SENTIMENT_THRESHOLD", "0.7"))
PROFIT_TAKE_PERCENT = float(os.getenv("PROFIT_TAKE_PERCENT", "20"))
STOP_LOSS_PERCENT = float(os.getenv("STOP_LOSS_PERCENT", "10"))

# DEX Trading settings
DEX_URL = os.getenv("DEX_URL", "https://api.1inch.io/v5.0/8453")  # Default to 1inch on Base
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "300000"))
SLIPPAGE = float(os.getenv("SLIPPAGE", "1"))  # 1%

# State tracking
tracked_tokens = {}  # address -> {data}
token_positions = {}  # address -> {entry_price, amount, timestamp}
token_candidates = {}  # address -> {score, mentions, sentiment, last_update}
blacklisted_tokens = set()  # Tokens we don't want to trade


class FarTrader:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        self.last_scan_time = datetime.now() - timedelta(hours=1)
        self.token_price_cache = {}  # address -> {price, timestamp}
        logger.info("FarTrader initialized")
        
        # Load state if exists
        self._load_state()
    
    def _load_state(self):
        """Load previous state if available"""
        try:
            if os.path.exists("fartrader_state.json"):
                with open("fartrader_state.json", "r") as f:
                    state = json.load(f)
                    global tracked_tokens, token_positions, token_candidates, blacklisted_tokens
                    tracked_tokens = state.get("tracked_tokens", {})
                    token_positions = state.get("token_positions", {})
                    token_candidates = state.get("token_candidates", {})
                    blacklisted_tokens = set(state.get("blacklisted_tokens", []))
                    logger.info(f"Loaded state: {len(tracked_tokens)} tracked, {len(token_positions)} positions")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
    
    def _save_state(self):
        """Save current state to disk"""
        try:
            state = {
                "tracked_tokens": tracked_tokens,
                "token_positions": token_positions,
                "token_candidates": token_candidates,
                "blacklisted_tokens": list(blacklisted_tokens),
                "last_updated": datetime.now().isoformat()
            }
            with open("fartrader_state.json", "w") as f:
                json.dump(state, f, indent=2)
            logger.info("State saved")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    async def _get_token_price(self, address: str) -> Optional[float]:
        """Get current token price in USD"""
        # Use cache if recent
        if address in self.token_price_cache:
            cache_time = self.token_price_cache[address]["timestamp"]
            if (datetime.now() - datetime.fromisoformat(cache_time)).total_seconds() < 300:  # 5 min cache
                return self.token_price_cache[address]["price"]
        
        # TODO: Implement actual price API call (e.g., CoinGecko, DEX price)
        # This is a placeholder - in production you'd call a price API
        try:
            # Simulation for testing
            import random
            price = random.uniform(0.01, 10.0)
            self.token_price_cache[address] = {
                "price": price,
                "timestamp": datetime.now().isoformat()
            }
            return price
        except Exception as e:
            logger.error(f"Error getting price for {address}: {e}")
            return None
    
    async def get_believer_scores(self, addresses: List[str]) -> Dict:
        """Get believer scores for a list of token addresses"""
        try:
            response = await self.client.post(
                f"{API_URL}/token-believer-score",
                json={"token_addresses": addresses}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error fetching believer scores: {response.status_code} - {response.text}")
                return {"fcs_data": []}
        except Exception as e:
            logger.error(f"Failed to get believer scores: {e}")
            return {"fcs_data": []}
    
    async def search_token_mentions(self, token_query: str) -> Dict:
        """Search for token mentions in casts"""
        try:
            response = await self.client.post(
                f"{API_URL}/casts-search-weighted",
                json={"query": token_query}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error searching casts: {response.status_code} - {response.text}")
                return {"casts": [], "total": 0, "metrics": {}}
        except Exception as e:
            logger.error(f"Failed to search casts: {e}")
            return {"casts": [], "total": 0, "metrics": {}}
    
    async def get_miniapp_mentions(self) -> Dict:
        """Get metrics on frame app usage"""
        try:
            response = await self.client.post(
                f"{API_URL}/farstore-miniapp-mentions-counts",
                params={"api_key": API_KEY}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error fetching miniapp data: {response.status_code} - {response.text}")
                return {"data": {"mentions": []}}
        except Exception as e:
            logger.error(f"Failed to get miniapp mentions: {e}")
            return {"data": {"mentions": []}}
    
    async def analyze_token_sentiment(self, token_symbol: str, token_address: str) -> Dict:
        """Comprehensive token analysis combining believer score, mentions, and sentiment"""
        # Step 1: Get believer score
        believer_data = await self.get_believer_scores([token_address])
        believer_score = 0
        holder_count = 0
        avg_social_score = 0
        
        for token in believer_data.get("fcs_data", []):
            if token.get("address", "").lower() == token_address.lower():
                believer_score = token.get("believerScore", 0)
                holder_count = token.get("holderCount", 0)
                avg_social_score = token.get("avgSocialCredScore", 0) or 0
                break
        
        # Step 2: Get token mentions and sentiment
        mentions_data = await self.search_token_mentions(token_symbol)
        casts = mentions_data.get("casts", [])
        metrics = mentions_data.get("metrics", {})
        
        # Calculate weighted score
        weighted_score = metrics.get("weighted_score", 0)
        unique_authors = metrics.get("uniqueAuthors", 0)
        mention_count = len(casts)
        
        # Step 3: Simple sentiment analysis (could be more sophisticated in production)
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        influencer_positive = 0  # Positive mentions from high cred score users
        
        positive_keywords = ["bullish", "moon", "going up", "buy", "undervalued", "gem"]
        negative_keywords = ["bearish", "dump", "sell", "overvalued", "scam", "rugpull"]
        
        for cast in casts:
            text = cast.get("text", "").lower()
            cred_score = cast.get("author_farcaster_cred_score", 0) or 0
            
            # Simple keyword matching
            is_positive = any(kw in text for kw in positive_keywords)
            is_negative = any(kw in text for kw in negative_keywords)
            
            if is_positive and not is_negative:
                positive_count += 1
                if cred_score > 0.7:  # High credibility author
                    influencer_positive += 1
            elif is_negative and not is_positive:
                negative_count += 1
            else:
                neutral_count += 1
        
        # Calculate sentiment score (-1 to 1)
        total_sentiment = positive_count - negative_count
        sentiment_score = total_sentiment / max(mention_count, 1)
        
        # Step 4: Get current price
        current_price = await self._get_token_price(token_address)
        
        return {
            "token_address": token_address,
            "token_symbol": token_symbol,
            "believer_score": believer_score,
            "holder_count": holder_count,
            "avg_social_score": avg_social_score,
            "mention_count": mention_count,
            "unique_authors": unique_authors,
            "weighted_score": weighted_score,
            "sentiment_score": sentiment_score,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "neutral_count": neutral_count,
            "influencer_positive": influencer_positive,
            "current_price": current_price,
            "timestamp": datetime.now().isoformat()
        }
    
    async def identify_trending_tokens(self) -> List[Dict]:
        """Identify trending tokens based on cast activity"""
        # Let's find tokens mentioned in casts
        trending_queries = ["$", "#", "token", "coin"]
        potential_tokens = set()
        
        for query in trending_queries:
            data = await self.search_token_mentions(query)
            casts = data.get("casts", [])
            
            for cast in casts:
                text = cast.get("text", "")
                # Look for cashtags ($TOKEN) or potential token mentions
                # This is a simple regex but you'd use NLP in production
                import re
                cashtags = re.findall(r'\$([A-Za-z0-9]+)', text)
                hashtags = re.findall(r'#([A-Za-z0-9]+)', text) 
                
                for tag in cashtags + hashtags:
                    if len(tag) > 2 and tag.isalnum():  # Basic filter
                        potential_tokens.add(tag.upper())
        
        logger.info(f"Found {len(potential_tokens)} potential token symbols")
        
        # For each potential token, get an analysis
        # In production, you'd need to map symbols to addresses
        token_analyses = []
        
        # This is a simulation - in production you'd use a token database
        # to map symbols to addresses
        for token in list(potential_tokens)[:10]:  # Limit to top 10 for demo
            # Generate a fake address for this demo
            fake_address = f"0x{token.lower()}{token.lower()}{'0' * (40 - 2*len(token))}"
            
            # Only analyze tokens we haven't analyzed recently
            if fake_address in token_candidates:
                last_update = datetime.fromisoformat(token_candidates[fake_address]["last_update"])
                if (datetime.now() - last_update).total_seconds() < 3600:  # 1 hour cache
                    continue
            
            analysis = await self.analyze_token_sentiment(token, fake_address)
            if analysis["mention_count"] > MIN_MENTIONS:
                token_analyses.append(analysis)
                
                # Update token candidates
                token_candidates[fake_address] = {
                    "symbol": token,
                    "sentiment": analysis["sentiment_score"],
                    "mentions": analysis["mention_count"],
                    "believer_score": analysis["believer_score"],
                    "weighted_score": analysis["weighted_score"],
                    "last_update": datetime.now().isoformat()
                }
        
        # Sort by a combined score of mentions and sentiment
        token_analyses.sort(key=lambda x: (x["mention_count"] * (x["sentiment_score"] + 1) * x["believer_score"]), reverse=True)
        
        return token_analyses
    
    async def execute_trade(self, token_address: str, side: str, amount_usd: float) -> bool:
        """Execute a trade through DEX"""
        if not TRADING_ENABLED:
            logger.info(f"Would {side} {amount_usd} USD of {token_address} (trading disabled)")
            return True
        
        try:
            # This is a placeholder - would connect to DEX API
            logger.info(f"Executing {side} for {amount_usd} USD of {token_address}")
            
            # Simulate trade
            success = True  # In production, check actual trade result
            
            if success:
                # Update positions
                if side == "buy":
                    price = await self._get_token_price(token_address)
                    token_amount = amount_usd / price if price else 0
                    
                    token_positions[token_address] = {
                        "entry_price": price,
                        "amount": token_amount,
                        "value_usd": amount_usd,
                        "timestamp": datetime.now().isoformat()
                    }
                    logger.info(f"Opened position: {token_amount} tokens at ${price}")
                    
                elif side == "sell" and token_address in token_positions:
                    price = await self._get_token_price(token_address)
                    position = token_positions[token_address]
                    profit_loss = ((price / position["entry_price"]) - 1) * 100
                    
                    logger.info(f"Closed position with P/L: {profit_loss:.2f}%")
                    del token_positions[token_address]
                
                self._save_state()
                return True
            return False
        except Exception as e:
            logger.error(f"Trade execution error: {e}")
            return False
    
    async def manage_positions(self):
        """Check existing positions and manage them (take profit/stop loss)"""
        for address, position in list(token_positions.items()):
            current_price = await self._get_token_price(address)
            if not current_price:
                continue
                
            entry_price = position["entry_price"]
            profit_loss_pct = ((current_price / entry_price) - 1) * 100
            
            # Take profit
            if profit_loss_pct >= PROFIT_TAKE_PERCENT:
                logger.info(f"Taking profit on {address}: {profit_loss_pct:.2f}%")
                await self.execute_trade(address, "sell", position["value_usd"])
            
            # Stop loss
            elif profit_loss_pct <= -STOP_LOSS_PERCENT:
                logger.info(f"Stop loss triggered on {address}: {profit_loss_pct:.2f}%")
                await self.execute_trade(address, "sell", position["value_usd"])
    
    async def trading_logic(self):
        """Main trading logic"""
        # Step 1: Manage existing positions
        await self.manage_positions()
        
        # Step 2: Find new trading opportunities
        trending_tokens = await self.identify_trending_tokens()
        
        # Step 3: Evaluate and open new positions
        for token in trending_tokens:
            address = token["token_address"]
            
            # Skip if already in a position or blacklisted
            if address in token_positions or address in blacklisted_tokens:
                continue
            
            # Trading criteria
            if (token["believer_score"] >= MIN_BELIEVER_SCORE and
                token["sentiment_score"] >= SENTIMENT_THRESHOLD and
                token["mention_count"] >= MIN_MENTIONS):
                
                # Calculate allocation based on conviction
                conviction = (token["sentiment_score"] + token["believer_score"]/100)/2
                allocation = MAX_TRADE_AMOUNT_USD * min(conviction, 1.0)
                
                logger.info(f"Opening position for {token['token_symbol']} with ${allocation:.2f} allocation")
                await self.execute_trade(address, "buy", allocation)
                
                # Track this token
                tracked_tokens[address] = token
                self._save_state()
    
    async def run(self):
        """Main bot loop"""
        logger.info("Starting FarTrader bot")
        
        while True:
            try:
                start_time = time.time()
                logger.info("Running trading cycle")
                
                await self.trading_logic()
                
                duration = time.time() - start_time
                logger.info(f"Trading cycle completed in {duration:.2f} seconds")
                
                # Sleep for a reasonable interval
                await asyncio.sleep(max(60, 300 - duration))  # 5 minutes minus processing time
                
            except Exception as e:
                logger.error(f"Error in trading cycle: {e}")
                await asyncio.sleep(60)  # Wait a minute and retry


async def main():
    """Entry point for the bot"""
    bot = FarTrader()
    await bot.run()


if __name__ == "__main__":
    # Run the bot
    asyncio.run(main()) 
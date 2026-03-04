from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Kalshi (only needed for trading — market data is public)
    kalshi_api_key_id: str = ""
    kalshi_private_key_path: str = ""

    # Anthropic (Phase 2 matching)
    anthropic_api_key: str = ""

    # Database
    database_url: str = "sqlite+aiosqlite:///./arb_scanner.db"

    # API URLs
    kalshi_base_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    gamma_base_url: str = "https://gamma-api.polymarket.com"

    # Matching
    use_claude_matching: bool = True  # Set to true to use Claude API for matching (costs credits)

    # Polling intervals
    price_poll_seconds: int = 30
    market_poll_seconds: int = 300  # 5 minutes

    class Config:
        env_file = ".env"


settings = Settings()

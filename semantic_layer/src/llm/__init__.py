"""
LLM provider package.

Unified interface over OpenAI, Anthropic, Google Gemini, and Ollama (local).
All prompts are external .txt files — editable without code changes.
Supports runtime provider/model switching from the Streamlit UI.
"""

from src.llm.provider import (
    LLMProvider,
    LLMError,
    LLMTimeoutError,
    LLMParseError,
    LLMRateLimitError,
)
from src.llm.response_parser import (
    parse_intent,
    parse_query,
    parse_json_safe,
)

__all__ = [
    "LLMProvider",
    "LLMError",
    "LLMTimeoutError",
    "LLMParseError",
    "LLMRateLimitError",
    "parse_intent",
    "parse_query",
    "parse_json_safe",
]

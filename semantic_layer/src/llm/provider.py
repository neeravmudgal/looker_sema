"""
Unified LLM provider interface for the GraphRAG Semantic Layer.

WHAT: A single class (LLMProvider) that wraps OpenAI, Anthropic, and Google
      Gemini behind a common API. Callers never import vendor SDKs directly;
      they call LLMProvider.complete() or LLMProvider.complete_json() and get
      back plain strings or parsed dicts.

WHY:  1. The Streamlit UI lets users switch models at runtime. Without an
         abstraction layer, every call-site would need if/elif/else chains.
      2. Structured JSON output is handled differently by each vendor
         (OpenAI uses response_format, Anthropic needs a system-prompt nudge,
         Gemini uses response_mime_type). This class hides those differences.
      3. Rate-limit retries, token tracking, and timeout handling are
         cross-cutting concerns that belong in one place, not scattered
         across the codebase.

WHO CALLS THIS:
    - IntentExtractor  (extracts structured intent from user questions)
    - QueryGenerator   (turns intent into Looker Explore query JSON)
    - ClarificationEngine (asks follow-up questions when the query is ambiguous)
    - ExplanationEngine   (explains how a query was answered in plain English)
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------
# We define narrow exception types so callers can decide how to react.
# For example, the Streamlit UI might show "LLM timed out — try again" for
# LLMTimeoutError but "Unexpected response format" for LLMParseError.
# ---------------------------------------------------------------------------

class LLMError(Exception):
    """Base class for all LLM-related errors."""


class LLMTimeoutError(LLMError):
    """Raised when the LLM call exceeds the allowed wall-clock time."""


class LLMParseError(LLMError):
    """Raised when the LLM returns text that cannot be parsed as JSON."""


class LLMRateLimitError(LLMError):
    """Raised when retries for rate-limit errors are exhausted."""


# ---------------------------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------------------------
# Three retries with exponential backoff: 1 s, 2 s, 4 s.
# This is intentionally conservative — we do NOT want to hammer a provider
# that is already rate-limiting us.
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3
_BACKOFF_SECONDS = [1, 2, 4]


class LLMProvider:
    """
    Unified interface over OpenAI, Anthropic, and Google Gemini.

    Usage
    -----
    >>> llm = LLMProvider(provider="openai", model="gpt-4o-mini", api_key="sk-...")
    >>> answer = llm.complete(system="You are helpful.", user="What is 2+2?")
    >>> parsed = llm.complete_json(system="Return JSON.", user="List 3 colors.")

    Parameters
    ----------
    provider : str
        One of "openai", "anthropic", or "google".
    model : str
        The model identifier (e.g. "gpt-4o-mini", "claude-sonnet-4-20250514",
        "gemini-2.0-flash").
    api_key : str, optional
        The API key. If omitted, falls back to the corresponding env var
        (OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY) via each SDK's
        default behaviour.
    """

    # We accept these exact strings. Anything else is a caller bug.
    # "ollama" uses the OpenAI-compatible API that Ollama exposes on localhost.
    _VALID_PROVIDERS = {"openai", "anthropic", "google", "ollama"}

    def __init__(
        self,
        provider: str,
        model: str,
        api_key: Optional[str] = None,
    ) -> None:
        if provider not in self._VALID_PROVIDERS:
            raise ValueError(
                f"Unknown provider '{provider}'. "
                f"Choose from: {', '.join(sorted(self._VALID_PROVIDERS))}"
            )

        self.provider = provider
        self.model = model

        # --- Token tracking ---
        # We accumulate tokens across calls so the UI can display a running
        # total (useful for cost estimation during a single user session).
        self._input_tokens: int = 0
        self._output_tokens: int = 0

        # --- Prompt log ---
        # Stores every (system, user, response) tuple so the UI can display
        # exactly what went to the LLM at each step. List of dicts.
        self._prompt_log: list = []

        # --- Lazy client initialisation ---
        # We create the vendor client once and reuse it. Each vendor SDK
        # manages its own connection pool internally.
        self._client: Any = None
        self._init_client(api_key)

    # ------------------------------------------------------------------
    # Client initialisation (one-time, per provider)
    # ------------------------------------------------------------------

    def _init_client(self, api_key: Optional[str]) -> None:
        """
        Create the vendor-specific SDK client.

        WHY lazy-ish: We do this in __init__ (not truly lazy) because we
        want to fail fast if the SDK is not installed or the key is invalid,
        rather than failing on the first .complete() call minutes later.
        """
        if self.provider == "openai":
            try:
                from openai import OpenAI
            except ImportError as exc:
                raise ImportError(
                    "Install the openai package: pip install openai"
                ) from exc
            kwargs: dict[str, Any] = {}
            if api_key:
                kwargs["api_key"] = api_key
            self._client = OpenAI(**kwargs)

        elif self.provider == "anthropic":
            try:
                from anthropic import Anthropic
            except ImportError as exc:
                raise ImportError(
                    "Install the anthropic package: pip install anthropic"
                ) from exc
            kwargs = {}
            if api_key:
                kwargs["api_key"] = api_key
            self._client = Anthropic(**kwargs)

        elif self.provider == "google":
            try:
                from google import genai
            except ImportError as exc:
                raise ImportError(
                    "Install the google-genai package: pip install google-genai"
                ) from exc
            kwargs = {}
            if api_key:
                kwargs["api_key"] = api_key
            self._client = genai.Client(**kwargs)

        elif self.provider == "ollama":
            # Ollama exposes an OpenAI-compatible API at http://localhost:11434/v1
            # We reuse the OpenAI SDK pointed at the local endpoint. No API key needed.
            try:
                from openai import OpenAI
            except ImportError as exc:
                raise ImportError(
                    "Install the openai package: pip install openai"
                ) from exc

            from src.config import settings
            base_url = getattr(settings, "ollama_base_url", "http://localhost:11434/v1")
            self._client = OpenAI(base_url=base_url, api_key="ollama")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def complete(
        self,
        system: str,
        user: str,
        temperature: float = 0.0,
        max_tokens: int = 2000,
    ) -> str:
        """
        Send a system + user message pair and return the assistant's text.

        This is the workhorse method. Every prompt in the system flows
        through here.

        Parameters
        ----------
        system : str
            The system prompt that sets the LLM's role and constraints.
        user : str
            The user's message (the actual question or filled-in template).
        temperature : float
            0.0 = deterministic, 1.0 = creative. We default to 0.0 because
            structured query generation needs reproducibility.
        max_tokens : int
            Upper bound on the response length.

        Returns
        -------
        str
            The raw text response from the model.

        Raises
        ------
        LLMTimeoutError
            If the request times out.
        LLMRateLimitError
            If rate-limit retries are exhausted.
        LLMError
            For any other unexpected error from the vendor SDK.
        """
        return self._call_with_retry(
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens,
            json_mode=False,
        )

    def complete_json(
        self,
        system: str,
        user: str,
        temperature: float = 0.0,
        max_tokens: int = 2000,
    ) -> dict:
        """
        Like complete(), but forces JSON output and returns a parsed dict.

        HOW each provider enforces JSON:
        - OpenAI:    response_format={"type": "json_object"}
        - Anthropic: We append "Return ONLY valid JSON." to the system prompt.
                     Claude respects this reliably at temperature 0.
        - Google:    generation_config with response_mime_type="application/json"

        Returns
        -------
        dict
            The parsed JSON response.

        Raises
        ------
        LLMParseError
            If the model returns text that is not valid JSON.
        """
        raw = self._call_with_retry(
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens,
            json_mode=True,
        )

        # --- Parse the JSON ------------------------------------------------
        # Even with JSON mode enabled, models occasionally wrap the JSON in
        # markdown code fences (```json ... ```). We strip those defensively.
        try:
            return _extract_json(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            raise LLMParseError(
                f"LLM returned invalid JSON.\n"
                f"Provider: {self.provider}, Model: {self.model}\n"
                f"Raw response (first 500 chars): {raw[:500]}"
            ) from exc

    def get_token_summary(self) -> dict:
        """
        Return accumulated token counts for the current session.

        WHY: The Streamlit sidebar shows a running token counter so users
        can estimate cost. This method provides the data.

        Returns
        -------
        dict
            {"input_tokens": int, "output_tokens": int, "total_tokens": int}
        """
        return {
            "input_tokens": self._input_tokens,
            "output_tokens": self._output_tokens,
            "total_tokens": self._input_tokens + self._output_tokens,
        }

    # ------------------------------------------------------------------
    # Internal: retry wrapper
    # ------------------------------------------------------------------

    def _call_with_retry(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        """
        Execute the vendor-specific call with exponential-backoff retries.

        WHY a retry loop here (instead of a generic retry decorator):
        We need to distinguish rate-limit errors (retryable) from timeouts
        (raise immediately) and other errors (raise immediately). Each
        vendor SDK uses different exception classes, so we handle them
        inline rather than trying to unify exception types at a higher level.
        """
        last_exception: Optional[Exception] = None

        for attempt in range(_MAX_RETRIES):
            try:
                return self._dispatch(
                    system=system,
                    user=user,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    json_mode=json_mode,
                )
            except Exception as exc:
                # --- Timeout: raise immediately, no retry ----------------
                if _is_timeout(exc, self.provider):
                    raise LLMTimeoutError(
                        f"Request to {self.provider}/{self.model} timed out."
                    ) from exc

                # --- Rate limit: back off and retry ----------------------
                if _is_rate_limit(exc, self.provider):
                    last_exception = exc
                    wait = _BACKOFF_SECONDS[attempt]
                    logger.warning(
                        "Rate-limited by %s (attempt %d/%d). "
                        "Sleeping %ds before retry.",
                        self.provider,
                        attempt + 1,
                        _MAX_RETRIES,
                        wait,
                    )
                    time.sleep(wait)
                    continue

                # --- Everything else: raise immediately ------------------
                raise LLMError(
                    f"Unexpected error from {self.provider}/{self.model}: {exc}"
                ) from exc

        # If we exhausted all retries, raise a clear error.
        raise LLMRateLimitError(
            f"Rate-limit retries exhausted for {self.provider}/{self.model} "
            f"after {_MAX_RETRIES} attempts."
        ) from last_exception

    # ------------------------------------------------------------------
    # Internal: per-provider dispatch
    # ------------------------------------------------------------------

    def get_prompt_log(self) -> list:
        """
        Return the full log of all LLM calls made this session.

        Each entry is a dict:
        {
            "call_number": int,
            "system_prompt": str,
            "user_prompt": str,
            "raw_response": str,
            "json_mode": bool,
        }

        WHY: Debugging query generation requires seeing exactly what the
        LLM received and returned. The Streamlit UI renders these in
        collapsible expanders under each pipeline stage.
        """
        return list(self._prompt_log)

    def clear_prompt_log(self) -> None:
        """Clear the prompt log (called at the start of each turn)."""
        self._prompt_log.clear()

    def _dispatch(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        """
        Route to the correct vendor SDK and return raw text.

        Each branch also updates the token counters. Vendor SDKs report
        usage in slightly different shapes; we normalise here.
        """
        if self.provider == "openai":
            raw = self._call_openai(system, user, temperature, max_tokens, json_mode)
        elif self.provider == "anthropic":
            raw = self._call_anthropic(system, user, temperature, max_tokens, json_mode)
        elif self.provider == "google":
            raw = self._call_google(system, user, temperature, max_tokens, json_mode)
        elif self.provider == "ollama":
            raw = self._call_ollama(system, user, temperature, max_tokens, json_mode)
        else:
            # Defensive — should never happen because __init__ validates.
            raise LLMError(f"Unknown provider: {self.provider}")

        # Log the full prompt + response for UI debugging
        self._prompt_log.append({
            "call_number": len(self._prompt_log) + 1,
            "system_prompt": system,
            "user_prompt": user,
            "raw_response": raw,
            "json_mode": json_mode,
        })

        return raw

    # ---- OpenAI --------------------------------------------------------

    def _call_openai(
        self,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        """
        Call the OpenAI Chat Completions API.

        JSON mode: OpenAI natively supports response_format={"type":"json_object"}.
        We just flip that flag; no system-prompt hacking needed.
        """
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = self._client.chat.completions.create(**kwargs)

        # --- Track tokens ---
        if response.usage:
            self._input_tokens += response.usage.prompt_tokens or 0
            self._output_tokens += response.usage.completion_tokens or 0

        return response.choices[0].message.content or ""

    # ---- Anthropic -----------------------------------------------------

    def _call_anthropic(
        self,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        """
        Call the Anthropic Messages API.

        JSON mode: Anthropic does not have a native JSON-mode flag.
        Instead, we append an explicit instruction to the system prompt.
        At temperature 0 this is extremely reliable.
        """
        effective_system = system
        if json_mode:
            effective_system = system.rstrip() + "\n\nReturn ONLY valid JSON."

        response = self._client.messages.create(
            model=self.model,
            system=effective_system,
            messages=[{"role": "user", "content": user}],
            temperature=temperature,
            max_tokens=max_tokens,
        )

        # --- Track tokens ---
        if response.usage:
            self._input_tokens += response.usage.input_tokens or 0
            self._output_tokens += response.usage.output_tokens or 0

        # Anthropic returns a list of content blocks. For text responses
        # there is exactly one block of type "text".
        return response.content[0].text if response.content else ""

    # ---- Google Gemini -------------------------------------------------

    def _call_google(
        self,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        """
        Call the Google Gemini API via the google-genai SDK.

        JSON mode: Gemini supports response_mime_type="application/json"
        inside GenerateContentConfig.
        """
        from google.genai import types

        config_kwargs: dict[str, Any] = {
            "system_instruction": system,
            "temperature": temperature,
            "max_output_tokens": max_tokens,
        }
        if json_mode:
            config_kwargs["response_mime_type"] = "application/json"

        config = types.GenerateContentConfig(**config_kwargs)

        response = self._client.models.generate_content(
            model=self.model,
            contents=user,
            config=config,
        )

        # --- Track tokens ---
        if response.usage_metadata:
            self._input_tokens += response.usage_metadata.prompt_token_count or 0
            self._output_tokens += response.usage_metadata.candidates_token_count or 0

        return response.text or ""

    # ---- Ollama (local, OpenAI-compatible API) -------------------------

    def _call_ollama(
        self,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        """
        Call a local Ollama model via its OpenAI-compatible endpoint.

        WHY Ollama: Runs entirely on your machine — zero API costs, no data
        leaves your network. Great for development and for enterprises that
        can't send data to external APIs.

        SETUP: Install Ollama (https://ollama.com), then:
            ollama pull llama3.2          # or any model you want
            ollama pull nomic-embed-text  # for embeddings (optional)

        The OpenAI SDK talks to Ollama's /v1 endpoint seamlessly.

        JSON mode: Ollama supports response_format={"type":"json_object"}
        for models that support it (llama3+, mistral, etc.)
        """
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = self._client.chat.completions.create(**kwargs)

        # Track tokens (Ollama reports these when available)
        if response.usage:
            self._input_tokens += response.usage.prompt_tokens or 0
            self._output_tokens += response.usage.completion_tokens or 0

        return response.choices[0].message.content or ""


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------
# These are plain functions (not methods) because they are stateless utilities
# that don't need access to the LLMProvider instance.
# ---------------------------------------------------------------------------


def _is_rate_limit(exc: Exception, provider: str) -> bool:
    """
    Detect whether an exception represents a rate-limit (HTTP 429) error.

    WHY separate function: Each vendor SDK raises a different exception
    class for 429s. Centralising the detection here keeps the retry loop
    clean and makes it easy to add new providers later.
    """
    exc_type_name = type(exc).__name__

    if provider in ("openai", "ollama"):
        # openai.RateLimitError is raised for HTTP 429
        # (Ollama uses the same OpenAI SDK, same exception classes)
        return exc_type_name == "RateLimitError"

    elif provider == "anthropic":
        # anthropic.RateLimitError for HTTP 429
        return exc_type_name == "RateLimitError"

    elif provider == "google":
        # google-genai raises google.api_core.exceptions.ResourceExhausted
        # or a generic ClientError with status 429
        if exc_type_name in ("ResourceExhausted", "TooManyRequests"):
            return True
        # Fallback: check the string representation for "429"
        return "429" in str(exc)

    return False


def _is_timeout(exc: Exception, provider: str) -> bool:
    """
    Detect whether an exception represents a timeout.

    We raise LLMTimeoutError immediately (no retry) because a timeout
    usually means the prompt is too large or the model is overloaded.
    Retrying would just waste more time.
    """
    exc_type_name = type(exc).__name__

    if provider in ("openai", "ollama"):
        # openai.APITimeoutError (Ollama reuses same SDK)
        return exc_type_name == "APITimeoutError"

    elif provider == "anthropic":
        # anthropic.APITimeoutError
        return exc_type_name == "APITimeoutError"

    elif provider == "google":
        # google.api_core.exceptions.DeadlineExceeded
        if exc_type_name == "DeadlineExceeded":
            return True
        return "timeout" in str(exc).lower()

    return False


def _extract_json(raw: str) -> dict:
    """
    Parse a JSON dict from a string, stripping common LLM artifacts.

    Handles two common LLM output patterns:
    1. <think>...</think> reasoning blocks emitted by qwen3, deepseek-r1,
       and other chain-of-thought models before the actual JSON response.
    2. Markdown code fences (```json ... ```) that models sometimes add
       around JSON output.

    Raises
    ------
    json.JSONDecodeError
        If the text does not contain valid JSON.
    ValueError
        If the parsed result is not a dict.
    """
    import re

    text = raw.strip()

    # Strip <think>...</think> blocks (qwen3, deepseek-r1, etc.)
    # These models emit a reasoning trace before the JSON answer.
    # The block can span multiple lines, so re.DOTALL is required.
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        # Remove the opening fence (optionally with a language tag)
        first_newline = text.index("\n")
        text = text[first_newline + 1:]
        # Remove the closing fence
        if text.endswith("```"):
            text = text[:-3].rstrip()

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"Expected a JSON object (dict), got {type(parsed).__name__}"
        )
    return parsed

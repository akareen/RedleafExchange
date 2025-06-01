# exchange_client.py
import os
import json
import logging
from typing import Optional, Dict, Any
import requests
from requests import Session, Response, HTTPError, RequestException
from dotenv import load_dotenv

from utils.logging import setup as setup_logging

# ────────────────────────────────────────────────────────────────────
# Load environment variables from `.env` if present
load_dotenv()

setup_logging()
logger = logging.getLogger("ExchangeClient")

# ────────────────────────────────────────────────────────────────────
class ExchangeClientError(Exception):
    """
    Base exception for errors raised by ExchangeClient.
    """
    pass


class ValidationError(ExchangeClientError):
    """
    Raised when the Exchange API returns a 422 (validation or unknown instrument).
    """
    def __init__(self, message: str, details: Any = None):
        super().__init__(message)
        self.details = details


class AuthenticationError(ExchangeClientError):
    """
    Raised when invalid credentials are supplied.
    """
    pass


class HTTPRequestError(ExchangeClientError):
    """
    Raised for non‐422 HTTP errors (4xx other than 422, 5xx).
    """
    def __init__(self, status_code: int, message: str):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code


# ────────────────────────────────────────────────────────────────────
class ExchangeClientConfig:
    """
    Configuration for ExchangeClient. Reads default values from environment variables.

    Environment variables:
      - API_URL:     Base URL of the Exchange API (e.g. "http://localhost:8000")
      - PARTY_ID:    Default trading party_id (integer)
      - PASSWORD:    Default password for the party

    Example `.env`:
        API_URL=http://localhost:8000
        PARTY_ID=1
        PASSWORD=pw
    """
    def __init__(
        self,
        api_url: Optional[str] = None,
        default_party_id: Optional[int] = None,
        default_password: Optional[str] = None
    ):
        env_api = os.getenv("API_URL")
        env_pid = os.getenv("PARTY_ID")
        env_pwd = os.getenv("PASSWORD")

        if api_url is not None:
            self.api_url = api_url.rstrip("/")
        elif env_api:
            self.api_url = env_api.rstrip("/")
        else:
            raise ExchangeClientError("API_URL must be set (either as param or in environment).")

        if default_party_id is not None:
            self.default_party_id = default_party_id
        elif env_pid:
            try:
                self.default_party_id = int(env_pid)
            except ValueError:
                raise ExchangeClientError("PARTY_ID environment variable must be an integer.")
        else:
            raise ExchangeClientError("PARTY_ID must be set (either as param or in environment).")

        if default_password is not None:
            self.default_password = default_password
        elif env_pwd:
            self.default_password = env_pwd
        else:
            raise ExchangeClientError("PASSWORD must be set (either as param or in environment).")


# ────────────────────────────────────────────────────────────────────
class ExchangeClient:
    """
    A thread‐safe Exchange client that wraps the public endpoints:
      * POST /new_book
      * POST /orders
      * POST /cancel

    Each method returns the parsed JSON on success or raises an
    ExchangeClientError‐derived exception on failure.

    Example usage:
        from public_endpoints import ExchangeClient, ExchangeClientConfig, ExchangeClientError

        # Option 1: Rely on environment variables (API_URL, PARTY_ID, PASSWORD)
        cfg = ExchangeClientConfig()

        # Option 2: Override defaults in code
        # cfg = ExchangeClientConfig(api_url="http://localhost:8000",
        #                            default_party_id=42,
        #                            default_password="supersecret")

        client = ExchangeClient(cfg)

        # Create a new instrument/book (admin privileges required)
        resp = client.create_order_book(instrument_id=123)

        # Place a GTC SELL order
        sell_resp = client.place_order(
            instrument_id=123,
            side="SELL",
            order_type="GTC",
            price_cents=10050,
            quantity=5
        )

        # Cancel an existing order
        cancel_resp = client.cancel_order(instrument_id=123, order_id=1)
    """

    def __init__(self, config: ExchangeClientConfig):
        self._config = config
        self._session: Session = requests.Session()
        self._session.headers.update(
            {"Content-Type": "application/json", "Accept": "application/json"}
        )

    @staticmethod
    def _handle_response(resp: Response) -> Dict[str, Any]:
        """
        Inspect HTTP response:
          - If status code == 422: raise ValidationError with details
          - If 200 ≤ code < 300: return parsed JSON
          - Otherwise: raise HTTPRequestError
        """
        if resp.status_code == 422:
            # Attempt to decode JSON details
            try:
                details = resp.json()
            except json.JSONDecodeError:
                details = resp.text
            logger.debug("ValidationError (422) response body: %s", details)
            raise ValidationError(f"Validation or unknown instrument (HTTP 422)", details)

        try:
            resp.raise_for_status()
        except HTTPError as e:
            status = resp.status_code
            text = resp.text.strip()
            logger.error("HTTP request failed: %d %s", status, text)
            raise HTTPRequestError(status, text) from e

        # 2xx success path
        try:
            return resp.json()
        except json.JSONDecodeError:
            message = f"Expected JSON response, got: {resp.text}"
            logger.error(message)
            raise ExchangeClientError(message)

    def create_order_book(
        self,
        instrument_id: int,
        *,
        admin_party_id: Optional[int] = None,
        admin_password: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new order book (instrument) via POST /new_book.
        Must supply admin credentials—defaults to `config.default_*`.

        Returns:
            The parsed JSON response, e.g. {"status":"CREATED", "instrument_id": ...}
        Raises:
            ValidationError, HTTPRequestError
        """
        url = f"{self._config.api_url}/new_book"
        payload = {
            "instrument_id": instrument_id,
            "party_id": admin_party_id if admin_party_id is not None else self._config.default_party_id,
            "password": admin_password if admin_password is not None else self._config.default_password,
        }
        logger.info("POST %s → payload: %s", url, payload)
        try:
            resp = self._session.post(url, json=payload, timeout=10.0)
        except RequestException as e:
            logger.error("Network error during create_order_book: %s", e)
            raise ExchangeClientError(f"Network error: {e}") from e

        return self._handle_response(resp)

    def place_order(
        self,
        instrument_id: int,
        side: str,
        order_type: str,
        quantity: int,
        price_cents: Optional[int] = None,
        *,
        party_id: Optional[int] = None,
        password: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Place a new order via POST /orders.

        Arguments:
            instrument_id: int
            side: "BUY" or "SELL"
            order_type: "MARKET", "GTC", or "IOC"
            quantity: positive int
            price_cents: non‐negative int if GTC/IOC; omit or None for MARKET
            party_id: override default party_id (int)
            password: override default password (str)

        Returns:
            Parsed JSON response, e.g.
            {
                "status": "ACCEPTED",
                "order_id": 42,
                "remaining_qty": 2,
                "cancelled": false,
                "trades": [ ... ]
            }

        Raises:
            ValidationError: if API returns HTTP 422
            HTTPRequestError: for any other 4xx/5xx
            ExchangeClientError: for non‐JSON or network errors
        """
        url = f"{self._config.api_url}/orders"
        payload: Dict[str, Any] = {
            "instrument_id": instrument_id,
            "side": side,
            "order_type": order_type,
            "quantity": quantity,
            "party_id": party_id if party_id is not None else self._config.default_party_id,
            "password": password if password is not None else self._config.default_password,
        }
        if price_cents is not None:
            payload["price_cents"] = price_cents

        logger.info("POST %s → payload: %s", url, payload)
        try:
            resp = self._session.post(url, json=payload, timeout=10.0)
        except RequestException as e:
            logger.error("Network error during place_order: %s", e)
            raise ExchangeClientError(f"Network error: {e}") from e

        return self._handle_response(resp)

    def cancel_order(
        self,
        instrument_id: int,
        order_id: int,
        *,
        party_id: Optional[int] = None,
        password: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Cancel an existing order via POST /cancel.

        Arguments:
            instrument_id: int
            order_id: int
            party_id: override default party_id (int)
            password: override default password (str)

        Returns:
            Parsed JSON response, e.g.
            {"status":"CANCELLED","order_id":...} or {"status":"ERROR","details":...}

        Raises:
            HTTPRequestError: if the HTTP status is not 200.
            ExchangeClientError: for network/non‐JSON errors.
        """
        url = f"{self._config.api_url}/cancel"
        payload = {
            "instrument_id": instrument_id,
            "order_id": order_id,
            "party_id": party_id if party_id is not None else self._config.default_party_id,
            "password": password if password is not None else self._config.default_password,
        }

        logger.info("POST %s → payload: %s", url, payload)
        try:
            resp = self._session.post(url, json=payload, timeout=10.0)
        except RequestException as e:
            logger.error("Network error during cancel_order: %s", e)
            raise ExchangeClientError(f"Network error: {e}") from e

        # The cancel endpoint always returns HTTP 200 with JSON {"status": ...}
        try:
            resp.raise_for_status()
        except HTTPError as e:
            status = resp.status_code
            text = resp.text.strip()
            logger.error("HTTP error during cancel_order: %d %s", status, text)
            raise HTTPRequestError(status, text) from e

        try:
            return resp.json()
        except json.JSONDecodeError:
            message = f"Expected JSON response for cancel_order, got: {resp.text}"
            logger.error(message)
            raise ExchangeClientError(message)
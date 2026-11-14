"""
tests/test_prepare_data.py

Unit tests for the FreshCart data-preparation helpers.
These run in CI (see .github/workflows/ci.yml) and locally via:
    uv run pytest tests/
"""

import pytest


# ---------------------------------------------------------------------------
# Helper functions (extracted from the ELT notebook for testability)
# ---------------------------------------------------------------------------

def clean_email(raw: str) -> str:
    """Strip whitespace and lowercase an email address."""
    return raw.strip().lower()


def parse_amount(raw) -> float | None:
    """
    Convert a raw amount string to float.
    - Removes commas (e.g. '1,250.00' → 1250.00)
    - Returns None for missing / non-numeric values.
    """
    if raw is None:
        return None
    try:
        return float(str(raw).replace(",", ""))
    except ValueError:
        return None


def is_valid_order(order: dict) -> tuple[bool, str]:
    """
    Validate a single order dict.
    Returns (True, '') if valid, or (False, reason) if invalid.
    """
    if not order.get("order_id"):
        return False, "missing order_id"
    if parse_amount(order.get("amount")) is None:
        return False, "unparseable amount"
    if not order.get("cust_email", "").strip():
        return False, "missing email"
    return True, ""


# ---------------------------------------------------------------------------
# Tests: clean_email
# ---------------------------------------------------------------------------

class TestCleanEmail:
    def test_strips_leading_and_trailing_spaces(self):
        assert clean_email("  Priya.R@Gmail.com ") == "priya.r@gmail.com"

    def test_lowercases(self):
        assert clean_email("USER@EXAMPLE.COM") == "user@example.com"

    def test_already_clean(self):
        assert clean_email("user@example.com") == "user@example.com"


# ---------------------------------------------------------------------------
# Tests: parse_amount
# ---------------------------------------------------------------------------

class TestParseAmount:
    def test_parses_comma_formatted_number(self):
        assert parse_amount("1,250.00") == pytest.approx(1250.0)

    def test_parses_plain_number(self):
        assert parse_amount("499.00") == pytest.approx(499.0)

    def test_returns_none_for_missing(self):
        assert parse_amount(None) is None

    def test_returns_none_for_non_numeric(self):
        assert parse_amount("N/A") is None

    def test_accepts_integer_input(self):
        assert parse_amount(500) == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# Tests: is_valid_order
# ---------------------------------------------------------------------------

class TestIsValidOrder:
    def _good_order(self):
        return {
            "order_id": "ORD-1001",
            "cust_email": "priya.r@gmail.com",
            "amount": "1,250.00",
        }

    def test_valid_order_passes(self):
        ok, reason = is_valid_order(self._good_order())
        assert ok is True
        assert reason == ""

    def test_missing_order_id_fails(self):
        order = self._good_order()
        order["order_id"] = ""
        ok, reason = is_valid_order(order)
        assert ok is False
        assert "order_id" in reason

    def test_missing_amount_fails(self):
        order = self._good_order()
        order["amount"] = None
        ok, reason = is_valid_order(order)
        assert ok is False
        assert "amount" in reason

    def test_bad_amount_string_fails(self):
        order = self._good_order()
        order["amount"] = "N/A"
        ok, reason = is_valid_order(order)
        assert ok is False

    def test_missing_email_fails(self):
        order = self._good_order()
        order["cust_email"] = "   "
        ok, reason = is_valid_order(order)
        assert ok is False
        assert "email" in reason

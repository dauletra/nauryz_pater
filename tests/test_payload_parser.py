"""Тесты для bot._parse_subscription_payload — парсинга invoice_payload.

Покрывают граничные случаи: пустая строка, мусор, неполный promo,
невалидный region_guid, нечисловые stars и т.д.
Защита от IndexError и ошибочной активации подписки на битый регион.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def valid_guid():
    import regions
    return next(iter(regions.REGIONS))


@pytest.fixture
def parse(monkeypatch, tmp_path):
    """Импортируем bot с минимальным env."""
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "x.db"))
    monkeypatch.setenv("TELEGRAM_TOKEN", "x")
    monkeypatch.setenv("WEBHOOK_SECRET", "x")
    import importlib, config, bot
    importlib.reload(config)
    importlib.reload(bot)
    return bot._parse_subscription_payload


class TestInvalidPayloads:
    def test_empty_string(self, parse):
        assert parse("") is None

    def test_no_prefix(self, parse):
        assert parse("garbage") is None

    def test_prefix_only_with_colon(self, parse):
        # "sub:" → split → ["sub", ""] → empty guid → невалиден
        assert parse("sub:") is None

    def test_prefix_only_no_colon(self, parse):
        # "sub" не начинается с "sub:" → отсекается префикс-чеком
        assert parse("sub") is None

    def test_invalid_guid(self, parse):
        # Главный новый чек: regions.is_valid_region отсекает чужие GUID'ы
        assert parse("sub:fake-guid-not-in-regions") is None

    def test_promo_too_short(self, parse, valid_guid):
        # 3 части — половина promo формата
        assert parse(f"sub:{valid_guid}:promo") is None

    def test_promo_missing_stars(self, parse, valid_guid):
        # 4 части — нет stars
        assert parse(f"sub:{valid_guid}:promo:CODE10") is None

    def test_promo_empty_code(self, parse, valid_guid):
        # 5 частей, но code пустой
        assert parse(f"sub:{valid_guid}:promo::200") is None

    def test_promo_non_numeric_stars(self, parse, valid_guid):
        # 5 частей, stars не int
        assert parse(f"sub:{valid_guid}:promo:CODE10:abc") is None

    def test_unknown_subtype(self, parse, valid_guid):
        # "other" вместо "promo"
        assert parse(f"sub:{valid_guid}:other:foo:bar") is None

    def test_too_many_parts(self, parse, valid_guid):
        # 6 частей — неизвестный формат
        assert parse(f"sub:{valid_guid}:promo:CODE10:200:extra") is None


class TestValidPayloads:
    def test_simple_subscription(self, parse, valid_guid):
        assert parse(f"sub:{valid_guid}") == (valid_guid, None, None)

    def test_promo_subscription(self, parse, valid_guid):
        result = parse(f"sub:{valid_guid}:promo:BLACK10:200")
        assert result == (valid_guid, "BLACK10", 200)

    def test_promo_subscription_zero_stars(self, parse, valid_guid):
        # 100% скидка → 0 stars (граничный случай)
        result = parse(f"sub:{valid_guid}:promo:FREE100:0")
        assert result == (valid_guid, "FREE100", 0)

    def test_promo_with_special_chars_in_code(self, parse, valid_guid):
        # Промокоды могут содержать буквы/цифры/дефисы
        result = parse(f"sub:{valid_guid}:promo:NY-2026:150")
        assert result == (valid_guid, "NY-2026", 150)


class TestSecurityRegression:
    """Регрессия на конкретные баги, которые мы закрыли."""

    def test_no_indexerror_on_short_payload(self, parse):
        """Раньше parts[1] падал с IndexError. Теперь возвращает None."""
        for short in ["", "s", "su", "sub", "sub:"]:
            assert parse(short) is None  # без exception

    def test_no_silent_pass_with_invalid_guid_at_correct_amount(self, parse):
        """Раньше payload с битым guid и правильной суммой подтверждался.
        Теперь отсекается на стадии парсинга.
        """
        # Эта проверка в _handle_pre_checkout: parsed=None → отказ.
        assert parse("sub:not-a-real-region") is None

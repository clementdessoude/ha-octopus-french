"""
Tests pour le scoping par PRM du coordinator électricité.

Vérifie qu'un compte avec plusieurs points de livraison (plusieurs Linky)
récupère et stocke les relevés/index de CHAQUE PRM séparément, et non plus
uniquement ceux du premier (bug de scoping mono-PRM).
"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from freezegun.api import FrozenDateTimeFactory

from custom_components.octopus_french.coordinator import (
    OctopusFrenchDataUpdateCoordinator,
)


def _make_coordinator(
    account_data: dict[str, Any],
    options: dict[str, Any] | None = None,
) -> OctopusFrenchDataUpdateCoordinator:
    """Instancie le coordinator sans passer par l'init lourd de DataUpdateCoordinator."""
    coordinator = OctopusFrenchDataUpdateCoordinator.__new__(
        OctopusFrenchDataUpdateCoordinator
    )
    coordinator.account_number = "ACC-123"
    coordinator.config_entry = SimpleNamespace(options=options or {})
    coordinator._load_curve_cache = {}

    api_client = AsyncMock()
    api_client.get_account_data.return_value = account_data

    async def _readings(
        account_id: str,
        start: str,
        end: str,
        prm_id: str,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return [{"startAt": "2026-05-01T00:00:00", "prm": prm_id, "value": prm_id}]

    async def _index(account_number: str, prm_id: str) -> dict[str, Any]:
        return {"prm": prm_id, "tariff_type": "BASE"}

    api_client.get_energy_readings.side_effect = _readings
    api_client.get_electricity_index.side_effect = _index
    api_client.get_all_payment_requests.return_value = {}
    coordinator.api_client = api_client
    return coordinator


async def test_electricity_scoped_per_prm() -> None:
    """Chaque PRM doit obtenir ses propres relevés et index."""
    account_data = {
        "account_id": "ID-1",
        "account_number": "ACC-123",
        "supply_points": {
            "electricity": [
                {"prm": "PRM_A", "distributorStatus": "SERVC"},
                {"prm": "PRM_B", "distributorStatus": "SERVC"},
            ],
            "gas": [],
        },
        "agreements": [],
        "ledgers": {},
    }

    coordinator = _make_coordinator(account_data)
    result = await coordinator._fetch_all_data()

    by_prm = result["electricity_by_prm"]
    assert set(by_prm) == {"PRM_A", "PRM_B"}
    # Chaque PRM porte SES propres données, pas celles du premier.
    assert by_prm["PRM_A"]["readings"][0]["prm"] == "PRM_A"
    assert by_prm["PRM_B"]["readings"][0]["prm"] == "PRM_B"
    assert by_prm["PRM_A"]["index"]["prm"] == "PRM_A"
    assert by_prm["PRM_B"]["index"]["prm"] == "PRM_B"


@pytest.mark.parametrize(
    ("today", "expected_start"),
    [
        pytest.param("2026-09-03T12:00:00", date(2026, 8, 1), id="early_month"),
        pytest.param("2026-09-20T12:00:00", date(2026, 8, 25), id="mid_month"),
    ],
)
async def test_electricity_window_covers_previous_month_at_rollover(
    freezer: FrozenDateTimeFactory, today: str, expected_start: date
) -> None:
    """
    Début de mois : le mois précédent est demandé en entier (issue #87).

    Les capteurs mensuels l'affichent encore tant que le premier relevé du
    nouveau mois n'est pas arrivé ; ensuite, seul le recouvrement d'une
    semaine reste nécessaire à l'import des statistiques.
    """
    freezer.move_to(today)
    account_data = {
        "account_id": "ID-1",
        "account_number": "ACC-123",
        "supply_points": {
            "electricity": [{"prm": "PRM_A", "distributorStatus": "SERVC"}],
            "gas": [],
        },
        "agreements": [],
        "ledgers": {},
    }
    coordinator = _make_coordinator(account_data)

    await coordinator._fetch_all_data()

    start = coordinator.api_client.get_energy_readings.call_args.args[1]
    assert datetime.fromisoformat(start).date() == expected_start


async def test_electricity_readings_use_own_property_id() -> None:
    """Chaque PRM interroge SA property, pas celle du premier logement (issue #56).

    Deux compteurs sur deux logements (properties) distincts : les relevés du
    second ne doivent pas être demandés sur la property du premier.
    """
    account_data = {
        "account_id": "PROP-1",
        "account_number": "ACC-123",
        "supply_points": {
            "electricity": [
                {
                    "prm": "PRM_A",
                    "distributorStatus": "SERVC",
                    "property_id": "PROP-1",
                },
                {
                    "prm": "PRM_B",
                    "distributorStatus": "SERVC",
                    "property_id": "PROP-2",
                },
            ],
            "gas": [],
        },
        "agreements": [],
        "ledgers": {},
    }

    coordinator = _make_coordinator(account_data)

    property_by_prm: dict[str, str] = {}

    async def _readings(
        property_id: str,
        start: str,
        end: str,
        prm_id: str,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        property_by_prm[prm_id] = property_id
        return [{"prm": prm_id}]

    coordinator.api_client.get_energy_readings.side_effect = _readings

    await coordinator._fetch_all_data()

    assert property_by_prm == {"PRM_A": "PROP-1", "PRM_B": "PROP-2"}


async def test_electricity_property_id_falls_back_to_account_id() -> None:
    """Sans property_id sur le compteur, on retombe sur account_id (compat)."""
    account_data = {
        "account_id": "PROP-1",
        "account_number": "ACC-123",
        "supply_points": {
            "electricity": [
                {"prm": "PRM_A", "distributorStatus": "SERVC"},
            ],
            "gas": [],
        },
        "agreements": [],
        "ledgers": {},
    }

    coordinator = _make_coordinator(account_data)

    seen: dict[str, str] = {}

    async def _readings(
        property_id: str,
        start: str,
        end: str,
        prm_id: str,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        seen[prm_id] = property_id
        return []

    coordinator.api_client.get_energy_readings.side_effect = _readings

    await coordinator._fetch_all_data()

    assert seen == {"PRM_A": "PROP-1"}


async def test_resiliated_supply_point_is_filtered_out() -> None:
    """Un point de livraison résilié (RESIL) est exclu du fetch."""
    account_data = {
        "account_id": "ID-1",
        "account_number": "ACC-123",
        "supply_points": {
            "electricity": [
                {"prm": "PRM_A", "distributorStatus": "SERVC"},
                {"prm": "PRM_OLD", "distributorStatus": "RESIL"},
            ],
            "gas": [],
        },
        "agreements": [],
        "ledgers": {},
    }

    coordinator = _make_coordinator(account_data)
    result = await coordinator._fetch_all_data()

    assert set(result["electricity_by_prm"]) == {"PRM_A"}


async def test_resiliated_but_powered_supply_point_is_kept() -> None:
    """Un RESIL distributeur encore alimenté reste exposé (issue #75).

    `distributorStatus` suit le contrat d'accès Enedis, pas la fourniture : il
    reste à RESIL après un changement de fournisseur alors que le compteur est
    alimenté et sous contrat.
    """
    account_data = {
        "account_id": "ID-1",
        "account_number": "ACC-123",
        "supply_points": {
            "electricity": [
                {
                    "prm": "PRM_ALIM",
                    "distributorStatus": "RESIL",
                    "poweredStatus": "ALIM",
                },
                {
                    "prm": "PRM_LIMI",
                    "distributorStatus": "RESIL",
                    "poweredStatus": "LIMI",
                },
            ],
            "gas": [],
        },
        "agreements": [],
        "ledgers": {},
    }

    coordinator = _make_coordinator(account_data)
    result = await coordinator._fetch_all_data()

    assert set(result["electricity_by_prm"]) == {"PRM_ALIM"}


_ONE_PRM_ACCOUNT = {
    "account_id": "ID-1",
    "account_number": "ACC-123",
    "supply_points": {
        "electricity": [{"prm": "PRM_A", "distributorStatus": "SERVC"}],
        "gas": [],
    },
    "agreements": [],
    "ledgers": {},
}


def _frequencies(coordinator: OctopusFrenchDataUpdateCoordinator) -> list[str]:
    return [
        call.kwargs.get("reading_frequency")
        for call in coordinator.api_client.get_energy_readings.call_args_list
    ]


async def test_load_curve_not_fetched_by_default() -> None:
    """Sans l'option, aucune courbe de charge n'est demandée à l'API."""
    coordinator = _make_coordinator(_ONE_PRM_ACCOUNT)
    result = await coordinator._fetch_all_data()

    assert _frequencies(coordinator) == ["DAY_INTERVAL"]
    assert result["electricity_by_prm"]["PRM_A"]["load_curve"] == []


async def test_load_curve_fetched_once_per_new_day() -> None:
    """
    Avec l'option, la courbe à la demi-heure est récupérée puis mise en cache.

    Octopus ne publie qu'une fois par jour : tant que le dernier relevé
    journalier ne change pas, la courbe n'est pas redemandée.
    """
    coordinator = _make_coordinator(
        _ONE_PRM_ACCOUNT, options={"hourly_statistics": True}
    )
    first = await coordinator._fetch_all_data()
    await coordinator._fetch_all_data()

    assert _frequencies(coordinator) == [
        "DAY_INTERVAL",
        "THIRTY_MIN_INTERVAL",
        "DAY_INTERVAL",
    ]
    assert first["electricity_by_prm"]["PRM_A"]["load_curve"][0]["prm"] == "PRM_A"


async def test_load_curve_refetched_until_latest_day_published() -> None:
    """Tant que la courbe du dernier jour manque, elle est redemandée au cycle suivant."""
    coordinator = _make_coordinator(
        _ONE_PRM_ACCOUNT, options={"hourly_statistics": True}
    )

    async def _readings(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        if kwargs.get("reading_frequency") == "THIRTY_MIN_INTERVAL":
            return [{"startAt": "2026-04-30T12:00:00+00:00", "value": 1.0}]
        return [{"startAt": "2026-05-02T12:00:00+00:00", "value": 5.0}]

    coordinator.api_client.get_energy_readings.side_effect = _readings
    await coordinator._fetch_all_data()
    await coordinator._fetch_all_data()

    assert _frequencies(coordinator).count("THIRTY_MIN_INTERVAL") == 2

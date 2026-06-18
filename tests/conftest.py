"""Shared pytest fixtures for armada-flyte unit tests.

None of these tests talk to a live Armada. The ArmadaConnector reaches the cluster
only through its lazily-built ``client`` property, which returns ``self._client`` when
that attribute is already set. Setting ``connector._client`` to a MagicMock therefore
short-circuits the gRPC channel entirely (the ``client`` property never constructs one).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

# Imported for its proto-pool side effect before any armada_client import happens.
import armada_flyte._proto_compat  # noqa: F401
from armada_flyte.connector import ArmadaConnector


@pytest.fixture
def mock_client() -> MagicMock:
    """A bare MagicMock standing in for ArmadaClient."""
    return MagicMock()


@pytest.fixture
def connector(mock_client: MagicMock) -> ArmadaConnector:
    """An ArmadaConnector whose gRPC client is replaced with a MagicMock.

    Because ``_client`` is pre-set, the ``client`` property short-circuits and never
    opens a socket, so the connector's create/get/delete logic runs offline.
    """
    c = ArmadaConnector(armada_url="localhost:50051")
    c._client = mock_client
    return c

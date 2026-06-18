"""Flyte 2 to Armada connector.

Import this package to register the Armada connector with Flyte's ConnectorRegistry,
then author DAG nodes with :class:`armada_flyte.task.ArmadaTask`.
"""

# Must be first: aliases armada_client's vendored google.api to the standard one before
# any armada_client import, avoiding a duplicate google/api/http.proto registration.
import armada_flyte._proto_compat  # noqa: F401,E402

from armada_flyte.connector import ArmadaConnector, ArmadaJobMetadata
from armada_flyte.task import ArmadaConfig, ArmadaTask

__all__ = ["ArmadaConnector", "ArmadaJobMetadata", "ArmadaConfig", "ArmadaTask"]

"""Gang annotations: only emitted for a real gang, with the right Armada keys."""

from __future__ import annotations

from armada_flyte.connector import (
    _GANG_CARDINALITY_ANNOTATION,
    _GANG_ID_ANNOTATION,
    _gang_annotations,
)


def test_no_gang_by_default():
    assert _gang_annotations({}) == {}
    assert _gang_annotations({"gang_id": None, "gang_cardinality": 0}) == {}


def test_cardinality_below_two_is_not_a_gang():
    # A gang of one is not a gang; do not annotate it.
    assert _gang_annotations({"gang_id": "g", "gang_cardinality": 1}) == {}


def test_gang_annotations():
    ann = _gang_annotations({"gang_id": "workers", "gang_cardinality": 3})
    assert ann[_GANG_ID_ANNOTATION] == "workers"
    assert ann[_GANG_CARDINALITY_ANNOTATION] == "3"

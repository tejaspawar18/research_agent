from shared.utils.slack_feedback import (
    build_feedback_id,
    get_candidate_week_years,
    get_week_year_from_message_ts,
    map_reaction_to_feedback_type,
)


def test_get_week_year_from_message_ts_uses_message_timestamp():
    assert get_week_year_from_message_ts("1704067200.000100") == "2024-W01"


def test_get_candidate_week_years_includes_boundary_fallbacks():
    candidates = get_candidate_week_years("1735516800.000100")

    assert "2025-W01" in candidates
    assert "2024-W52" in candidates


def test_map_reaction_to_feedback_type_normalizes_common_reactions():
    assert map_reaction_to_feedback_type("thumbsup") == "positive"
    assert map_reaction_to_feedback_type("-1") == "negative"
    assert map_reaction_to_feedback_type("eyes") == "reaction"


def test_build_feedback_id_is_deterministic():
    left = build_feedback_id("reaction", "2026-W13", "C123", "1740000000.000100", "U123", "thumbsup")
    right = build_feedback_id("reaction", "2026-W13", "C123", "1740000000.000100", "U123", "thumbsup")

    assert left == right

"""
Detailed diagnostic test for find_best_matching_persona function.

This test specifically checks that the index mapping between
the names list and the dates list is correct.
"""

import pandas as pd
from typing import List, Tuple, Optional
from rapidfuzz import fuzz, process


def find_best_matching_persona(target_name: str, persona_names: List[Tuple[str, str]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Find the best matching PRIEKŠMETS - PERSONA name using fuzzy matching.

    Args:
        target_name: The name to match (recenzeta_darba_autors)
        persona_names: List of tuples (persona_name, persona_dates) to match against

    Returns:
        Tuple of (best_matching_name, corresponding_dates) or (None, None) if no good match
    """
    if pd.isna(target_name) or not target_name or not persona_names:
        return None, None

    # Filter out None values
    valid_personas = [(name, dates) for name, dates in persona_names if pd.notna(name) and name]

    if not valid_personas:
        return None, None

    # Extract just the names for matching
    names_only = [name for name, _ in valid_personas]

    # Use rapidfuzz to find the best match
    # Using token_sort_ratio which handles word order differences well
    result = process.extractOne(
        target_name,
        names_only,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=10  # Liberal cutoff - accept matches with 10% or higher similarity
    )

    if result is None:
        return None, None

    best_match_name, score, index = result

    # Get the corresponding dates
    best_match_dates = valid_personas[index][1]

    return best_match_name, best_match_dates


def test_index_mapping():
    """Test that the index mapping between names and dates is correct."""

    print("=" * 80)
    print("DETAILED INDEX MAPPING TEST")
    print("=" * 80)

    # Test case with distinct dates to verify correct mapping
    print("\nTest 1: Verify correct name-date pairing")
    print("-" * 80)
    target = "Viesturs Kairišs"
    personas = [
        ("Māra Ķimele", "1943-"),
        ("Jānis Rokpelnis", "1945-"),
        ("Viesturs Kairišs", "1971-"),
        ("Dace Priede", "1954-"),
    ]

    print(f"Target: {target}")
    print("\nAvailable personas:")
    for i, (name, dates) in enumerate(personas):
        print(f"  [{i}] {name} ({dates})")

    # Show all scores
    print("\nScores for all candidates:")
    valid_personas = [(name, dates) for name, dates in personas if pd.notna(name) and name]
    names_only = [name for name, _ in valid_personas]

    all_results = process.extract(
        target,
        names_only,
        scorer=fuzz.token_sort_ratio,
        limit=len(names_only)
    )

    for matched_name, score, idx in all_results:
        dates = valid_personas[idx][1]
        print(f"  [{idx}] {matched_name} ({dates}) - Score: {score}")

    # Get the best match using our function
    result_name, result_dates = find_best_matching_persona(target, personas)
    print("\nBest match returned by function:")
    print(f"  Name: {result_name}")
    print(f"  Dates: {result_dates}")

    # Verify it's correct
    expected_dates = "1971-"
    assert result_dates == expected_dates, f"Expected {expected_dates}, got {result_dates}"
    print(f"✓ Correct! Dates match expected: {expected_dates}")

    # Test case 2: First persona is the match
    print("\n" + "=" * 80)
    print("\nTest 2: Match is the first persona in list")
    print("-" * 80)
    target = "Māra Ķimele"
    personas = [
        ("Māra Ķimele", "1943-"),
        ("Jānis Rokpelnis", "1945-"),
        ("Viesturs Kairišs", "1971-"),
    ]

    print(f"Target: {target}")
    print("\nAvailable personas:")
    for i, (name, dates) in enumerate(personas):
        print(f"  [{i}] {name} ({dates})")

    result_name, result_dates = find_best_matching_persona(target, personas)
    print("\nBest match returned by function:")
    print(f"  Name: {result_name}")
    print(f"  Dates: {result_dates}")

    expected_dates = "1943-"
    assert result_dates == expected_dates, f"Expected {expected_dates}, got {result_dates}"
    print(f"✓ Correct! Dates match expected: {expected_dates}")

    # Test case 3: Last persona is the match
    print("\n" + "=" * 80)
    print("\nTest 3: Match is the last persona in list")
    print("-" * 80)
    target = "Dace Priede"
    personas = [
        ("Māra Ķimele", "1943-"),
        ("Jānis Rokpelnis", "1945-"),
        ("Viesturs Kairišs", "1971-"),
        ("Dace Priede", "1954-"),
    ]

    print(f"Target: {target}")
    print("\nAvailable personas:")
    for i, (name, dates) in enumerate(personas):
        print(f"  [{i}] {name} ({dates})")

    result_name, result_dates = find_best_matching_persona(target, personas)
    print("\nBest match returned by function:")
    print(f"  Name: {result_name}")
    print(f"  Dates: {result_dates}")

    expected_dates = "1954-"
    assert result_dates == expected_dates, f"Expected {expected_dates}, got {result_dates}"
    print(f"✓ Correct! Dates match expected: {expected_dates}")

    # Test case 4: With None values in the list
    print("\n" + "=" * 80)
    print("\nTest 4: List contains None values")
    print("-" * 80)
    target = "Jānis Rokpelnis"
    personas = [
        (None, None),
        ("Māra Ķimele", "1943-"),
        (None, None),
        ("Jānis Rokpelnis", "1945-"),
        ("Viesturs Kairišs", "1971-"),
        (None, None),
    ]

    print(f"Target: {target}")
    print("\nAvailable personas (including None values):")
    for i, (name, dates) in enumerate(personas):
        print(f"  [{i}] {name} ({dates})")

    # Show valid personas after filtering
    valid_personas = [(name, dates) for name, dates in personas if pd.notna(name) and name]
    print("\nAfter filtering None values:")
    for i, (name, dates) in enumerate(valid_personas):
        print(f"  [{i}] {name} ({dates})")

    result_name, result_dates = find_best_matching_persona(target, personas)
    print("\nBest match returned by function:")
    print(f"  Name: {result_name}")
    print(f"  Dates: {result_dates}")

    expected_dates = "1945-"
    assert result_dates == expected_dates, f"Expected {expected_dates}, got {result_dates}"
    print(f"✓ Correct! Dates match expected: {expected_dates}")

    # Test case 5: Partial match scenario (not exact)
    print("\n" + "=" * 80)
    print("\nTest 5: Partial/fuzzy match (not exact)")
    print("-" * 80)
    target = "Viesturs Kairishs"  # Misspelled
    personas = [
        ("Māra Ķimele", "1943-"),
        ("Viesturs Kairišs", "1971-"),
        ("Viesturs Kaprāns", "1965-"),
    ]

    print(f"Target: {target} (misspelled)")
    print("\nAvailable personas:")
    for i, (name, dates) in enumerate(personas):
        score = fuzz.token_sort_ratio(target, name)
        print(f"  [{i}] {name} ({dates}) - Score: {score}")

    result_name, result_dates = find_best_matching_persona(target, personas)
    print("\nBest match returned by function:")
    print(f"  Name: {result_name}")
    print(f"  Dates: {result_dates}")

    # Should match "Viesturs Kairišs" with highest score
    expected_dates = "1971-"
    assert result_dates == expected_dates, f"Expected {expected_dates}, got {result_dates}"
    print(f"✓ Correct! Dates match expected: {expected_dates}")

    print("\n" + "=" * 80)
    print("ALL TESTS PASSED ✓")
    print("=" * 80)


if __name__ == "__main__":
    try:
        test_index_mapping()
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        raise


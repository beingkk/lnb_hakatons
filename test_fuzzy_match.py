"""
Test script for find_best_matching_persona function.

This script tests the fuzzy matching functionality used to match
recenzeta_darba_autors with PRIEKŠMETS - PERSONA fields.
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


def test_fuzzy_matching():
    """Test various scenarios for fuzzy matching."""

    print("=" * 80)
    print("TESTING find_best_matching_persona FUNCTION")
    print("=" * 80)

    # Test 1: Exact match
    print("\n1. EXACT MATCH TEST")
    print("-" * 80)
    target = "Jānis Rokpelnis"
    personas = [
        ("Jānis Rokpelnis", "1945-"),
        ("Māra Ķimele", "1943-"),
        ("Dace Priede", "1954-")
    ]
    result_name, result_dates = find_best_matching_persona(target, personas)
    score = fuzz.token_sort_ratio(target, result_name) if result_name else 0
    print(f"Target: {target}")
    print(f"Match:  {result_name} ({result_dates})")
    print(f"Score:  {score}")

    # Test 2: Word order different
    print("\n2. WORD ORDER TEST")
    print("-" * 80)
    target = "Rokpelnis Jānis"
    personas = [
        ("Jānis Rokpelnis", "1945-"),
        ("Māra Ķimele", "1943-"),
    ]
    result_name, result_dates = find_best_matching_persona(target, personas)
    score = fuzz.token_sort_ratio(target, result_name) if result_name else 0
    print(f"Target: {target}")
    print(f"Match:  {result_name} ({result_dates})")
    print(f"Score:  {score}")

    # Test 3: Partial name (initials)
    print("\n3. PARTIAL NAME TEST (initials)")
    print("-" * 80)
    target = "J. Rokpelnis"
    personas = [
        ("Jānis Rokpelnis", "1945-"),
        ("Jānis Runcis", "1950-"),
    ]
    result_name, result_dates = find_best_matching_persona(target, personas)
    score = fuzz.token_sort_ratio(target, result_name) if result_name else 0
    print(f"Target: {target}")
    print(f"Match:  {result_name} ({result_dates})")
    print(f"Score:  {score}")

    # Test 4: Misspelling
    print("\n4. MISSPELLING TEST")
    print("-" * 80)
    target = "Janis Rokpelnis"  # Missing diacritic
    personas = [
        ("Jānis Rokpelnis", "1945-"),
        ("Māra Ķimele", "1943-"),
    ]
    result_name, result_dates = find_best_matching_persona(target, personas)
    score = fuzz.token_sort_ratio(target, result_name) if result_name else 0
    print(f"Target: {target}")
    print(f"Match:  {result_name} ({result_dates})")
    print(f"Score:  {score}")

    # Test 5: Multiple candidates
    print("\n5. MULTIPLE CANDIDATES TEST")
    print("-" * 80)
    target = "Viesturs Kairišs"
    personas = [
        ("Viesturs Kairišs", "1971-"),
        ("Viesturs Kaprāns", "1965-"),
        ("Andris Kairišs", "1980-"),
    ]
    result_name, result_dates = find_best_matching_persona(target, personas)
    # Show all scores
    print(f"Target: {target}")
    print("All candidates:")
    for name, dates in personas:
        score = fuzz.token_sort_ratio(target, name)
        print(f"  - {name} ({dates}) - Score: {score}")
    print(f"\nBest match: {result_name} ({result_dates})")

    # Test 6: Cyrillic characters
    print("\n6. CYRILLIC CHARACTERS TEST")
    print("-" * 80)
    target = "Сергій Жадан"
    personas = [
        ("Сергій Вікторович Жадан", "1974-"),
        ("Андрій Петрович", "1980-"),
    ]
    result_name, result_dates = find_best_matching_persona(target, personas)
    score = fuzz.token_sort_ratio(target, result_name) if result_name else 0
    print(f"Target: {target}")
    print(f"Match:  {result_name} ({result_dates})")
    print(f"Score:  {score}")

    # Test 7: No match (None input)
    print("\n7. NONE INPUT TEST")
    print("-" * 80)
    target = None
    personas = [
        ("Jānis Rokpelnis", "1945-"),
    ]
    result_name, result_dates = find_best_matching_persona(target, personas)
    print(f"Target: {target}")
    print(f"Match:  {result_name} ({result_dates})")

    # Test 8: Empty persona list
    print("\n8. EMPTY PERSONA LIST TEST")
    print("-" * 80)
    target = "Jānis Rokpelnis"
    personas = []
    result_name, result_dates = find_best_matching_persona(target, personas)
    print(f"Target: {target}")
    print(f"Match:  {result_name} ({result_dates})")

    # Test 9: Director format (as appears in data)
    print("\n9. DIRECTOR FORMAT TEST")
    print("-" * 80)
    target = "Viesturs Kairišs, Jānis Ābele"  # Multiple directors
    personas = [
        ("Viesturs Kairišs", "1971-"),
        ("Jānis Ābele", "1980-"),
        ("Māra Ķimele", "1943-"),
    ]
    result_name, result_dates = find_best_matching_persona(target, personas)
    score = fuzz.token_sort_ratio(target, result_name) if result_name else 0
    print(f"Target: {target}")
    print(f"Match:  {result_name} ({result_dates})")
    print(f"Score:  {score}")
    print("All candidates:")
    for name, dates in personas:
        score = fuzz.token_sort_ratio(target, name)
        print(f"  - {name} ({dates}) - Score: {score}")

    print("\n" + "=" * 80)
    print("TESTING COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    test_fuzzy_matching()


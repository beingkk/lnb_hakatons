"""
Data cleaning pipeline for Latvian National Library art criticism records.

This script processes MARC bibliographic data from the Digital Library,
cleaning and harmonizing review records for analysis.

Usage:
    uv run python lnb_hakatons/pipeline/clean_data.py

Input: data/Mākslu kritika/cleaned-records-33-wide.csv
Output:
    - data/cleaned/recenzijas_clean.csv (filtered and processed data)
    - data/cleaned/recenzijas_filtered_out.csv (data that was filtered out for inspection)
"""

import pandas as pd
import re
import logging
import warnings
from typing import Dict, List, Optional, Union, Tuple
from rapidfuzz import fuzz, process

from lnb_hakatons import PROJECT_DIR

## Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## Suppress performance warnings
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

## Main variables
DATA_DIR = PROJECT_DIR / "data/Mākslu kritika"
DATA_FILE = "cleaned-records-33-wide.csv"
OUTPUT_PATH = PROJECT_DIR / "data/cleaned/recenzijas_clean_check.csv"
FILTERED_OUT_PATH = PROJECT_DIR / "data/cleaned/recenzijas_filtered_out.csv"
KEEP_OTHER_COLUMNS = True # keep columns that are not processed and explicitly dropped

## Helper variables
# Lauki, kurus ņemam ārā
columns_to_remove = [
    "UDK (080)",
    "UDK - 2 (080)",
    "ILUSTRĀCIJAS (300)",
    "SATURA VEIDS (336)",
    "SATURA VEIDS 2 (336)",
    "BIBLIOGRĀFIJA (504)"
]

# Laukus, kurus vajag vienkāršot (expand $$ subfields)
key_columns = [
    'AUTORS (100)',
    'RAKSTA NOSAUKUMS (245)',
    'PRIEKŠMETS - TEMATS (650)',
    'PRIEKŠMETS - ŽANRS (655)',
    'PRIEKŠMETS - ŽANRS - 2 (655)',
    'RECENZĒTAIS IZDEVUMS (787)',
    'RECENZĒTAIS IZDEVUMS - 2 (787)',
    'RECENZĒTAIS IZDEVUMS (500)',
    "RECENZĒTĀ FILMA VAI IZRĀDE (630)",
    "RECENZĒTĀ FILMA VAI IZRĀDE - 2 (630)",
    "RECENZĒTĀ FILMA VAI IZRĀDE - 3 (630)",
    "RECENZĒTĀ FILMA VAI IZRĀDE - 4 (630)",
    "RECENZĒTĀ FILMA VAI IZRĀDE -2 (630)",
    "AVOTA NOSAUKUMS (773)",
    "ELEKTRONISKĀ ADRESE (856)",
    "PAPILDRAKSTS (700)",
    "PAPILDRAKSTS - 2 (700)",
    "PRIEKŠMETS - PERSONA (600)",
    "PRIEKŠMETS - PERSONA - 2 (600)",
    "PRIEKŠMETS - PERSONA - 3 (600)",
    "PRIEKŠMETS - PERSONA - 4 (600)",
    "PRIEKŠMETS - PERSONA - 5 (600)",
    "NEKONTROLĒTS PERSONAS VĀRDS (720)",
    "NEKONTROLĒTS PERSONAS VĀRDS - 2 (720)",
    "NEKONTROLĒTS PERSONAS VĀRDS - 3 (720)",
    "NEKONTROLĒTS PERSONAS VĀRDS - 4 (720)",
    "NEKONTROLĒTS PERSONAS VĀRDS - 5 (720)",
    "PRIEKŠMETS - INSTITŪCIJA (610)",
    "PRIEKŠMETS - INSTITŪCIJA - 2 (610)",
    "PRIEKŠMETS - INSTITŪCIJA - 2 (610)2",
    "PRIEKŠMETS - INSTITŪCIJA - 3 (610)",
    "PRIEKŠMETS - INSTITŪCIJA - 4 (610)",
]

# Autoru tipi, kurus analizējam
FILTER_BY_AUTHOR_TYPE = False
AUTORS_100_4_values = ["aut", "rev"]

# Literatūras "žanri"
literature_categories = [
    'Grāmatu apskati',
    'Latgaliešu dzeja',
    'Latviešu bērnu dzeja',
    'Krievu dzeja',
    'Latviešu jaunatnes proza',
    'Latviešu fantastiskā proza',
    'Igauņu dzeja',
    'Angļu spiegu romāni',
    'Dāņu romāni',
    'Amerikāņu fantastiskā proza',
    'Zviedru detektīvromāni',
    'Čehu romāni',
    'Latviešu dienasgrāmatu proza',
    'Vācu dzeja',
    'Latviešu zinātniskā fantastika',
    'Somu dzeja',
    'Franču esejas',
    'Katalāņu romāni',
    'Grieķu dzeja',
    'Dienvidafrikāņu romāni (angļu valoda)',
    'Čehu stāsti',
    'Grieķu romāni',
    'Latīņu dzeja',
    'Zviedru jaunatnes proza',
    'Itāliešu esejas',
    'Latviešu skolas proza',
    'Krievu detektīvromāni',
    'Franču detektīvromāni',
    'Austriešu dzeja',
    'Čigānu dzeja',
    'Spāņu dzeja',
    'Armēņu vēsturiskā proza',
    'Katoļu himnas un dziesmas',
    'Franču dzeja',
    'Igauņu romāni',
    'Krievu zinātniskā fantastika',
    'Mīlas dzeja',
    'Bulgāru dzeja',
    'Azerbaidžāņu dzeja',
    'Zviedru bērnu dzeja',
    'Zviedru romāni',
    'Poļu fantastiskā proza',
    'Holandiešu romāni',
    'Latgaliešu bērnu dzeja',
    'Krievu Ziemassvētku stāsti',
    'Igauņu episkā dzeja',
    'Grieķu dzeja, hellēnisma',
    'Franču piedzīvojumu proza',
    'Krievu bērnu dzeja',
    'Čehu dzeja',
    'Latviešu romantiskā proza',
    'Vācu proza',
    'Amerikāņu lugas',
    'Amerikāņu proza',
    'Poļu bērnu dzeja',
    'Spiegu romāni',
    'Vācu lugas',
    'Latviešu stāsti',
    'Latviešu dzeja',
    'Izraēliešu proza',
    'Indiešu dzeja',
    'Franču proza',
    'Angļu dzeja',
 ]

review_types = [
    "Teātra recenzijas",
    "Literatūras recenzijas",
    "Kinofilmu recenzijas",
    "Mūzikas recenzijas",
    "Izstāžu recenzijas",
    "Operas recenzijas",
    "Televīzijas raidījumu recenzijas",
    "Dejas recenzijas",
    "Baleta recenzijas",
    "Apskati un recenzijas",
]

final_processed_columns = [
    "AUTORS (100)_4", # author type; just need rev and aut
    "AUTORS (100)_a", # author name
    "AUTORS (100)_c", # additional comment on author; needs to be normalised
    "AUTORS (100)_d", # date of birth and death; probably needs to be normalised
    # extra author
    "PAPILDRAKSTS (700)_4", # extra author name
    "PAPILDRAKSTS (700)_a", # extra author
    "PAPILDRAKSTS (700)_c", # extra author comment
    "PAPILDRAKSTS (700)_d", # extra author address
    # extra author 2
    "PAPILDRAKSTS - 2 (700)_4", # extra author note
    "PAPILDRAKSTS - 2 (700)_a", # extra author name
    "PAPILDRAKSTS - 2 (700)_c", # extra author comment
    "PAPILDRAKSTS - 2 (700)_d", # extra author address
    # title
    "RAKSTA NOSAUKUMS (245)_a", # title, need to remove trailing colon or dash, also quotation marks
    "RAKSTA NOSAUKUMS (245)_b", # sub-title, remove square brackets
    "RAKSTA NOSAUKUMS (245)_c", # author again?
    # subject
    "RECENZĒTAIS IZDEVUMS (787)_a", # reviewed author
    "RECENZĒTAIS IZDEVUMS (787)_t", # reviewed title
    "RECENZĒTAIS IZDEVUMS (787)_d", # reviewed publisher
    "RECENZĒTAIS IZDEVUMS (500)_a", # reviewed title? (all together)
    # filma vai izrāde
    "RECENZĒTĀ FILMA VAI IZRĀDE (630)_a", # film title
    "RECENZĒTĀ FILMA VAI IZRĀDE (630)_g", # film type
    "RECENZĒTĀ FILMA VAI IZRĀDE (630)_f", # year
    # source
    "AVOTA NOSAUKUMS (773)_t", # laikraksts
    "AVOTA NOSAUKUMS (773)_g", # laikraksta izdevums
    # url source
    "ELEKTRONISKĀ ADRESE (856)_u",
    # genre
    "PRIEKŠMETS - TEMATS (650)_a", # topic
    "PRIEKŠMETS - ŽANRS (655)_a", # genre
    "PRIEKŠMETS - ŽANRS (655)_x", # broader genre
    "PRIEKŠMETS - ŽANRS - 2 (655)_a", # genre 2
    "PRIEKŠMETS - ŽANRS - 2 (655)_x", # broader genre 2
    # institution
    "PRIEKŠMETS - INSTITŪCIJA (610)_a", # institution name
    "PRIEKŠMETS - INSTITŪCIJA (610)_g", # institution type
]

 ## Helper functions
def parse_marc_subfields(text: Union[str, None]) -> Dict[str, str]:
    """
    Parse MARC subfields from text containing $$ delimiters.

    Args:
        text: String containing MARC subfields with $$ delimiters

    Returns:
        dict: Dictionary with subfield codes as keys and content as values
    """
    if pd.isna(text) or text == 'NA':
        return {}

    # Pattern to match $$ followed by single character and content
    pattern = r'\$\$([a-z0-9])([^$]*)'
    matches = re.findall(pattern, str(text))

    result = {}
    for code, content in matches:
        # Clean up content (remove leading/trailing whitespace)
        clean_content = content.strip()
        if clean_content:
            result[code] = clean_content

    return result

def expand_marc_columns(df: pd.DataFrame, column_name: str, prefix: Optional[str] = None) -> pd.DataFrame:
    """
    Expand a MARC column into separate subfield columns.

    Args:
        df: DataFrame containing MARC data
        column_name: Name of the column to expand
        prefix: Optional prefix for new column names

    Returns:
        DataFrame: Original DataFrame with new MARC subfield columns
    """
    if prefix is None:
        prefix = column_name

    # Parse all MARC subfields in the column
    parsed_data = df[column_name].apply(parse_marc_subfields)

    # Collect all unique subfield codes
    all_codes = set()
    for subfields in parsed_data:
        all_codes.update(subfields.keys())

    # Create new columns for each subfield code
    for code in sorted(all_codes):
        new_col_name = f"{prefix}_{code}"
        df[new_col_name] = parsed_data.apply(lambda x: x.get(code, None))

    return df



def change_name_pattern(text: Union[str, None]) -> Union[str, None]:
    """
    Change the pattern "Surname, Name" to "Name Surname"

    Handles various surname patterns including:
    - Simple surnames: "Smith, John" -> "John Smith"
    - Hyphenated surnames: "Lukšo-Ražinska, Elizabete" -> "Elizabete Lukšo-Ražinska"
    - Multiple surnames: "van der Berg, Jan" -> "Jan van der Berg"
    - Names with apostrophes: "O'Connor, Mary" -> "Mary O'Connor"
    - Names with periods: "van der Berg, J." -> "J. van der Berg"
    - Single names with trailing comma: "Sjón," -> "Sjón"

    Args:
        text: Name in "Surname, Name" format

    Returns:
        Name in "Name Surname" format, or original text if no pattern matches
    """
    if pd.isna(text) or not text:
        return text

    text = str(text).strip()

    # Pattern to match surname (including hyphens, spaces, apostrophes, periods) followed by comma and first name
    # [^,]+ matches everything up to the comma (handles complex surnames)
    pattern = r'([^,]+),\s*([^,]+)'

    match = re.search(pattern, text)
    if match:
        surname = match.group(1).strip()
        first_name = match.group(2).strip()
        return f"{first_name} {surname}"

    # If no match, strip any trailing comma (handles cases like "Sjón,")
    return text.rstrip(',')



def create_uncontrolled_name_columns() -> List[str]:
    """Create sub-field columns for uncontrolled name fields"""
    sub_fields = ["_4", "_a", "_c", "_d"]
    uncontrolled_name_columns = []
    for i in range(1, 6):
        if i == 1:
            i = ""
        else:
            i = f" - {i}"
        col_name = f"NEKONTROLĒTS PERSONAS VĀRDS{i} (720)"
        columns = [col_name + sub_field for sub_field in sub_fields]
        uncontrolled_name_columns += columns
    return uncontrolled_name_columns


def create_persona_columns() -> List[str]:
    """Create sub-field columns for PRIEKŠMETS - PERSONA fields"""
    sub_fields = ["_a", "_c", "_d"]
    persona_columns = []
    for i in range(1, 6):
        if i == 1:
            i = ""
        else:
            i = f" - {i}"
        col_name = f"PRIEKŠMETS - PERSONA{i} (600)"
        columns = [col_name + sub_field for sub_field in sub_fields]
        persona_columns += columns
    return persona_columns


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
        score_cutoff=50  # Accept matches with 60% or higher similarity (avoids bad matches)
    )

    if result is None:
        return None, None

    best_match_name, score, index = result

    # Get the corresponding dates
    best_match_dates = valid_personas[index][1]

    return best_match_name, best_match_dates


def extract_director_from_245(text: Union[str, None]) -> Optional[str]:
    """
    Extract director name(s) from MARC (245)_b subfield text.

    Looks for patterns like (režisors Name Surname), (rež. Name Surname), etc.
    Handles various declensions: režisors, režisore, režisori, režisores, rež.
    Supports multiple directors separated by commas.
    Handles additional words between director title and name (e.g., "režisors un scenārists FirstName LastName").

    Args:
        text: The text from (245)_b subfield

    Returns:
        Director name(s) if found, None otherwise
    """
    if pd.isna(text) or not text:
        return None

    # First, find the opening parenthesis and director title
    director_title_pattern = r'\((?:rež(?:isors?|isore?|isori|isores?|\.)|режиссёр(?:а|ы|ом|у|е|ов|ям|ями|ях)?|режиссер(?:а|ы|ом|у|е|ов|ям|ями|ях)?)'

    match = re.search(director_title_pattern, text, re.IGNORECASE)
    if not match:
        return None

    # Get the position after the director title
    start_pos = match.end()

    # Find the closing parenthesis
    end_pos = text.find(')', start_pos)
    if end_pos == -1:
        return None

    # Extract everything between the director title and closing parenthesis
    directors_text = text[start_pos:end_pos].strip()

    # Clean up: remove any leading words that aren't names (like "un scenārists")
    # Split by spaces and find the first capitalized word
    words = directors_text.split()
    name_start_idx = 0

    for i, word in enumerate(words):
        # Check if this looks like a name (starts with capital letter)
        if word and word[0].isupper():
            name_start_idx = i
            break

    # Take everything from the first capitalized word to the end
    directors_text = ' '.join(words[name_start_idx:])

    if not directors_text:
        return None

    # Split by comma and clean up each director name
    directors = []
    for director in directors_text.split(','):
        director = director.strip()
        # Clean up the name (remove extra spaces, normalize)
        director = re.sub(r'\s+', ' ', director)
        if director:  # Only add non-empty names
            directors.append(director)

    directors = ", ".join(directors)
    return directors if directors else None


def extract_title_from_245(text: Union[str, None]) -> Optional[str]:
    """
    Extract title from MARC (245)_b subfield text.

    Looks for the first phrase in double quotes, typically after words like "filma", "izrāde", etc.

    Args:
        text: The text from (245)_b subfield

    Returns:
        Title if found, None otherwise
    """
    if pd.isna(text) or not text:
        return None

    # Pattern to match content in double quotes
    # Looks for the first occurrence of text in quotes
    title_pattern = r'"([^"]+)"'

    match = re.search(title_pattern, text)
    if match:
        title = match.group(1).strip()
        return title

    return None


def extract_author_from_500(text: Union[str, None]) -> Optional[str]:
    """
    Extract author from MARC (500)_a field text.

    Looks for author name in curly braces {Surname, Name.} format.
    Removes trailing full stop from the name.

    Args:
        text: The text from (500)_a field

    Returns:
        Author name if found, None otherwise
    """
    if pd.isna(text) or not text:
        return None

    # Pattern to match author in curly braces
    author_pattern = r'\{([^}]+)\}'

    match = re.search(author_pattern, text)
    if match:
        author = match.group(1).strip()
        # Remove trailing full stop if present
        author = author.rstrip('.')
        # Apply name pattern change (Surname, Name -> Name Surname)
        author = change_name_pattern(author)
        return author

    return None


def extract_title_from_500(text: Union[str, None]) -> Optional[str]:
    """
    Extract title from MARC (500)_a field text.

    Looks for title between closing brace } and slash /.

    Args:
        text: The text from (500)_a field

    Returns:
        Title if found, None otherwise
    """
    if pd.isna(text) or not text:
        return None

    # Pattern to match title between } and /
    title_pattern = r'\}\s*([^/]+?)\s*/'

    match = re.search(title_pattern, text)
    if match:
        title = match.group(1).strip()
        # Clean up title (remove extra spaces, normalize)
        title = re.sub(r'\s+', ' ', title)
        return title

    return None


def extract_publisher_from_500(text: Union[str, None]) -> Optional[str]:
    """
    Extract publisher from MARC (500)_a field text.

    Looks for publisher between colon after slash and comma.

    Args:
        text: The text from (500)_a field

    Returns:
        Publisher if found, None otherwise
    """
    if pd.isna(text) or not text:
        return None

    # Pattern to match publisher between colon after slash and comma
    # First find the slash, then look for colon after it, then capture until comma
    publisher_pattern = r'/\s*[^:]*:\s*([^,]+)'

    match = re.search(publisher_pattern, text)
    if match:
        publisher = match.group(1).strip()
        # Clean up publisher (remove extra spaces, normalize)
        publisher = re.sub(r'\s+', ' ', publisher)
        return publisher

    return None


def extract_director_from_630_g(text: Union[str, None]) -> Optional[str]: # noqa: C901
    """
    Extract director/author name(s) from RECENZĒTĀ FILMA VAI IZRĀDE (630)_g field text.

    Looks for text after the ":" symbol, strips it, and removes trailing closing parenthesis.
    Handles multiple comma-separated names by applying name pattern transformation to each.

    Examples:
        "(teātra izrāde : Jānis Balodis, Anna Belkovska, Viesturs Balodis)."
        "(filma : režisors Surname, Name)"

    Args:
        text: The text from (630)_g field

    Returns:
        Director/author name(s) if found after ":", None otherwise
    """
    if pd.isna(text) or not text:
        return None

    # Find the colon and extract everything after it
    if ':' in str(text):
        parts = str(text).split(':', 1)
        if len(parts) > 1:
            director = parts[1].strip()
            # Remove trailing closing parenthesis and periods
            director = director.rstrip('.)')
            director = director.rstrip(')')
            director = director.strip()

            if not director:
                return None

            # Check if there are multiple names separated by semicolons
            # (common separator for multiple people in MARC)
            if ';' in director:
                names = [n.strip() for n in director.split(';')]
                processed_names = [change_name_pattern(name) for name in names if name]
                return ", ".join(processed_names)

            # Try to detect multiple names by looking for pattern:
            # "FirstWord SecondWord, FirstWord SecondWord"
            # This handles both "Name Surname, Name Surname" and "Surname, Name, Surname, Name"

            # Split by comma and check if we have multiple complete names
            potential_names = [n.strip() for n in director.split(',')]

            # If we have multiple parts and at least one has 2+ words (indicating "Name Surname" format)
            if len(potential_names) > 1:
                # Check if parts look like separate complete names (have spaces, indicating multi-word names)
                # vs. "Surname, Name" pattern (first part no space, second part might have space)
                first_part_words = len(potential_names[0].split())

                # If first part has 2+ words, these are likely already formatted "Name Surname" entries
                if first_part_words >= 2:
                    # These are complete "Name Surname" entries, just clean each one
                    processed_names = [change_name_pattern(name) for name in potential_names if name]
                    return ", ".join(processed_names)

                # Otherwise, try to detect "Surname, Name" pairs
                # Pattern: consecutive pairs of "word, word"
                if len(potential_names) % 2 == 0:
                    # Even number of parts - might be "Surname, Name, Surname, Name"
                    names = []
                    for i in range(0, len(potential_names), 2):
                        if i + 1 < len(potential_names):
                            # Reconstruct "Surname, Name" and transform
                            pair = f"{potential_names[i]}, {potential_names[i+1]}"
                            names.append(change_name_pattern(pair))
                    if names:
                        return ", ".join(names)

            # Single name or simple case - apply pattern transformation
            return change_name_pattern(director)

    return None


def deduplicate_author_lists(row: pd.Series) -> Tuple[List[str], List[str]]:
    """
    Create deduplicated lists of authors (names only and names with dates).

    Collects authors from all PRIEKŠMETS - PERSONA fields,
    deduplicates based on name+dates combination while maintaining order.

    Args:
        row: DataFrame row containing author fields

    Returns:
        Tuple of (visi_autori, visi_autori_gadi) - deduplicated lists in corresponding order
    """
    author_pairs = []

    # Collect from PRIEKŠMETS - PERSONA fields
    persona_sources = [
        ("PRIEKŠMETS - PERSONA (600)_a", "PRIEKŠMETS - PERSONA (600)_d"),
        ("PRIEKŠMETS - PERSONA - 2 (600)_a", "PRIEKŠMETS - PERSONA - 2 (600)_d"),
        ("PRIEKŠMETS - PERSONA - 3 (600)_a", "PRIEKŠMETS - PERSONA - 3 (600)_d"),
        ("PRIEKŠMETS - PERSONA - 4 (600)_a", "PRIEKŠMETS - PERSONA - 4 (600)_d"),
        ("PRIEKŠMETS - PERSONA - 5 (600)_a", "PRIEKŠMETS - PERSONA - 5 (600)_d"),
    ]

    for name_field, date_field in persona_sources:
        name = row.get(name_field)
        dates = row.get(date_field)

        # Skip if name is empty/null
        if pd.isna(name) or name == "":
            continue

        # Clean up name: remove periods followed by spaces (e.g., "Elina. Cērpa" -> "Elina Cērpa")
        name = str(name).replace(". ", " ").strip()

        # Combine name with dates if dates exist
        if pd.notna(dates) and dates != "":
            # Clean up dates (remove trailing period if present)
            dates_clean = str(dates).rstrip('.')
            name_with_dates = f"{name} {dates_clean}".strip()
        else:
            name_with_dates = name

        author_pairs.append((name, name_with_dates))

    # Deduplicate based on normalized name_with_dates while preserving order
    seen = set()
    visi_autori = []
    visi_autori_gadi = []

    for name, name_with_dates in author_pairs:
        # Normalize the key for deduplication (remove trailing periods, extra spaces)
        normalized_key = name_with_dates.rstrip('.').strip()

        if normalized_key not in seen:
            seen.add(normalized_key)
            visi_autori.append(name)
            visi_autori_gadi.append(name_with_dates)

    return visi_autori, visi_autori_gadi


def deduplicate_reviewer_lists(row: pd.Series) -> Tuple[List[str], List[str]]:
    """
    Create deduplicated lists of reviewers (names only and names with dates).

    Collects reviewers from AUTORS (100) and PAPILDRAKSTS (700) fields,
    filtering out those with roles 'ive', 'aqt', or 'trl' in the _4 subfield,
    and deduplicates based on name+dates combination while maintaining order.

    Args:
        row: DataFrame row containing reviewer fields

    Returns:
        Tuple of (visi_recenzenti, visi_recenzenti_gadi) - deduplicated lists in corresponding order
    """
    reviewer_pairs = []

    # Define sources with their role fields
    reviewer_sources = [
        ("AUTORS (100)_a", "AUTORS (100)_d", "AUTORS (100)_4"),
        ("PAPILDRAKSTS (700)_a", "PAPILDRAKSTS (700)_d", "PAPILDRAKSTS (700)_4"),
        ("PAPILDRAKSTS - 2 (700)_a", "PAPILDRAKSTS - 2 (700)_d", "PAPILDRAKSTS - 2 (700)_4"),
    ]

    # Roles to exclude
    excluded_roles = ["ive", "aqt", "trl"]

    for name_field, date_field, role_field in reviewer_sources:
        name = row.get(name_field)
        dates = row.get(date_field)
        role = row.get(role_field)

        # Skip if name is empty/null
        if pd.isna(name) or name == "":
            continue

        # Skip if role is one of the excluded roles
        if pd.notna(role) and str(role).strip().lower() in excluded_roles:
            continue

        # Combine name with dates if dates exist
        if pd.notna(dates) and dates != "":
            # Clean up dates (remove trailing period if present)
            dates_clean = str(dates).rstrip('.')
            name_with_dates = f"{name} {dates_clean}".strip()
        else:
            name_with_dates = name

        reviewer_pairs.append((name, name_with_dates))

    # Deduplicate based on normalized name_with_dates while preserving order
    seen = set()
    visi_recenzenti = []
    visi_recenzenti_gadi = []

    for name, name_with_dates in reviewer_pairs:
        # Normalize the key for deduplication (remove trailing periods, extra spaces)
        normalized_key = name_with_dates.rstrip('.').strip()

        if normalized_key not in seen:
            seen.add(normalized_key)
            visi_recenzenti.append(name)
            visi_recenzenti_gadi.append(name_with_dates)

    return visi_recenzenti, visi_recenzenti_gadi


def remove_reviewers_from_authors(row: pd.Series) -> Tuple[List[str], List[str]]:
    """
    Remove reviewers from author lists within the same row.

    Filters out any authors who are also reviewers in the same record,
    using case-insensitive exact name matching.

    Args:
        row: DataFrame row containing both author and reviewer fields

    Returns:
        Tuple of (filtered_visi_autori, filtered_visi_autori_gadi)
    """
    visi_autori = row.get("visi_autori", [])
    visi_autori_gadi = row.get("visi_autori_gadi", [])
    visi_recenzenti = row.get("visi_recenzenti", [])

    if not isinstance(visi_autori, list) or not isinstance(visi_recenzenti, list):
        return visi_autori, visi_autori_gadi

    # Create set of reviewer names (normalized)
    recenzenti_set = {str(name).strip().lower() for name in visi_recenzenti if pd.notna(name)}

    # Filter authors
    filtered_autori = []
    filtered_autori_gadi = []

    for i, autor in enumerate(visi_autori):
        if pd.isna(autor):
            continue
        autor_normalized = str(autor).strip().lower()
        if autor_normalized not in recenzenti_set:
            filtered_autori.append(autor)
            if i < len(visi_autori_gadi):
                filtered_autori_gadi.append(visi_autori_gadi[i])

    return filtered_autori, filtered_autori_gadi


def collect_aqt_reviewers(row: pd.Series) -> Tuple[List[str], List[str]]:
    """
    Collect reviewers with role "aqt" (quoted authors) from 100/700 fields.

    Args:
        row: DataFrame row containing author fields

    Returns:
        Tuple of (reviewer_names, reviewer_names_with_dates)
    """
    reviewer_pairs = []

    reviewer_sources = [
        ("AUTORS (100)_a", "AUTORS (100)_d", "AUTORS (100)_4"),
        ("PAPILDRAKSTS (700)_a", "PAPILDRAKSTS (700)_d", "PAPILDRAKSTS (700)_4"),
        ("PAPILDRAKSTS - 2 (700)_a", "PAPILDRAKSTS - 2 (700)_d", "PAPILDRAKSTS - 2 (700)_4"),
    ]

    for name_field, date_field, role_field in reviewer_sources:
        name = row.get(name_field)
        dates = row.get(date_field)
        role = row.get(role_field)

        if pd.isna(name) or name == "":
            continue

        if pd.notna(role) and str(role).strip().lower() == "aqt":
            if pd.notna(dates) and dates != "":
                dates_clean = str(dates).rstrip('.')
                name_with_dates = f"{name} {dates_clean}".strip()
            else:
                name_with_dates = name
            reviewer_pairs.append((name, name_with_dates))

    seen = set()
    visi_recenzenti = []
    visi_recenzenti_gadi = []

    for name, name_with_dates in reviewer_pairs:
        normalized_key = name_with_dates.rstrip('.').strip()
        if normalized_key not in seen:
            seen.add(normalized_key)
            visi_recenzenti.append(name)
            visi_recenzenti_gadi.append(name_with_dates)

    return visi_recenzenti, visi_recenzenti_gadi


def parse_245c_names(text: Union[str, None]) -> List[str]:
    """
    Parse comma-separated reviewer names from RAKSTA NOSAUKUMS (245)_c field.

    Args:
        text: Comma-separated names from (245)_c field

    Returns:
        List of parsed and formatted names
    """
    if pd.isna(text) or not text:
        return []

    names = []
    for name in str(text).split(','):
        name = name.strip()
        if name:
            formatted_name = change_name_pattern(name)
            if formatted_name:
                names.append(formatted_name)

    return names


def build_reviewer_lookup(df: pd.DataFrame) -> Dict[str, Tuple[str, Optional[str]]]:
    """
    Build a lookup dictionary of all known reviewers in the dataset.

    Args:
        df: DataFrame with visi_recenzenti and visi_recenzenti_gadi columns

    Returns:
        Dictionary mapping normalized_name -> (full_name, dates)
    """
    reviewer_lookup = {}

    for _, row in df.iterrows():
        recenzenti = row.get("visi_recenzenti", [])
        recenzenti_gadi = row.get("visi_recenzenti_gadi", [])

        if not isinstance(recenzenti, list):
            continue

        for i, name in enumerate(recenzenti):
            if pd.isna(name) or name == "":
                continue

            normalized_name = str(name).strip().lower()

            if normalized_name not in reviewer_lookup:
                name_with_dates = recenzenti_gadi[i] if i < len(recenzenti_gadi) else name

                dates = None
                if pd.notna(name_with_dates) and name_with_dates != name:
                    dates = name_with_dates.replace(name, "").strip()

                reviewer_lookup[normalized_name] = (str(name), dates)

    return reviewer_lookup


def impute_missing_reviewers(row: pd.Series, reviewer_lookup: Dict[str, Tuple[str, Optional[str]]]) -> Tuple[List[str], List[str], str]:
    """
    Impute missing reviewers using fallback sources.

    Tries in order:
    1. If reviewers exist, return them as-is
    2. Try collecting from aqt roles in 100/700 fields
    3. Try parsing from RAKSTA NOSAUKUMS (245)_c and lookup dates

    Args:
        row: DataFrame row
        reviewer_lookup: Global reviewer name->dates lookup dictionary

    Returns:
        Tuple of (visi_recenzenti, visi_recenzenti_gadi, imputation_source)
        imputation_source can be: 'original', 'aqt', '245c', or 'none'
    """
    visi_recenzenti = row.get("visi_recenzenti", [])
    visi_recenzenti_gadi = row.get("visi_recenzenti_gadi", [])

    if isinstance(visi_recenzenti, list) and len(visi_recenzenti) > 0:
        return visi_recenzenti, visi_recenzenti_gadi, 'original'

    recenzenti, recenzenti_gadi = collect_aqt_reviewers(row)
    if len(recenzenti) > 0:
        return recenzenti, recenzenti_gadi, 'aqt'

    names_245c = parse_245c_names(row.get("RAKSTA NOSAUKUMS (245)_c"))
    if len(names_245c) == 0:
        return [], [], 'none'

    recenzenti = []
    recenzenti_gadi = []

    for name in names_245c:
        normalized_name = str(name).strip().lower()
        recenzenti.append(name)

        if normalized_name in reviewer_lookup:
            full_name, dates = reviewer_lookup[normalized_name]
            if dates:
                recenzenti_gadi.append(f"{name} {dates}".strip())
            else:
                recenzenti_gadi.append(name)
        else:
            recenzenti_gadi.append(name)

    return recenzenti, recenzenti_gadi, '245c'


def build_author_lookup(df: pd.DataFrame) -> Dict[str, str]:
    """
    Build a lookup dictionary of author names to names with dates.

    Args:
        df: DataFrame with author columns

    Returns:
        Dictionary mapping normalized_name -> name_with_dates
    """
    author_lookup = {}

    for _, row in df.iterrows():
        galvenais_autors = row.get("Galvenais autors (600)")
        galvenais_autors_gadi = row.get("Galvenais autors un gadi (600)")

        if pd.notna(galvenais_autors) and galvenais_autors != "":
            normalized_name = str(galvenais_autors).strip().lower()

            if normalized_name not in author_lookup and pd.notna(galvenais_autors_gadi) and galvenais_autors_gadi != "":
                author_lookup[normalized_name] = str(galvenais_autors_gadi)

    return author_lookup


def impute_authors_from_galvenais(row: pd.Series, author_lookup: Dict[str, str], skip_ids: set) -> Tuple[List[str], List[str]]:
    """
    Impute empty author lists from Galvenais autors field.

    Args:
        row: DataFrame row
        author_lookup: Global author name->name_with_dates lookup dictionary
        skip_ids: Set of IDs to skip (faulty data)

    Returns:
        Tuple of (visi_autori, visi_autori_gadi)
    """
    record_id = row.get("ID")
    if record_id in skip_ids:
        return row.get("Autori (list)", []), row.get("Autori un gadi (list)", [])

    visi_autori = row.get("Autori (list)", [])
    visi_autori_gadi = row.get("Autori un gadi (list)", [])

    if isinstance(visi_autori, list) and len(visi_autori) > 0:
        return visi_autori, visi_autori_gadi

    galvenais_autors = row.get("Galvenais autors")
    if pd.isna(galvenais_autors) or galvenais_autors == "":
        return [], []

    normalized_name = str(galvenais_autors).strip().lower()

    if normalized_name in author_lookup:
        name_with_dates = author_lookup[normalized_name]
        return [galvenais_autors], [name_with_dates]
    else:
        return [galvenais_autors], [galvenais_autors]


def collect_reviewed_works(row: pd.Series) -> Tuple[List[str], str]: # noqa: C901
    """
    Collect all reviewed works from various MARC fields.

    Checks multiple sources:
    - RECENZĒTĀ FILMA VAI IZRĀDE (630) fields (all variants)
    - RECENZĒTAIS IZDEVUMS (787) fields (all variants)
    - Extracted titles from (245)_b and (500)_a

    Args:
        row: DataFrame row containing reviewed work fields

    Returns:
        Tuple of (unique_works_list, primary_source)
        primary_source can be: '630', '787', '500', '245', or 'none'
    """
    works = []
    primary_source = 'none'

    # Sources in priority order (films/performances, then books, then fallbacks)
    # Group 1: 630 fields
    field_630 = [
        "RECENZĒTĀ FILMA VAI IZRĀDE (630)_a",
        "RECENZĒTĀ FILMA VAI IZRĀDE - 2 (630)_a",
        "RECENZĒTĀ FILMA VAI IZRĀDE - 3 (630)_a",
        "RECENZĒTĀ FILMA VAI IZRĀDE - 4 (630)_a",
        "RECENZĒTĀ FILMA VAI IZRĀDE -2 (630)_a",
    ]

    # Group 2: 787 fields (direct)
    field_787_direct = [
        "RECENZĒTAIS IZDEVUMS (787)_t",
        "RECENZĒTAIS IZDEVUMS - 2 (787)_t",
    ]

    # Group 3: 787 extracted (from 500)
    field_787_extracted = ["(787)_title"]

    # Group 4: 245 extracted
    field_245 = ["(245)_title"]

    # Check 630 fields
    for field in field_630:
        value = row.get(field)
        if pd.notna(value) and value != "":
            cleaned_value = str(value).strip()
            if ":" in cleaned_value:
                cleaned_value = cleaned_value.split(":")[0].strip()
            if cleaned_value:
                if cleaned_value == "7":
                    cleaned_value = "007: Spektrs"
                works.append(cleaned_value)
                if primary_source == 'none':
                    primary_source = '630'

    # Check 787 direct fields
    for field in field_787_direct:
        value = row.get(field)
        if pd.notna(value) and value != "":
            cleaned_value = str(value).strip()
            if ":" in cleaned_value:
                cleaned_value = cleaned_value.split(":")[0].strip()
            if cleaned_value:
                works.append(cleaned_value)
                if primary_source == 'none':
                    primary_source = '787'

    # Check 787 extracted (from 500)
    for field in field_787_extracted:
        value = row.get(field)
        if pd.notna(value) and value != "":
            cleaned_value = str(value).strip()
            if ":" in cleaned_value:
                cleaned_value = cleaned_value.split(":")[0].strip()
            if cleaned_value:
                works.append(cleaned_value)
                if primary_source == 'none':
                    primary_source = '500'

    # Check 245 field
    for field in field_245:
        value = row.get(field)
        if pd.notna(value) and value != "":
            cleaned_value = str(value).strip()
            if ":" in cleaned_value:
                cleaned_value = cleaned_value.split(":")[0].strip()
            if cleaned_value:
                works.append(cleaned_value)
                if primary_source == 'none':
                    primary_source = '245'

    # Deduplicate while preserving order
    seen = set()
    unique_works = []
    for work in works:
        normalized = work.lower().strip()
        if normalized not in seen:
            seen.add(normalized)
            unique_works.append(work)

    return unique_works, primary_source


def collect_institutions(row: pd.Series) -> Tuple[List[str], str]: # noqa: C901
    """
    Collect all institutions from PRIEKŠMETS - INSTITŪCIJA fields.

    Args:
        row: DataFrame row containing institution fields

    Returns:
        Tuple of (unique_institutions_list, primary_source)
        primary_source can be: '610', '787_publisher', or 'none'
    """
    institutions = []
    primary_source = 'none'

    # Group 1: PRIEKŠMETS - INSTITŪCIJA (610) fields
    field_610 = [
        "PRIEKŠMETS - INSTITŪCIJA (610)_a",
        "PRIEKŠMETS - INSTITŪCIJA - 2 (610)_a",
        "PRIEKŠMETS - INSTITŪCIJA - 2 (610)2_a",
        "PRIEKŠMETS - INSTITŪCIJA - 3 (610)_a",
        "PRIEKŠMETS - INSTITŪCIJA - 4 (610)_a",
    ]

    # Group 2: Publisher field as fallback
    field_publisher = ["(787)_publisher"]

    # Check 610 fields
    for field in field_610:
        value = row.get(field)

        if pd.isna(value) or value == "":
            continue

        # Clean up the value
        cleaned_value = str(value).strip()

        # Remove periods
        cleaned_value = cleaned_value.replace(".", "")

        # Fix if there's a colon: take text between colon and comma
        if ":" in cleaned_value:
            parts = cleaned_value.split(":")
            if len(parts) > 1:
                after_colon = parts[1]
                if "," in after_colon:
                    cleaned_value = after_colon.split(",")[0].strip()
                else:
                    cleaned_value = after_colon.strip()

        # Skip if empty after cleaning
        if not cleaned_value:
            continue

        # Apply specific normalization rules
        if cleaned_value == "Latvijas Nacionālā opera":
            cleaned_value = "Latvijas Nacionālā opera un balets"

        institutions.append(cleaned_value)
        if primary_source == 'none':
            primary_source = '610'

    # Check publisher field as fallback
    for field in field_publisher:
        value = row.get(field)

        if pd.isna(value) or value == "":
            continue

        # Clean up the value
        cleaned_value = str(value).strip()

        # Remove periods
        cleaned_value = cleaned_value.replace(".", "")

        # Fix if there's a colon: take text between colon and comma
        if ":" in cleaned_value:
            parts = cleaned_value.split(":")
            if len(parts) > 1:
                after_colon = parts[1]
                if "," in after_colon:
                    cleaned_value = after_colon.split(",")[0].strip()
                else:
                    cleaned_value = after_colon.strip()

        # Skip if empty after cleaning
        if not cleaned_value:
            continue

        # Apply specific normalization rules
        if cleaned_value == "Latvijas Nacionālā opera":
            cleaned_value = "Latvijas Nacionālā opera un balets"

        institutions.append(cleaned_value)
        if primary_source == 'none':
            primary_source = '787_publisher'

    # Deduplicate while preserving order
    seen = set()
    unique_institutions = []
    for inst in institutions:
        normalized = inst.lower().strip()
        if normalized not in seen:
            seen.add(normalized)
            unique_institutions.append(inst)

    return unique_institutions, primary_source



if __name__ == "__main__":
    ## Load the data
    data_df = (
        pd.read_csv(DATA_DIR / DATA_FILE, sep=';')
        .drop(columns=columns_to_remove, axis=1)
        .rename(columns={
            "PRIEKŠMETS - PERSONA 2 (600)": "PRIEKŠMETS - PERSONA - 2 (600)"
        })
    )

    ## Expand MARC columns
    # Create a simplified version of the data with expanded MARC columns
    simplified_df = data_df.copy()

    # Debug: Check if original columns exist
    zanr_columns = [col for col in data_df.columns if "ŽANRS" in col]
    logger.info(f"Original ŽANRS columns in data: {zanr_columns}")

    for col in key_columns:
        if col in simplified_df.columns:
            #logger.info(f"Expanding column: {col}")
            simplified_df = expand_marc_columns(simplified_df, col)
        else:
            #logger.info(f"Column not found (skipping): {col}")
            pass

    # Keep the rest of the columns
    keep_columns = list(set(data_df.columns).difference(set(key_columns)))

    # Add all the new MARC subfield columns
    marc_columns = [col for col in simplified_df.columns if '_' in col]
    all_columns = keep_columns + marc_columns

    # Create the simplified dataframe
    simplified_df = simplified_df[all_columns]

    final_columns = final_processed_columns + create_uncontrolled_name_columns() + create_persona_columns()

    # Add rest of the columns
    if KEEP_OTHER_COLUMNS:
        final_columns_all = final_columns + sorted(keep_columns)
    else:
        final_columns_all = final_columns

    final_columns_all = [col for col in final_columns_all if col in simplified_df.columns]

    ## Filtering
    logger.info(f"Original number of rows: {len(data_df)}")

    # Create a copy for filtering operations
    working_df = simplified_df.copy()[final_columns_all]

    # First filter: author type (only if FILTER_BY_AUTHOR_TYPE is True)
    if FILTER_BY_AUTHOR_TYPE:
        author_filter = working_df["AUTORS (100)_4"].isin(AUTORS_100_4_values)
        filtered_by_author = working_df[author_filter]
        filtered_out_by_author = working_df[~author_filter]

        logger.info(f"Filtering by author type: {AUTORS_100_4_values}")
        logger.info(f"Number of rows after filtering authors: {len(filtered_by_author)}")
        logger.info(f"Number of rows filtered out by author type: {len(filtered_out_by_author)}")
    else:
        filtered_by_author = working_df
        filtered_out_by_author = pd.DataFrame()
        logger.info("Skipping author type filtering")

    # Second filter: review type
    ir_recenzija = (
        filtered_by_author["PRIEKŠMETS - ŽANRS (655)_a"].fillna("").str.lower().str.contains("recenzija") |
        filtered_by_author["PRIEKŠMETS - ŽANRS - 2 (655)_a"].fillna("").str.lower().str.contains("recenzija")
    )
    ir_gramata = (
        filtered_by_author["PRIEKŠMETS - ŽANRS (655)_a"].fillna("").str.lower().str.contains("grāmatu apskati") |
        filtered_by_author["PRIEKŠMETS - ŽANRS - 2 (655)_a"].fillna("").str.lower().str.contains("grāmatu apskati")
    )
    ir_vesture = (
        filtered_by_author["PRIEKŠMETS - ŽANRS (655)_x"].fillna("").str.lower().str.contains("vēsture un kritika") |
        filtered_by_author["PRIEKŠMETS - ŽANRS - 2 (655)_x"].fillna("").str.lower().str.contains("vēsture un kritika")
    )

    review_filter = ir_recenzija | ir_vesture | ir_gramata
    final_df = filtered_by_author[review_filter]
    filtered_out_by_review = filtered_by_author[~review_filter]

    logger.info(f"Number of rows after filtering recenzijas: {len(final_df)}")
    logger.info(f"Number of rows filtered out by review type: {len(filtered_out_by_review)}")

    # Combine all filtered-out data
    filtered_out_list = []
    if not filtered_out_by_author.empty:
        filtered_out_list.append(filtered_out_by_author.assign(filter_reason="Author type not 'aut' or 'rev'"))
    if not filtered_out_by_review.empty:
        filtered_out_list.append(filtered_out_by_review.assign(filter_reason="Not a review, book review, or history/criticism"))

    all_filtered_out = pd.concat(filtered_out_list, ignore_index=True) if filtered_out_list else pd.DataFrame()

    logger.info(f"Total rows filtered out: {len(all_filtered_out)}")

    ## Processing

    final_df = (
        final_df
        .assign(**{
            # Change the format of the author from Surname, Name to Name Surname
            "AUTORS (100)_a": lambda df: df["AUTORS (100)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ),
            "PAPILDRAKSTS (700)_a": lambda df: df["PAPILDRAKSTS (700)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ),
            "PAPILDRAKSTS - 2 (700)_a": lambda df: df["PAPILDRAKSTS - 2 (700)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ),
            "NEKONTROLĒTS PERSONAS VĀRDS (720)_a": lambda df: df["NEKONTROLĒTS PERSONAS VĀRDS (720)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ),
            "NEKONTROLĒTS PERSONAS VĀRDS - 2 (720)_a": lambda df: df["NEKONTROLĒTS PERSONAS VĀRDS - 2 (720)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ),
            "NEKONTROLĒTS PERSONAS VĀRDS - 3 (720)_a": lambda df: df["NEKONTROLĒTS PERSONAS VĀRDS - 3 (720)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ),
            "NEKONTROLĒTS PERSONAS VĀRDS - 4 (720)_a": lambda df: df["NEKONTROLĒTS PERSONAS VĀRDS - 4 (720)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ),
            "NEKONTROLĒTS PERSONAS VĀRDS - 5 (720)_a": lambda df: df["NEKONTROLĒTS PERSONAS VĀRDS - 5 (720)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ),
        })
        .pipe(lambda df: (
            df.assign(**{
                "PRIEKŠMETS - PERSONA (600)_a": lambda d: d["PRIEKŠMETS - PERSONA (600)_a"].apply(
                    lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
                )
            })
            if "PRIEKŠMETS - PERSONA (600)_a" in df.columns else df
        ))
        .pipe(lambda df: (
            df.assign(**{
                "PRIEKŠMETS - PERSONA - 2 (600)_a": lambda d: d["PRIEKŠMETS - PERSONA - 2 (600)_a"].apply(
                    lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
                )
            })
            if "PRIEKŠMETS - PERSONA - 2 (600)_a" in df.columns else df
        ))
        .pipe(lambda df: (
            df.assign(**{
                "PRIEKŠMETS - PERSONA - 3 (600)_a": lambda d: d["PRIEKŠMETS - PERSONA - 3 (600)_a"].apply(
                    lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
                )
            })
            if "PRIEKŠMETS - PERSONA - 3 (600)_a" in df.columns else df
        ))
        .pipe(lambda df: (
            df.assign(**{
                "PRIEKŠMETS - PERSONA - 4 (600)_a": lambda d: d["PRIEKŠMETS - PERSONA - 4 (600)_a"].apply(
                    lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
                )
            })
            if "PRIEKŠMETS - PERSONA - 4 (600)_a" in df.columns else df
        ))
        .pipe(lambda df: (
            df.assign(**{
                "PRIEKŠMETS - PERSONA - 5 (600)_a": lambda d: d["PRIEKŠMETS - PERSONA - 5 (600)_a"].apply(
                    lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
                )
            })
            if "PRIEKŠMETS - PERSONA - 5 (600)_a" in df.columns else df
        ))
        .assign(**{
            # Combine subfields _a and _b
            "RAKSTA NOSAUKUMS (245)_ab": lambda df: df["RAKSTA NOSAUKUMS (245)_a"].fillna("") + " " + df["RAKSTA NOSAUKUMS (245)_b"].fillna(""),
            # Remove full stops in genre
            "PRIEKŠMETS - ŽANRS (655)_a": lambda df: df["PRIEKŠMETS - ŽANRS (655)_a"].str.replace(".", "").str.strip(),
            "PRIEKŠMETS - ŽANRS - 2 (655)_a": lambda df: df["PRIEKŠMETS - ŽANRS - 2 (655)_a"].str.replace(".", "").str.strip(),
            "PRIEKŠMETS - INSTITŪCIJA (610)_a": lambda df: df["PRIEKŠMETS - INSTITŪCIJA (610)_a"].str.replace(".", "").str.strip(),
        })
        # Conditionally process - 2 fields if they exist
        .pipe(lambda df: (
            df.assign(**{
                "PRIEKŠMETS - ŽANRS - 2 (655)_a": lambda d: d["PRIEKŠMETS - ŽANRS - 2 (655)_a"].str.replace(".", "").str.strip()
            })
            if "PRIEKŠMETS - ŽANRS - 2 (655)_a" in df.columns else df
        ))
        .pipe(lambda df: (
            df.assign(**{
                "PRIEKŠMETS - ŽANRS - 2 (655)_x": lambda d: d["PRIEKŠMETS - ŽANRS - 2 (655)_x"].str.replace(".", "").str.strip()
            })
            if "PRIEKŠMETS - ŽANRS - 2 (655)_x" in df.columns else df
        ))
        .assign(**{
            # remove colon from the end of the title (only the end - there might be a space before and/or after)
            "RAKSTA NOSAUKUMS (245)_a": lambda df: df["RAKSTA NOSAUKUMS (245)_a"].fillna("").str.rstrip(": /").str.strip(),
        })
        .assign(**{
            # Replace only exact matches of "Latvijas Nacionālā opera" with "Latvijas Nacionālā opera un balets"
            "PRIEKŠMETS - INSTITŪCIJA (610)_a": lambda df: df["PRIEKŠMETS - INSTITŪCIJA (610)_a"].apply(
                lambda val: "Latvijas Nacionālā opera un balets" if val == "Latvijas Nacionālā opera" else val
            )
        })
        # Extract director and title from (245)_b
        .assign(**{
            "(245)_director": lambda df: df["RAKSTA NOSAUKUMS (245)_b"].apply(
                lambda val: extract_director_from_245(val)
            ),
            "(245)_title": lambda df: df["RAKSTA NOSAUKUMS (245)_b"].apply(
                lambda val: extract_title_from_245(val)
            ),
        })
        # Extract book author, title, and publisher from (500)_a
        .assign(**{
            "(500)_author": lambda df: df["RECENZĒTAIS IZDEVUMS (500)_a"].apply(
                lambda val: extract_author_from_500(val)
            ),
            "(500)_title": lambda df: df["RECENZĒTAIS IZDEVUMS (500)_a"].apply(
                lambda val: extract_title_from_500(val)
            ),
            "(500)_publisher": lambda df: df["RECENZĒTAIS IZDEVUMS (500)_a"].apply(
                lambda val: extract_publisher_from_500(val)
            ),
        })
        # Book authors
        # Populate 787 fields with extracted data (prioritize 787 over 500_a if available)
        .assign(**{
            "(787)_author": lambda df: df["RECENZĒTAIS IZDEVUMS (787)_a"].apply(
                lambda val: change_name_pattern(val) if pd.notna(val) and val != "" else None
            ).fillna(df["(500)_author"]),
            "(787)_title": lambda df: df["RECENZĒTAIS IZDEVUMS (787)_t"].apply(
                lambda val: val if pd.notna(val) and val != "" else None
            ).fillna(df["(500)_title"]),
            "(787)_publisher": lambda df: df["RECENZĒTAIS IZDEVUMS (787)_d"].apply(
                lambda val: val if pd.notna(val) and val != "" else None
            ).fillna(df["(500)_publisher"]),
        })
        # Extract director from all (630)_g fields
        .assign(**{
            "(630)_director": lambda df: df["RECENZĒTĀ FILMA VAI IZRĀDE (630)_g"].apply(extract_director_from_630_g),
        })
        .assign(**{
            # For galvenais_autors, prioritize (630)_director, then use (787)_author, then (245)_director
            "galvenais_autors": lambda df: (
                df["(630)_director"]
                .replace("", None)
                .fillna(df["(787)_author"])
                .replace("", None)
                .fillna(df["(245)_director"])
            ),
        })
        # Fuzzy match galvenais_autors with PRIEKŠMETS - PERSONA fields
        .assign(**{
            "recenzeta_darba_autors_600": lambda df: df.apply(
                lambda row: find_best_matching_persona(
                    row["galvenais_autors"],
                    [
                        (row.get("PRIEKŠMETS - PERSONA (600)_a"), row.get("PRIEKŠMETS - PERSONA (600)_d")),
                        (row.get("PRIEKŠMETS - PERSONA - 2 (600)_a"), row.get("PRIEKŠMETS - PERSONA - 2 (600)_d")),
                        (row.get("PRIEKŠMETS - PERSONA - 3 (600)_a"), row.get("PRIEKŠMETS - PERSONA - 3 (600)_d")),
                        (row.get("PRIEKŠMETS - PERSONA - 4 (600)_a"), row.get("PRIEKŠMETS - PERSONA - 4 (600)_d")),
                        (row.get("PRIEKŠMETS - PERSONA - 5 (600)_a"), row.get("PRIEKŠMETS - PERSONA - 5 (600)_d")),
                    ]
                )[0],
                axis=1
            ),
            "_matched_dates": lambda df: df.apply(
                lambda row: find_best_matching_persona(
                    row["galvenais_autors"],
                    [
                        (row.get("PRIEKŠMETS - PERSONA (600)_a"), row.get("PRIEKŠMETS - PERSONA (600)_d")),
                        (row.get("PRIEKŠMETS - PERSONA - 2 (600)_a"), row.get("PRIEKŠMETS - PERSONA - 2 (600)_d")),
                        (row.get("PRIEKŠMETS - PERSONA - 3 (600)_a"), row.get("PRIEKŠMETS - PERSONA - 3 (600)_d")),
                        (row.get("PRIEKŠMETS - PERSONA - 4 (600)_a"), row.get("PRIEKŠMETS - PERSONA - 4 (600)_d")),
                        (row.get("PRIEKŠMETS - PERSONA - 5 (600)_a"), row.get("PRIEKŠMETS - PERSONA - 5 (600)_d")),
                    ]
                )[1],
                axis=1
            ),
        })
        .assign(**{
            "recenzeta_darba_autors_gads": lambda df: df["recenzeta_darba_autors_600"].fillna("") + " " + df["_matched_dates"].fillna(""),
        })
        .assign(**{
            "recenzeta_darba_autors_gads": lambda df: df["recenzeta_darba_autors_gads"].str.strip().replace("", None),
        })
        .drop(columns=["_matched_dates"])
        .assign(**{
            "recenzijas_tips": lambda df: df.apply(
                lambda row: (
                    # Priority 1: PRIEKŠMETS - ŽANRS (655)_a contains "recenzija"
                    row["PRIEKŠMETS - ŽANRS (655)_a"] if (pd.notna(row["PRIEKŠMETS - ŽANRS (655)_a"]) and
                                                           "recenzija" in row["PRIEKŠMETS - ŽANRS (655)_a"].lower())
                    # Priority 2: PRIEKŠMETS - ŽANRS - 2 (655)_a contains "recenzija"
                    else row["PRIEKŠMETS - ŽANRS - 2 (655)_a"] if (pd.notna(row["PRIEKŠMETS - ŽANRS - 2 (655)_a"]) and
                                                                    "recenzija" in row["PRIEKŠMETS - ŽANRS - 2 (655)_a"].lower())
                    # Priority 3: Either field is in literature_categories
                    else "Literatūras recenzijas" if (
                        (pd.notna(row["PRIEKŠMETS - ŽANRS (655)_a"]) and row["PRIEKŠMETS - ŽANRS (655)_a"] in literature_categories) or
                        (pd.notna(row["PRIEKŠMETS - ŽANRS - 2 (655)_a"]) and row["PRIEKŠMETS - ŽANRS - 2 (655)_a"] in literature_categories)
                    )
                    # Priority 4: Fall back to first field, then second field
                    else (row["PRIEKŠMETS - ŽANRS (655)_a"] if pd.notna(row["PRIEKŠMETS - ŽANRS (655)_a"])
                          else row["PRIEKŠMETS - ŽANRS - 2 (655)_a"])
                ),
                axis=1
            )
        })
        # Unique author name + year info
        .assign(**{
            "AUTORS_ad": lambda df: df["AUTORS (100)_a"].fillna("") + " " + df["AUTORS (100)_d"].fillna(""),
        })
        .astype({"GADS (008)": "int64"})
        .query("`GADS (008)` >= 2015")
        .query("`recenzijas_tips` in @review_types")
        # Format the title column: remove square brackets and the trailing / slash
        .assign(**{
            "RAKSTA NOSAUKUMS (245)_ab": lambda df: (
                df["RAKSTA NOSAUKUMS (245)_ab"]
                .str.replace("[", "")
                .str.replace("]", "")
                .str.strip()
                .str.rstrip("/")
                .str.strip()
            ),
        })
        # Create deduplicated author lists (names only and names with dates)
        .assign(**{
            "_author_tuples": lambda df: df.apply(deduplicate_author_lists, axis=1),
        })
        .assign(**{
            "visi_autori": lambda df: df["_author_tuples"].apply(lambda x: x[0]),
            "visi_autori_gadi": lambda df: df["_author_tuples"].apply(lambda x: x[1]),
        })
        .drop(columns=["_author_tuples"])
        # Track where galvenais_autors came from for later analysis
        .assign(**{
            "_galvenais_autors_source": lambda df: df.apply(
                lambda row: (
                    '630' if pd.notna(row["(630)_director"]) and row["(630)_director"] != ""
                    else '787/500' if pd.notna(row["(787)_author"]) and row["(787)_author"] != ""
                    else '245' if pd.notna(row["(245)_director"]) and row["(245)_director"] != ""
                    else 'none'
                ),
                axis=1
            )
        })
        # Create deduplicated reviewer lists (names only and names with dates)
        .assign(**{
            "_reviewer_tuples": lambda df: df.apply(deduplicate_reviewer_lists, axis=1),
        })
        .assign(**{
            "visi_recenzenti": lambda df: df["_reviewer_tuples"].apply(lambda x: x[0]),
            "visi_recenzenti_gadi": lambda df: df["_reviewer_tuples"].apply(lambda x: x[1]),
        })
        .drop(columns=["_reviewer_tuples"])
        # Collect all reviewed works from multiple sources
        .assign(**{
            "_reviewed_works_tuples": lambda df: df.apply(collect_reviewed_works, axis=1),
        })
        .assign(**{
            "visi_recenzetie_darbi": lambda df: df["_reviewed_works_tuples"].apply(lambda x: x[0]),
            "_works_source": lambda df: df["_reviewed_works_tuples"].apply(lambda x: x[1]),
        })
        .drop(columns=["_reviewed_works_tuples"])
        # Collect all institutions from multiple sources
        .assign(**{
            "_institutions_tuples": lambda df: df.apply(collect_institutions, axis=1),
        })
        .assign(**{
            "visas_institucijas": lambda df: df["_institutions_tuples"].apply(lambda x: x[0]),
            "_institutions_source": lambda df: df["_institutions_tuples"].apply(lambda x: x[1]),
        })
        .drop(columns=["_institutions_tuples"])
    )

    # Log reviewed works statistics
    works_source_counts = final_df["_works_source"].value_counts()
    logger.info("Reviewed works source breakdown:")
    logger.info(f"  - From 630 field (films/performances): {works_source_counts.get('630', 0)}")
    logger.info(f"  - From 787 field (books, direct): {works_source_counts.get('787', 0)}")
    logger.info(f"  - From 500 field (extracted into 787): {works_source_counts.get('500', 0)}")
    logger.info(f"  - From 245 field (extracted from title): {works_source_counts.get('245', 0)}")
    logger.info(f"  - No reviewed works found: {works_source_counts.get('none', 0)}")

    empty_works = final_df["visi_recenzetie_darbi"].apply(lambda x: len(x) == 0 if isinstance(x, list) else True).sum()
    logger.info(f"Total records with NO reviewed works: {empty_works}")

    # Drop the temporary column
    final_df = final_df.drop(columns=["_works_source"])

    # Log institutions statistics
    institutions_source_counts = final_df["_institutions_source"].value_counts()
    logger.info("Organizations/Institutions source breakdown:")
    logger.info(f"  - From 610 field (PRIEKŠMETS - INSTITŪCIJA): {institutions_source_counts.get('610', 0)}")
    logger.info(f"  - From 787_publisher field (publisher as fallback): {institutions_source_counts.get('787_publisher', 0)}")
    logger.info(f"  - No institutions found: {institutions_source_counts.get('none', 0)}")

    empty_institutions = final_df["visas_institucijas"].apply(lambda x: len(x) == 0 if isinstance(x, list) else True).sum()
    logger.info(f"Total records with NO institutions: {empty_institutions}")

    # Drop the temporary column
    final_df = final_df.drop(columns=["_institutions_source"])

    # Log initial reviewer statistics before imputation
    empty_reviewers_initial = final_df["visi_recenzenti"].apply(lambda x: len(x) == 0 if isinstance(x, list) else True).sum()
    has_reviewers_initial = len(final_df) - empty_reviewers_initial
    logger.info(f"BEFORE IMPUTATION: {has_reviewers_initial} records have reviewers, {empty_reviewers_initial} records have no reviewers")

    # Build reviewer lookup from the dataset for imputation
    logger.info("Building global reviewer lookup for imputation...")
    reviewer_lookup = build_reviewer_lookup(final_df)
    logger.info(f"Built reviewer lookup with {len(reviewer_lookup)} unique reviewers")

    # Continue pipeline with imputation
    final_df = (
        final_df
        # Impute missing reviewers using aqt roles and 245_c field
        .assign(**{
            "_imputed_reviewer_tuples": lambda df: df.apply(
                lambda row: impute_missing_reviewers(row, reviewer_lookup), axis=1
            ),
        })
        .assign(**{
            "visi_recenzenti": lambda df: df["_imputed_reviewer_tuples"].apply(lambda x: x[0]),
            "visi_recenzenti_gadi": lambda df: df["_imputed_reviewer_tuples"].apply(lambda x: x[1]),
            "_imputation_source": lambda df: df["_imputed_reviewer_tuples"].apply(lambda x: x[2]),
        })
        .drop(columns=["_imputed_reviewer_tuples"])
    )

    # Log imputation statistics
    imputation_counts = final_df["_imputation_source"].value_counts()
    logger.info("Imputation sources breakdown:")
    logger.info(f"  - Original (had reviewers): {imputation_counts.get('original', 0)}")
    logger.info(f"  - Imputed from aqt field: {imputation_counts.get('aqt', 0)}")
    logger.info(f"  - Imputed from 245_c field: {imputation_counts.get('245c', 0)}")
    logger.info(f"  - Still missing reviewers: {imputation_counts.get('none', 0)}")

    empty_reviewers = final_df["visi_recenzenti"].apply(lambda x: len(x) == 0 if isinstance(x, list) else True).sum()
    logger.info(f"AFTER IMPUTATION: {empty_reviewers} records still have no reviewers")

    # Drop the temporary column
    final_df = final_df.drop(columns=["_imputation_source"])

    # Continue pipeline
    final_df = (
        final_df
        # Remove reviewers from author lists
        .assign(**{
            "_filtered_author_tuples": lambda df: df.apply(remove_reviewers_from_authors, axis=1),
        })
        .assign(**{
            "visi_autori": lambda df: df["_filtered_author_tuples"].apply(lambda x: x[0]),
            "visi_autori_gadi": lambda df: df["_filtered_author_tuples"].apply(lambda x: x[1]),
        })
        .drop(columns=["_filtered_author_tuples"])
        # Create comma-separated text versions of the lists
        .assign(**{
            "visi_autori_teksts": lambda df: df["visi_autori"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else None
            ),
            "visi_autori_gadi_teksts": lambda df: df["visi_autori_gadi"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else None
            ),
            "visi_recenzenti_teksts": lambda df: df["visi_recenzenti"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else None
            ),
            "visi_recenzenti_gadi_teksts": lambda df: df["visi_recenzenti_gadi"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else None
            ),
            "visi_recenzetie_darbi_teksts": lambda df: df["visi_recenzetie_darbi"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else None
            ),
            "visas_institucijas_teksts": lambda df: df["visas_institucijas"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else None
            ),
        })
        # Detect language of the title
        # .assign(**{
        #     "RAKSTA_NOSAUKUMS_valoda": lambda df: df["RAKSTA NOSAUKUMS (245)_ab"].apply(
        #         lambda x: detect(x) if pd.notna(x) and len(str(x).strip()) > 0 else None
        #     ),
        # })
        # drop the helper columns for authors and title, and keep only the harmonised columns
        .drop(columns=[
            "(245)_director",
            "(245)_title",
            "(500)_author",
            "(500)_title",
            "(500)_publisher",
            "(787)_author",
            "(787)_title",
            "(787)_publisher",
        ])
    )

    ## Rename and reorder columns
    column_rename_map = {
        "visi_recenzenti": "Recenzenti (list)",
        "visi_recenzenti_teksts": "Recenzenti",
        "visi_recenzenti_gadi": "Recenzenti un gadi (list)",
        "visi_recenzenti_gadi_teksts": "Recenzenti un gadi",
        "AVOTA NOSAUKUMS (773)_t": "Avots",
        "PRIEKŠMETS - TEMATS (650)_a": "Temats",
        "GADS (008)": "Gads",
        "RAKSTA NOSAUKUMS (245)_ab": "Recenzijas virsraksts",
        "galvenais_autors": "Galvenais autors",
        "recenzeta_darba_autors_600": "Galvenais autors (600)",
        "recenzeta_darba_autors_gads": "Galvenais autors un gadi (600)",
        "visi_autori": "Autori (list)",
        "visi_autori_teksts": "Autori",
        "visi_autori_gadi": "Autori un gadi (list)",
        "visi_autori_gadi_teksts": "Autori un gadi",
        "visi_recenzetie_darbi": "Recenzētais darbs (list)",
        "visi_recenzetie_darbi_teksts": "Recenzētais darbs",
        "visas_institucijas": "Saistītā organizācija vai notikums (list)",
        "visas_institucijas_teksts": "Saistītā organizācija vai notikums",
        "recenzijas_tips": "Recenzijas tips",
    }

    # Rename columns
    final_df = final_df.rename(columns=column_rename_map)

    # Reorder columns: ID first, then renamed columns, then the rest
    priority_columns = ["ID"] + list(column_rename_map.values())
    other_columns = [col for col in final_df.columns if col not in priority_columns]
    final_columns_ordered = [col for col in priority_columns if col in final_df.columns] + other_columns
    final_df = final_df[final_columns_ordered]

    ## Impute empty author lists from Galvenais autors
    skip_ids = {3761104, 3739729, 3669493, 3721723, 3734207}

    # Log initial author statistics (from 600 fields)
    empty_authors_initial = final_df["Autori (list)"].apply(lambda x: len(x) == 0 if isinstance(x, list) else True).sum()
    has_authors_initial = len(final_df) - empty_authors_initial
    logger.info(f"WORK AUTHORS - BEFORE IMPUTATION: {has_authors_initial} records have authors from 600 fields, {empty_authors_initial} records have no authors")

    logger.info("Building author lookup for imputation from Galvenais autors...")
    author_lookup = build_author_lookup(final_df)
    logger.info(f"Built author lookup with {len(author_lookup)} unique authors")

    # Track which records will be imputed and their source
    needs_imputation_mask = final_df["Autori (list)"].apply(lambda x: not isinstance(x, list) or len(x) == 0)
    records_needing_imputation = final_df[needs_imputation_mask].copy()

    # Apply imputation
    final_df = final_df.assign(
        _imputed_author_tuples=lambda df: df.apply(
            lambda row: impute_authors_from_galvenais(row, author_lookup, skip_ids), axis=1
        )
    ).assign(
        **{
            "Autori (list)": lambda df: df["_imputed_author_tuples"].apply(lambda x: x[0]),
            "Autori un gadi (list)": lambda df: df["_imputed_author_tuples"].apply(lambda x: x[1]),
        }
    ).drop(columns=["_imputed_author_tuples"])

    # Recreate text versions
    final_df = final_df.assign(
        **{
            "Autori": lambda df: df["Autori (list)"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else None
            ),
            "Autori un gadi": lambda df: df["Autori un gadi (list)"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) and len(x) > 0 else None
            ),
        }
    )

    # Log imputation statistics by source
    empty_authors_after = final_df["Autori (list)"].apply(lambda x: len(x) == 0 if isinstance(x, list) else True).sum()
    imputed_count = empty_authors_initial - empty_authors_after

    # For records that were successfully imputed, break down by source
    if imputed_count > 0:
        successfully_imputed_mask = (
            needs_imputation_mask &
            final_df["Autori (list)"].apply(lambda x: isinstance(x, list) and len(x) > 0)
        )
        imputed_records = final_df[successfully_imputed_mask].copy()
        source_counts = imputed_records["_galvenais_autors_source"].value_counts()

        logger.info(f"WORK AUTHORS - IMPUTATION breakdown ({imputed_count} records imputed):")
        logger.info(f"  - Imputed from 630 field (film/performance director): {source_counts.get('630', 0)}")
        logger.info(f"  - Imputed from 787/500 field (book author): {source_counts.get('787/500', 0)}")
        logger.info(f"  - Imputed from 245 field (director from title): {source_counts.get('245', 0)}")

    logger.info(f"WORK AUTHORS - AFTER IMPUTATION: {empty_authors_after} records still have no authors")

    # Drop the temporary tracking column
    final_df = final_df.drop(columns=["_galvenais_autors_source"])

    ## Deduplicate rows
    # Define primary columns to measure completeness
    primary_columns = [
        "Recenzenti un gadi",
        "Avots",
        "Galvenais autors",
        "Recenzētais darbs",
        "Saistītā organizācija vai notikums",
        "Recenzijas tips"
    ]

    # Score each row based on completeness in primary columns
    final_df = final_df.assign(
        _completeness_score=lambda df: df[primary_columns].notna().sum(axis=1)
    )

    # Define duplicate subset
    duplicate_subset = ["Recenzenti un gadi", "Gads", "Recenzijas virsraksts"]

    # Identify ALL duplicate rows (not just removed ones, but all rows in duplicate groups)
    duplicates_mask = final_df.duplicated(subset=duplicate_subset, keep=False)
    all_duplicates_df = final_df[duplicates_mask].copy()

    # Sort duplicates by duplicate keys and completeness score for easier review
    all_duplicates_df = all_duplicates_df.sort_values(
        by=duplicate_subset + ["_completeness_score"],
        ascending=[True, True, True, False]
    )

    logger.info(f"Found {len(all_duplicates_df)} total rows that are part of duplicate groups")
    logger.info(f"These represent {all_duplicates_df.duplicated(subset=duplicate_subset, keep='first').sum()} duplicate rows to be removed")

    # For each duplicate group, keep the row with the highest completeness score
    deduplicated_df = (
        final_df
        .sort_values(by="_completeness_score", ascending=False)
        .drop_duplicates(subset=duplicate_subset, keep="first")
        .drop(columns="_completeness_score")
    )

    final_df = deduplicated_df
    logger.info(f"Number of rows after deduplication: {len(final_df)}")

    ## Save the data (DISABLED FOR CHECK SCRIPT - NOT SAVING TO AVOID OVERWRITING)
    # final_df.to_csv(OUTPUT_PATH, sep=',', index=False)
    logger.info(f"Final number of rows: {len(final_df)}")
    logger.info(f"[CHECK MODE] Skipping save to avoid overwriting: {OUTPUT_PATH}")

    # Create and save exploded tables for reviewers
    recenzenti_df = (
        final_df[["ID", "Recenzenti (list)", "Recenzenti un gadi (list)"]]
        .explode(["Recenzenti (list)", "Recenzenti un gadi (list)"])
        .dropna(subset=["Recenzenti (list)"])
        .rename(columns={
            "Recenzenti (list)": "Recenzents",
            "Recenzenti un gadi (list)": "Recenzents un gadi"
        })
    )
    recenzenti_path = PROJECT_DIR / "data/cleaned/recenzijas_recenzenti.csv"
    # recenzenti_df.to_csv(recenzenti_path, sep=',', index=False)
    # logger.info(f"Saved exploded reviewers table to: {recenzenti_path}")
    logger.info(f"Number of reviewer rows: {len(recenzenti_df)}")
    logger.info(f"[CHECK MODE] Skipping save to avoid overwriting: {recenzenti_path}")

    # Create and save exploded tables for authors
    autori_df = (
        final_df[["ID", "Autori (list)", "Autori un gadi (list)"]]
        .explode(["Autori (list)","Autori un gadi (list)"])
        .dropna(subset=["Autori (list)"])
        .rename(columns={
            "Autori (list)": "Autors",
            "Autori un gadi (list)": "Autors un gadi"
        })
    )
    autori_path = PROJECT_DIR / "data/cleaned/recenzijas_autori.csv"
    # autori_df.to_csv(autori_path, sep=',', index=False)
    # logger.info(f"Saved exploded authors table to: {autori_path}")
    logger.info(f"Number of author rows: {len(autori_df)}")
    logger.info(f"[CHECK MODE] Skipping save to avoid overwriting: {autori_path}")

    # Create and save exploded tables for reviewed works
    recenzetie_darbi_df = (
        final_df[["ID", "Recenzētais darbs (list)"]]
        .explode(["Recenzētais darbs (list)"])
        .dropna(subset=["Recenzētais darbs (list)"])
        .rename(columns={
            "Recenzētais darbs (list)": "Recenzētais darbs"
        })
    )
    recenzetie_darbi_path = PROJECT_DIR / "data/cleaned/recenzijas_recenzetie_darbi.csv"
    # recenzetie_darbi_df.to_csv(recenzetie_darbi_path, sep=',', index=False)
    # logger.info(f"Saved exploded reviewed works table to: {recenzetie_darbi_path}")
    logger.info(f"Number of reviewed work rows: {len(recenzetie_darbi_df)}")
    logger.info(f"[CHECK MODE] Skipping save to avoid overwriting: {recenzetie_darbi_path}")

    # Create and save exploded tables for institutions
    institucijas_df = (
        final_df[["ID", "Saistītā organizācija vai notikums (list)"]]
        .explode(["Saistītā organizācija vai notikums (list)"])
        .dropna(subset=["Saistītā organizācija vai notikums (list)"])
        .rename(columns={
            "Saistītā organizācija vai notikums (list)": "Saistītā organizācija vai notikums"
        })
    )
    institucijas_path = PROJECT_DIR / "data/cleaned/recenzijas_institucijas.csv"
    # institucijas_df.to_csv(institucijas_path, sep=',', index=False)
    # logger.info(f"Saved exploded institutions table to: {institucijas_path}")
    logger.info(f"Number of institution rows: {len(institucijas_df)}")
    logger.info(f"[CHECK MODE] Skipping save to avoid overwriting: {institucijas_path}")

    # Save filtered-out data for inspection
    # all_filtered_out.to_csv(FILTERED_OUT_PATH, sep=',', index=False)
    # logger.info(f"Saved filtered-out data to: {FILTERED_OUT_PATH}")
    logger.info(f"[CHECK MODE] Skipping save to avoid overwriting: {FILTERED_OUT_PATH}")

    ## Export diagnostic/reference files
    logger.info("\n=== EXPORTING DIAGNOSTIC REFERENCE FILES ===")

    # 1. Export ALL duplicate rows (all rows that are part of duplicate groups)
    # Remove the temporary completeness score column before exporting
    all_duplicates_df = all_duplicates_df.drop(columns=["_completeness_score"])
    duplicates_path = PROJECT_DIR / "data/cleaned/recenzijas_duplicates_all.csv"
    all_duplicates_df.to_csv(duplicates_path, sep=',', index=False)
    logger.info(f"Saved ALL duplicate rows to: {duplicates_path}")
    logger.info(f"  Total duplicate rows: {len(all_duplicates_df)}")

    # 2. Export reviews without work authors
    missing_authors_df = final_df[
        final_df["Autori (list)"].apply(lambda x: not isinstance(x, list) or len(x) == 0)
    ].copy()
    missing_authors_path = PROJECT_DIR / "data/cleaned/recenzijas_missing_work_authors.csv"
    missing_authors_df.to_csv(missing_authors_path, sep=',', index=False)
    logger.info(f"Saved reviews without work authors to: {missing_authors_path}")
    logger.info(f"  Records without work authors: {len(missing_authors_df)}")

    # 3. Export reviews without reviewers
    missing_reviewers_df = final_df[
        final_df["Recenzenti (list)"].apply(lambda x: not isinstance(x, list) or len(x) == 0)
    ].copy()
    missing_reviewers_path = PROJECT_DIR / "data/cleaned/recenzijas_missing_reviewers.csv"
    missing_reviewers_df.to_csv(missing_reviewers_path, sep=',', index=False)
    logger.info(f"Saved reviews without reviewers to: {missing_reviewers_path}")
    logger.info(f"  Records without reviewers: {len(missing_reviewers_df)}")


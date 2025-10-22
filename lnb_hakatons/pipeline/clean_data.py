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
from typing import Dict, List, Optional, Union

from lnb_hakatons import PROJECT_DIR

## Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## Main variables
DATA_DIR = PROJECT_DIR / "data/Mākslu kritika"
DATA_FILE = "cleaned-records-33-wide.csv"
OUTPUT_PATH = PROJECT_DIR / "data/cleaned/recenzijas_clean.csv"
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
    'RECENZĒTAIS IZDEVUMS (787)',
    'RECENZĒTAIS IZDEVUMS (500)',
    "RECENZĒTĀ FILMA VAI IZRĀDE (630)",
    "AVOTA NOSAUKUMS (773)",
    "ELEKTRONISKĀ ADRESE (856)",
    "PAPILDRAKSTS (700)",
    "PAPILDRAKSTS - 2 (700)",
    "NEKONTROLĒTS PERSONAS VĀRDS (720)",
    "NEKONTROLĒTS PERSONAS VĀRDS - 2 (720)",
    "NEKONTROLĒTS PERSONAS VĀRDS - 3 (720)",
    "NEKONTROLĒTS PERSONAS VĀRDS - 4 (720)",
    "NEKONTROLĒTS PERSONAS VĀRDS - 5 (720)",
    "PRIEKŠMETS - INSTITŪCIJA (610)",
]

# Autoru tipi, kurus analizējam
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

    Args:
        text: Name in "Surname, Name" format

    Returns:
        Name in "Name Surname" format, or original text if no pattern matches
    """
    if pd.isna(text) or not text:
        return text

    # Pattern to match surname (including hyphens, spaces, apostrophes, periods) followed by comma and first name
    # [^,]+ matches everything up to the comma (handles complex surnames)
    pattern = r'([^,]+),\s*([^,]+)'

    match = re.search(pattern, text.strip())
    if match:
        surname = match.group(1).strip()
        first_name = match.group(2).strip()
        return f"{first_name} {surname}"

    return text



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



if __name__ == "__main__":
    ## Load the data
    data_df = (
        pd.read_csv(DATA_DIR / DATA_FILE, sep=';')
        .drop(columns=columns_to_remove, axis=1)
    )

    ## Expand MARC columns
    # Create a simplified version of the data with expanded MARC columns
    simplified_df = data_df.copy()

    for col in key_columns:
        if col in simplified_df.columns:
            simplified_df = expand_marc_columns(simplified_df, col)

    # Keep the rest of the columns
    keep_columns = list(set(data_df.columns).difference(set(key_columns)))

    # Add all the new MARC subfield columns
    marc_columns = [col for col in simplified_df.columns if '_' in col]
    all_columns = keep_columns + marc_columns

    # Create the simplified dataframe
    simplified_df = simplified_df[all_columns]

    final_columns = final_processed_columns + create_uncontrolled_name_columns()

    # Add rest of the columns
    if KEEP_OTHER_COLUMNS:
        final_columns_all = final_columns + sorted(keep_columns)
    else:
        final_columns_all = final_columns

    final_columns_all = [col for col in final_columns_all if col in simplified_df.columns]

    ## Filtering
    # Filter by author type
    logger.info(f"Original number of rows: {len(data_df)}")

    # Create a copy for filtering operations
    working_df = simplified_df.copy()[final_columns_all]

    # First filter: author type
    author_filter = working_df["AUTORS (100)_4"].isin(AUTORS_100_4_values)
    filtered_by_author = working_df[author_filter]
    filtered_out_by_author = working_df[~author_filter]

    logger.info(f"Number of rows after filtering authors: {len(filtered_by_author)}")
    logger.info(f"Number of rows filtered out by author type: {len(filtered_out_by_author)}")

    # Second filter: review type
    ir_recenzija = filtered_by_author["PRIEKŠMETS - ŽANRS (655)_a"].fillna("").str.lower().str.contains("recenzija")
    ir_gramata = filtered_by_author["PRIEKŠMETS - ŽANRS (655)_a"].fillna("").str.lower().str.contains("grāmatu apskati")
    ir_vesture = filtered_by_author["PRIEKŠMETS - ŽANRS (655)_x"].fillna("").str.lower().str.contains("vēsture un kritika")

    review_filter = ir_recenzija | ir_vesture | ir_gramata
    final_df = filtered_by_author[review_filter]
    filtered_out_by_review = filtered_by_author[~review_filter]

    logger.info(f"Number of rows after filtering recenzijas: {len(final_df)}")
    logger.info(f"Number of rows filtered out by review type: {len(filtered_out_by_review)}")

    # Combine all filtered-out data
    all_filtered_out = pd.concat([
        filtered_out_by_author.assign(filter_reason="Author type not 'aut' or 'rev'"),
        filtered_out_by_review.assign(filter_reason="Not a review, book review, or history/criticism")
    ], ignore_index=True)

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
            # Combine all authors into one column as a list
            "visas_personas": lambda df: df[["AUTORS (100)_a", "PAPILDRAKSTS (700)_a", "PAPILDRAKSTS - 2 (700)_a", "NEKONTROLĒTS PERSONAS VĀRDS (720)_a", "NEKONTROLĒTS PERSONAS VĀRDS - 2 (720)_a", "NEKONTROLĒTS PERSONAS VĀRDS - 3 (720)_a", "NEKONTROLĒTS PERSONAS VĀRDS - 4 (720)_a", "NEKONTROLĒTS PERSONAS VĀRDS - 5 (720)_a"]].apply(
                lambda row: [val for val in row if pd.notna(val) and val != ""],
                axis=1
            ),
            # Combine subfields _a and _b
            "RAKSTA NOSAUKUMS (245)_ab": lambda df: df["RAKSTA NOSAUKUMS (245)_a"].fillna("") + " " + df["RAKSTA NOSAUKUMS (245)_b"].fillna(""),
            # Remove full stops in genre
            "PRIEKŠMETS - ŽANRS (655)_a": lambda df: df["PRIEKŠMETS - ŽANRS (655)_a"].str.replace(".", "").str.strip(),
            "PRIEKŠMETS - INSTITŪCIJA (610)_a": lambda df: df["PRIEKŠMETS - INSTITŪCIJA (610)_a"].str.replace(".", "").str.strip(),
        })
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
        # harmonized recenzeta_darba_autors un recenzetais_darbs to combine either 245 or 787
        .assign(**{
            "recenzeta_darba_autors": lambda df: df["(787)_author"].replace("", None).fillna(df["(245)_director"]),
            "recenzetais_darbs": lambda df: df["(787)_title"].replace("", None).fillna(df["(245)_title"]),
            "publicetajs_vai_institucija": lambda df: df["(787)_publisher"].replace("", None).fillna(df["PRIEKŠMETS - INSTITŪCIJA (610)_a"]),
        })
        # fix if there is still a colon in the publicetajs_vai_institucija then take the text between the colon and the next comma
        .assign(**{
            "publicetajs_vai_institucija": lambda df: df["publicetajs_vai_institucija"].apply(
                lambda x: x.split(":")[1].split(",")[0].strip() if pd.notna(x) and ":" in str(x) else x
            ),
        })
        # try filling rezentais_darbs nulls with RECENZĒTĀ FILMA VAI IZRĀDE (630)_a
        .assign(**{
            "recenzetais_darbs": lambda df: df["recenzetais_darbs"].fillna(df["RECENZĒTĀ FILMA VAI IZRĀDE (630)_a"]),
        })
        .assign(**{
            "recenzetais_darbs": lambda df: df["recenzetais_darbs"].str.split(":").str[0].str.strip(),
        })
        # if  PRIEKŠMETS - ŽANRS (655)_a is in literature_categories the use "Literatūra", otherwise keep the value
        .assign(**{
            "recenzijas_tips": lambda df: df["PRIEKŠMETS - ŽANRS (655)_a"].apply(
                lambda val: "Literatūras recenzijas" if val in literature_categories else val
            )
        })
        # drop the helper columns for authors and title, and keep only the harmonised columns
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

    ## Save the data
    final_df.to_csv(OUTPUT_PATH, sep=',', index=False)

    # Save filtered-out data for inspection
    all_filtered_out.to_csv(FILTERED_OUT_PATH, sep=',', index=False)
    logger.info(f"Saved filtered-out data to: {FILTERED_OUT_PATH}")


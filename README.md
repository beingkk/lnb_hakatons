# lnb_hakatons

Repo lai izpētītu LNB hakatona datus (Komanda #3, "Dālijas")

## Setup

Clone this repo

```bash
git clone https://github.com/beingkk/lnb_hakatons
cd lnb_hakatons
```

Install dependencies using `uv`:

```bash
uv sync
```

Ensure the `data` folder exists:

```bash
mkdir -p data
```

Download data from GDrive and add the three folders in the `data` folder. Your repo structure should look like this:

```
lnb_hakatons/
├── LICENSE
├── main.py
├── pyproject.toml
├── README.md
├── uv.lock
├── .pre-commit-config.yaml
├── .gitignore
├── lnb_hakatons/
│   ├── pipeline/
│       ├── clean_data.py
├── notebooks/
├── data/
│   ├── Digitālās bibliotēkas lietojums/
│   ├── Digitālās bibliotēkas saturs/
│   ├── Mākslu kritika/
│       ├── MARC bibliogrāfisko datu formāts.docx
│       ├── cleaned-records-2-wide.xlsx
│       ├── cleaned_records_2.txt
│       ├── cleaned_records_2.xlsx
```

## Data cleaning

After the input data is added to the data folder, the data cleaning script can be ran from the terminal with the following command:

```
uv run python lnb_hakatons/pipeline/clean_data.py
```

This will create several output files that were used in a Power BI dashboard. Please reach out to me for further documentation and discussion of limitations.

The rest of .py files and Jupyter Notebooks were used for development and checking outputs, but are not needed for data cleaning.

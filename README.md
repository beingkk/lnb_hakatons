# lnb_hakatons

Repo lai izpētītu LNB hakatona datus

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
├── data/
│   ├── Digitālās bibliotēkas lietojums/
│   ├── Digitālās bibliotēkas saturs/
│   ├── Mākslu kritika/
│       ├── MARC bibliogrāfisko datu formāts.docx
│       ├── cleaned-records-2-wide.xlsx
│       ├── cleaned_records_2.txt
│       ├── cleaned_records_2.xlsx
```

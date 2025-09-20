# Hotels-In-England
Extract UK hotel data with director emails for CRM upload.

## Structure
```
Hotels-In-England/
├── .venv/                   # Virtual environment
├── output/                  # For all project results
│   ├── hotels.db            # SQLite database file
│   └── hotels_data.xlsx     # Final Excel spreadsheet
├── src/                     # All your source code
│   ├── scraper.py           # The core scraping logic
│   └── main.py              # The main script to run the project
├── requirements.txt         # Lists all project dependencies
└── README.md
```

## Quick Start
```bash
conda install requests beautifulsoup4 pandas openpyxl phonenumbers
python src/main.py
```

## Output
- SQLite DB: `output/hotels.db`
- Excel file: `output/hotels_data.xlsx`
- 90%+ director email coverage (filtered results)
- UK phone format, no duplicates, CRM-ready
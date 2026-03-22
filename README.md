# Company Research Pipeline

AI-powered company data enrichment pipeline using Perplexity API. Given a list of company names, it researches and fills in missing details — website, industry, size, location, and more — with iterative version improvements for maximum accuracy.

## What It Does

Input: CSV with company names (and partial data)
Output: Enriched CSV with filled-in company details:
- Website URL
- Industry / Sector
- Company size / employee count
- Headquarters location
- Description

## Contents

| Folder | Description |
|--------|-------------|
| `perplexity_research/` | Main pipeline — Perplexity API powered, v7–v8.7 iterations |
| `basic_scraper/` | Basic scraper for company details from known reference links |

## Versions

The `perplexity_research/` folder contains multiple version iterations:

| File | Version | Key Improvement |
|------|---------|-----------------|
| `company_research_perplexity_v7_ULTRA_FIXED.py` | v7 | Core Perplexity integration |
| `company_research_v8.7_ENHANCED.py` | v8.7 Enhanced | Better prompt engineering |
| `company_research_v8.7_MAXIMUM_ACCURACY.py` | v8.7 Max | Accuracy-focused prompts |
| `company_research_v8.7_OPTIMIZED.py` | v8.7 Optimized | Speed + accuracy balance |

**Recommended: use `company_research_v8.7_MAXIMUM_ACCURACY.py`** for best results.

## Setup

```bash
pip install requests pandas openpyxl

# Set your Perplexity API key
export PERPLEXITY_API_KEY=your_key_here

python perplexity_research/company_research_v8.7_MAXIMUM_ACCURACY.py
```

## Input Format

```csv
company_name,website,industry,size
Acme Corp,,,
TechStart Inc,,,
```

## Features

- Stateful processing — tracks which companies are already researched
- Handles rate limits with automatic retry
- Structured JSON parsing from Perplexity responses
- Confidence-based validation for filled fields

## Tech Stack

- Python, Pandas
- Perplexity API (sonar model)
- Requests
- JSON state management

# Quality Filtering 

1. Load everything into a DF 
2. Run filtering rules using .apply()


Pre-OCR - quality filtering, initial dedup (can explore web-pages type later)
1. write some rule based filters (and perhaps LLM) to get unique names
2. Filter for PDFs more than 5 pages
3. Use VLM to do Language detection - take first 10K characters
4. Additive quality filtering (maybe) for scoring

Post-OCR - what is actually meaningful text to keep which improves model????
1. get rid of MD formatting (### and similar)
2. check how tables are dealt with, just removed completely?
3. math formulas, special symbols remove?
4. remove titles
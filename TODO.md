# Quality Filtering 

1. load from raw web scrapes in /data using collate_pdfs.py to get all valid PDF journals in indo_journals
2. sample_and_split.py splits indo_journals into subsets of 10K PDFs, which should average to around 2B LLAMA tokens (based on estimate)
3. filter_pdfs.py to build initial raw DF filtered using Anthropic prompt, because some Journals actually in English, not useful for Indonesian linguistic adaptation, powered by Spark
    a. iniital OCR uses pypdf cos uses CPU compute, sufficient for LLM to do initial filtering
4. use MathPix to get OCR with structured MD tags used for REGEX processing
5. write REGEX and all data processing to get continuous text with dominantly Indonesian only to train model
    a. will get some contamination from other languages like English but normal

1. Load everything into a DF 
2. Run filtering rules using .apply()

naive filtering:
1. filter for PDFs > 2 pages
2. throw away everything but continuous text
    a. remove titles, newline characters
    b. use LLM to write filtering logic based on analysis of MPix format (authors, tables, page numbers, exhibits, diagrams, special symbols)
    c. use Gemini for LID -> throw away anything containing English


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
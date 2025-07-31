ANALYZE_PROMPT = """You are a Python expert specializing in PDF data extraction. Analyze the bank statement PDF and CSV files to understand the structure.

Your analysis should cover:

1. **CSV Structure Analysis:**
   - Column names and their meaning
   - Data types for each column
   - Date formats used
   - Numeric formats (currency, decimals)
   - Sample data patterns

2. **PDF Structure Analysis:**
   - Page layout and organization
   - Table structure (if tables exist)
   - Text patterns and formatting
   - Headers and footers
   - Transaction data location
   - Potential extraction challenges

3. **Extraction Strategy:**
   - Recommend the best pdfplumber approach (extract_tables vs text parsing)
   - Identify key text patterns for regex extraction
   - Suggest data cleaning steps needed
   - Flag potential edge cases

Provide a detailed technical analysis that will guide the parser generation."""

GENERATE_PARSER_PROMPT = """Create a robust Python parser function that extracts bank statement data from PDF and returns a pandas DataFrame.

**STRICT REQUIREMENTS:**
- Function signature: `parse(pdf_path: str) -> pd.DataFrame`
- Use ONLY these libraries: pdfplumber, pandas, re, datetime
- NO other libraries (no tabula, camelot, PyPDF2, etc.)

**CRITICAL pdfplumber SYNTAX:**
- Use `page.extract_tables()` NOT `page.tables`
- Use `page.extract_text()` for text extraction
- Tables are returned as list of lists, not objects

**IMPLEMENTATION STRATEGY:**
1. Try multiple extraction methods in order:
   - `page.extract_tables()` for structured tables
   - Text-based parsing with regex for semi-structured data
   - Line-by-line parsing for unstructured text

2. **Error Handling:**
   - Handle empty PDFs gracefully
   - Manage missing or malformed data
   - Provide informative error messages
   - Return empty DataFrame with correct columns if no data found

3. **Data Processing:**
   - Clean and normalize extracted data
   - Parse dates correctly (handle DD/MM/YYYY, DD-MM-YYYY formats)
   - Handle currency formatting (remove commas, handle negative values)
   - Ensure column names match exactly

**WORKING TEMPLATE:**
```python
import pdfplumber
import pandas as pd
import re
from datetime import datetime

def parse(pdf_path: str) -> pd.DataFrame:
    all_data = []
    expected_columns = {columns}
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # Method 1: Try extracting tables
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        if table and len(table) > 1:  # Has header + data
                            headers = table[0]
                            # Skip header row, process data rows
                            for row in table[1:]:
                                if row and len(row) >= len(expected_columns):
                                    # Clean and process row data
                                    clean_row = [str(cell).strip() if cell else "" for cell in row]
                                    if any(clean_row):  # Not all empty
                                        all_data.append(clean_row[:len(expected_columns)])
                
                # Method 2: If no tables found, try text parsing
                if not tables or not all_data:
                    text = page.extract_text()
                    if text:
                        lines = text.split('\\n')
                        for line in lines:
                            # Look for transaction patterns (date at start)
                            if re.match(r'^\\d{{1,2}}[/-]\\d{{1,2}}[/-]\\d{{4}}', line.strip()):
                                # Split by whitespace/tabs, try to extract columns
                                parts = re.split(r'\\s{{2,}}|\\t', line.strip())
                                if len(parts) >= len(expected_columns):
                                    all_data.append(parts[:len(expected_columns)])
        
        # Create DataFrame with exact column names
        if all_data:
            df = pd.DataFrame(all_data, columns=expected_columns)
            # Clean data types
            return df
        else:
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=expected_columns)
            
    except Exception as e:
        raise Exception(f"Error parsing PDF: {{str(e)}}")
```

**Analysis Context:**
{analysis}

**Target Bank:** {target_bank}
**Expected Columns:** {columns}

Generate complete, production-ready parser code that follows the exact template structure."""

SELF_CORRECT_PROMPT = """The parser failed. Analyze the error and implement a fix using ONLY allowed libraries.

**ALLOWED LIBRARIES ONLY:**
- pdfplumber (PDF reading)
- pandas (DataFrames)
- re (regex)
- datetime (date parsing)

**CRITICAL pdfplumber SYNTAX FIXES:**
- Use `page.extract_tables()` NOT `page.tables` 
- Use `page.extract_text()` NOT `page.text`
- Tables are lists of lists: `[[row1_col1, row1_col2], [row2_col1, row2_col2]]`

**ERROR ANALYSIS:**
Error: {error}

**CURRENT CODE:**
{code}

**COMMON FIXES NEEDED:**

1. **If "object has no attribute 'tables'":**
```python
# WRONG:
tables = page.tables
# CORRECT:
tables = page.extract_tables()
```

2. **If "Transaction table not found" or empty DataFrame:**
```python
# Add more robust text parsing as fallback:
if not all_data:
    text = page.extract_text()
    if text:
        lines = text.split('\\n')
        for line in lines:
            # Look for date patterns to identify transactions
            if re.match(r'\\d{{1,2}}[/-]\\d{{1,2}}[/-]\\d{{4}}', line):
                # Parse this line as transaction
                parts = re.split(r'\\s{{2,}}|\\t', line.strip())
                if len(parts) >= len(expected_columns):
                    all_data.append(parts[:len(expected_columns)])
```

3. **If columns match but no rows extracted:**
```python
# Ensure you're processing data rows, not just headers
for table in tables:
    if table and len(table) > 1:  # Has header + data rows
        # Skip header (index 0), process data rows
        for row in table[1:]:
            if row and any(str(cell).strip() for cell in row):
                clean_row = [str(cell).strip() if cell else "" for cell in row]
                all_data.append(clean_row[:len(expected_columns)])
```

**DEBUGGING STRATEGY:**
1. Check if tables are being found: `print(f"Found {{len(tables)}} tables")`
2. Check table structure: `print(f"Table has {{len(table)}} rows")`
3. Check if data rows exist: `print(f"Processing row: {{row}}")`
4. Ensure fallback text parsing is working

Provide the corrected code that addresses the specific error while maintaining the same function signature and requirements."""


REFLECTION_PROMPT = """Reflect on the parser generation process and identify what went wrong.

**Context:**
- Bank: {target_bank}
- Attempts Made: {attempts_made}
- Final Error: {final_error}
- Code Generated: {final_code}

**Reflection Areas:**
1. **PDF Structure Understanding:**
   - Did we correctly identify the PDF layout?
   - Were our assumptions about table structure wrong?

2. **Extraction Strategy:**
   - Was the chosen extraction method appropriate?
   - Should we have tried different approaches?

3. **Error Patterns:**
   - What types of errors occurred repeatedly?
   - Are there systematic issues in our approach?

4. **Improvements for Next Time:**
   - What would you do differently?
   - What additional strategies should be tried?

Provide a detailed reflection that could help improve future parser generation attempts."""
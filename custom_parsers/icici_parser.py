import pdfplumber
import pandas as pd
import re
from datetime import datetime

def parse(pdf_path: str) -> pd.DataFrame:
    all_data = []
    expected_columns = ['Date', 'Description', 'Debit Amt', 'Credit Amt', 'Balance']
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # Method 1: Try extracting tables
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        if table and len(table) > 1:  # Check if table has header and data
                            headers = table[0]
                            if any('Date' in str(cell) for cell in headers):
                                for row in table[1:]:
                                    if row and len(row) >= len(expected_columns):
                                        clean_row = []
                                        for cell in row:
                                            if cell is not None:
                                                cell_str = str(cell).strip()
                                                # Clean date format
                                                if re.match(r'\d{2}[/-]\d{2}[/-]\d{4}', cell_str):
                                                    if '/' in cell_str:
                                                        date_format = '%m/%d/%Y'
                                                    else:
                                                        date_format = '%m-%d-%Y'
                                                    date_obj = datetime.strptime(cell_str, date_format)
                                                    clean_row.append(date_obj.strftime('%m-%d-%Y'))
                                                else:
                                                    clean_row.append(cell_str)
                                        if len(clean_row) >= len(expected_columns):
                                            # Handle numeric values
                                            for i in [2, 3, 4]:
                                                if i < len(clean_row):
                                                    value = clean_row[i].replace(',', '')
                                                    if value.replace('.', '', 1).isdigit():
                                                        clean_row[i] = float(value)
                                            all_data.append(clean_row[:len(expected_columns)])
                
                # Method 2: If no tables found, try text parsing
                if not tables or not all_data:
                    text = page.extract_text()
                    if text:
                        lines = text.split('\n')
                        for line in lines:
                            line = line.strip()
                            # Look for transaction patterns (date at start)
                            if re.match(r'^\d{2}[/-]\d{2}[/-]\d{4}', line):
                                parts = re.split(r'\s{2,}|\t', line)
                                if len(parts) >= len(expected_columns):
                                    clean_parts = []
                                    # Process date
                                    date_part = parts[0].strip()
                                    if re.match(r'\d{2}[/-]\d{2}[/-]\d{4}', date_part):
                                        if '/' in date_part:
                                            date_format = '%m/%d/%Y'
                                        else:
                                            date_format = '%m-%d-%Y'
                                        date_obj = datetime.strptime(date_part, date_format)
                                        clean_parts.append(date_obj.strftime('%m-%d-%Y'))
                                    else:
                                        clean_parts.append(date_part)
                                    # Process remaining parts
                                    for part in parts[1:len(expected_columns)]:
                                        part = part.strip()
                                        clean_parts.append(part)
                                    # Handle numeric values
                                    for i in [2, 3, 4]:
                                        if i < len(clean_parts):
                                            value = clean_parts[i].replace(',', '')
                                            if value.replace('.', '', 1).isdigit():
                                                clean_parts[i] = float(value)
                                    all_data.append(clean_parts)
        
        # Create DataFrame with exact column names
        if all_data:
            df = pd.DataFrame(all_data, columns=expected_columns)
            # Clean data types
            df['Debit Amt'] = pd.to_numeric(df['Debit Amt'], errors='coerce')
            df['Credit Amt'] = pd.to_numeric(df['Credit Amt'], errors='coerce')
            df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce')
            return df
        else:
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=expected_columns)
            
    except Exception as e:
        raise Exception(f"Error parsing PDF: {str(e)}")
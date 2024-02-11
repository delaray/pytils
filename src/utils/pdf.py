# *********************************************************************
# PDF Document Parsing Tools
# *********************************************************************

# Reference: https://realpython.com/pdf-python/

# History of pyPdf, PyPDF2, and PyPDF4
# pdfrw: An Alternative
# Installation
# How to Extract Document Information From a PDF in Python
# How to Rotate Pages
# How to Merge PDFs
# How to Split PDFs
# How to Add Watermarks
# How to Encrypt a PDF
# Conclusion
# Further Reading

# Other Packages
# https://pypi.org/project/doi2pdf/
# https://pypi.org/project/dreamai-pdf/
#
# ********************************************************************
#
# Anystyle (Ruby BaseBased)
# https://github.com/inukshuk/anystyle
#
# AnyStyle is a fast and smart parser of bibliographic
# references. Originally inspired by parsCit and FreeCite, AnyStyle
# uses machine learning algorithms and aims to make it easy to train
# models with data that's relevant to you.

# *********************************************************************

# Python Imports
import os
import re
import platform
import pandas as pd
from unidecode import unidecode
from datetime import datetime
from itertools import chain

# from pypdf import PdfReader
from PyPDF2 import PdfReader
import pdfreader
from pdfreader import PDFDocument, SimplePDFViewer


# Pytils Imports
from utils.data import split_list


# ********************************************************************************
# Part 1: Loading PDF Files
# ********************************************************************************

PDF_FILE = '/education/papers/Attention-is-All-You-Need.pdf'


# --------------------------------------------------------------------------
# PyPDF2 Example
# --------------------------------------------------------------------------

# Code copied from:
# https://stackoverflow.com/questions/26494211/extracting-text-from-a-pdf-file-using-pdfminer-in-python
#
# Install pdfminer using: pip install git+https://github.com/pdfminer/pdfminer.six.git
# NB1: Requires 
# NB: Needed to change StringIO import statement to import from io in Python 3

# Upgraded to handle exceptions of type NameError when text extraction not allowed:
#
# In [49]: PDFTextExtractionNotAllowed
# ---------------------------------------------------------------------------
# NameError                                 Traceback(most recent call last)
# <ipython-input-49-3103147440c4> in <module>()
# ----> 1 PDFTextExtractionNotAllowed

def load_pdf_file(pdfname):

    try:
        # PDFMiner boilerplate
        rsrcmgr = PDFResourceManager()
        sio = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, sio, codec=codec, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        
        # Extract text
        fp = open(pdfname, 'rb')
        for page in PDFPage.get_pages(fp):
            interpreter.process_page(page)
        fp.close()
            
        # Get text from StringIO
        text = sio.getvalue()
            
        # Cleanup
        device.close()
        sio.close()

        return text
    except Exception:
        return ''

# ----------------------------------------------------------------------------

def load_pdf_directory(dir):
    files = files_in_dir(dir)
    return [_pdf_file(f) for f in files]

# **************************************************************************
# PyPDF2
# **************************************************************************


# --------------------------------------------------------------------------
# PyPDF2 Example
# --------------------------------------------------------------------------

def extract_information(pdf_path):
    with open(pdf_path, 'rb') as f:
        pdf = PdfReader(f)
        information = pdf.metadata
        number_of_pages = len(pdf.pages)

    txt = f"""
    Information about {pdf_path}: 

    Author: {information.author}
    Creator: {information.creator}
    Producer: {information.producer}
    Subject: {information.subject}
    Title: {information.title}
    Number of pages: {number_of_pages}
    """

    print(txt)

    return pdf

# --------------------------------------------------------------------------
# Ignore Footers and Headers
# --------------------------------------------------------------------------

def ignore_header_and_footer(page):

    parts = []
    
    def visitor_body(text, cm, tm, fontDict, fontSize):
        y = tm[5]
        if y > 50 and y < 720:
            parts.append(text)

    page.extract_text(visitor_text=visitor_body)
    text_body = "".join(parts)
    return text_body

# --------------------------------------------------------------------------
# Join Lines
# --------------------------------------------------------------------------

def join_lines(lines):

    if lines == []:
        return ''
    if len(lines) == 1:
        return lines[0]
    else:
        s1, s2 = lines[0], lines[1:]

        for s in  s2:
            if s1[:-1] != '-':
                s1 = f'{s1} {s}'
            else:
                s1 = s1 + s
        return s1

# --------------------------------------------------------------------------
# Parse Abstract
# --------------------------------------------------------------------------

def parse_title(page):
    text = page.extract_text()
    return text.split('\n')[0]
    
# --------------------------------------------------------------------------

def parse_abstract(page):
    text = page.extract_text()
    lines = text.split('\n')
    start = lines.index('Abstract') + 1
    if 'Introduction' in lines[start:]:
        end = lines.index('.\n1 Introduction')
    else:
        end = lines[start:].index('\n') + start
    lines = join_lines(lines[start: end])
    return lines

# --------------------------------------------------------------------------

def is_valid_email(email: str) -> bool:
    try:
        email.encode('utf-8')
    except UnicodeEncodeError:
        return False

    # Define a pattern for a well-structured email address
    pattern = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"

    if re.match(pattern, email):
        return True
    else:
        return False


# --------------------------------------------------------------------------

EMAIL_PATTERN = '^([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})$'

# TODO: Make author extraction more robust:

def parse_authors(page):
    text = page.extract_text()
    if 'Abstract' in text:
        
        lines = text.split('\n')
        start = 1
        end = lines.index('Abstract')
        text = '\n'.join(lines[start: end])
        
        text = text.replace('.com', '.com \n')
        text = text.replace('.edu', '.edu \n')
        text = text.replace('.org', '.org \n')
        lines = text.split('\n')
        lines = list(filter(lambda elmt: elmt != '', lines))
        lines = [unidecode(line) for line in lines]
        authors = []
        i = 0
        # print(f'LINES:\n{lines}')

        try:
            while i < len(lines):
                name = lines[i]
                i += 1
                company = lines[i]
                if '@' not in company:
                    i += 1
                    email = lines[i]
                else:
                    email = company
                    company = name
                i += 1
                
            email = email.strip()
            if len(re.findall(EMAIL_PATTERN, email)) > 0:
                name = name.replace('\u2217', '')
                authors.append([name, company, email])

            # print(f'\nParsed document authors:\n{authors}\n')
            return authors
        
        except Exception as err:
            # print(f'\nError in parse_authors.\n{err}\n')
            return []

    else:
        return []

# --------------------------------------------------------------------------

# Convert PDF Date format to datetime D:YYYYMMDDHHmmSS

def convert_pdf_date(date):
    year = int(date[2:6])
    month = int(date[6:8])
    day = int(date[8:10])
    hour = int(date[10:12])
    minute = int(date[12:14])
    second = int(date[14:16])
    return [year, month, day, hour, minute, second]


# --------------------------------------------------------------------------

# Convert PDF Date format to datetime D:YYYYMMDDHHmmSSdef parse_document(pathname):

def parse_document(path, include_content=True):

    with open(path, 'rb') as f:
        reader = PdfReader(f)

        # Process metadata for date information
        try:
            metadata = reader.metadata
            published_date = convert_pdf_date(metadata['/CreationDate'])
            published = datetime(*published_date)
            updated_date =  convert_pdf_date(metadata['/ModDate'])
            updated = datetime(*updated_date)
            year = published_date[0]
            month = published_date[1]
        except Exception as err:
            print(f'\nError parsing document metadata:{path}\n{err}\n')
            published, updated, year = None, None, 0

        # Process title, authors and abstract
        pages = reader.pages
        title = parse_title(pages[0])
        authors = parse_authors(pages[0])
        abstract = parse_abstract(pages[0])
        if include_content is True:
            content = list(map(lambda page: page.extract_text(), pages))
        else:
            content = None
        
        return {'title': title,
                'authors': authors,
                'parseed_authors': authors,
                'month': month,
                'year' : year,
                'published': published,
                'updated': updated,
                'abstract': abstract,
                'content': content}


# *********************************************************************
# Chunking
# ********************************************************************

def chunk_paragraph(text: str, chunk_count=None, chunk_size=1,
                    delim='. ') -> str:
    'Split into< chunk_count> chunks or <chunk_size> sentences per chunk.'
    
    if chunk_count is not None:
        partitions = split_list(sentences, chunk_count)
        # Concatenate the sentences in each partition.
        chunks = [delim.join(partition) for partition in partitions]
        
    else:
        # Compute quotient and remainder of sentences per chunk (spc)
        count = len(sentences) // chunk_size
        remainder = len(sentences) % chunk_size
    
        # Partition into <count> chunks of <chunk_size> sentences per chunk
        chunks = [sentences[chunk_size*i:chunk_size*(i+1)] for i in range(count)]
    
        # Add the last remaining chunk
        chunks.append(sentences[-remainder:])
    
        # Concatenate the sentences in each chunk.
        chunks = [delim.join(chunk) for chunk in chunks]
    
    return chunks


# --------------------------------------------------------------------------

def chunk_paragraphs(texts: list, chunk_count=None, chunk_size=None,
                     delim='. ') -> list:
    'Splits texts into a list of strings using chunk_count or chunk_size.'

    if chunk_count is not None:
        # Convert into a single paragraph
        text = delim.join(texts)
        # Chunk into <chunk_count> chunks
        chunks = chunk_paragraph(text, chunk_count=chunk_count, delim=delim)
        chunks = [delim.join(chunk) for chunk in chunks]
        # Return list of strings
        return chunks
    
    else:
        # Each pargraph contains chunks <chunk_size> sentences)
        chunks = [chunk_paragraph(text, spc=spc, delim=delim)
                  for text in texts]
        # Return list of strings
        return list(chain(*chunks))


# *********************************************************************
# End of File
# ********************************************************************

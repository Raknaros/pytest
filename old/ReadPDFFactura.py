import pandas as pd
import pdfquery
import tabula


def extract_table_from_pdf(file_path):
    df = tabula.read_pdf(file_path, pages='all')
    return df


df = extract_table_from_pdf('C:/Users/Raknaros/Desktop/temporal/pdfs/PDF-DOC-E001-179820601056756.pdf')
print(df)

# pdf = pdfquery.PDFQuery('C:/Users/Raknaros/Desktop/temporal/pdfs/PDF-DOC-E001-179820601056756.pdf')
# pdf.load()
# pdf.tree.write('C:/Users/Raknaros/Desktop/temporal/pdfs/test.xml', pretty_print=True)

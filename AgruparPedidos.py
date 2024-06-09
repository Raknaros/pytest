import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np
import pypdf
from pypdf import PdfReader, PdfWriter, PdfMerger

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

pdfFiles = []  # variable '1', '2', '3'

for root, dirs, filenames in os.walk('C:\\Users\\Raknaros\\Desktop\\temporal\\pdfpedidos'):  # Root and directory pathway.
    for filename in filenames:
        #print(root.replace('\\', '/') + '/' + filename)
        #print(os.path.join(root, filename))
        if filename.lower().endswith('.pdf'):  # for loop for all files with .pdf in the name.
            pdfFiles.append(os.path.join(root, filename))
        # Appending files to root name from OS (operating system).
#CAMBIAR EL ENCABEZADO GUIA A DOC_REFERENCIA
pedidos = pd.read_excel('C:/Users/Raknaros/Desktop' + '/Result_3.xlsx',
                        dtype={'proveedor': str, 'adquiriente': str, 'factura': str},
                        na_values=' ')

# Condición para strings que comienzan con 'EG07'
condition = pedidos['guia'].str.startswith('EG')

# Aplicar slice solo a los que cumplen la condición
pedidos.loc[condition, 'sliced_col'] = pedidos.loc[condition, 'guia'].str.slice(7, -1)
#CAMBIAR EL ENCABEZADO SLICED COL A GUIA
# Quitar ceros iniciales a los valores que cumplen la condición
pedidos.loc[condition, 'sliced_col'] = pedidos.loc[condition, 'sliced_col'].str.lstrip('0')

# Para los valores que no cumplen la condición, copiar la columna original
#pedidos.loc[~condition, 'sliced_col'] = pedidos.loc[~condition, 'guia']



adquirientes = pedidos['adquiriente'].unique()
for adquiriente in adquirientes:
    print(adquiriente)

# Iterar sobre cada valor único
for adquiriente in adquirientes:
    merger = PdfMerger()
    # Filtrar el DataFrame para obtener el sub DataFrame
    pedido = pedidos[pedidos['adquiriente'] == adquiriente]

    # Crear una lista para almacenar los resultados de este sub DataFrame
    sub_list = []

    # Iterar sobre las filas del sub DataFrame y agregar los valores de las columnas 2 y 3 a la lista
    for index, row in pedido.iterrows():

        print('C:/Users/Raknaros/Desktop/temporal/pdfpedidos/'+'PDF-DOC-E001'+row['factura']+row['proveedor']+'.pdf')
        merger.append('C:/Users/Raknaros/Desktop/temporal/pdfpedidos/'+'PDF-DOC-E001'+row['factura']+row['proveedor']+'.pdf')

        if pd.notna(row['sliced_col']):
            print('C:/Users/Raknaros/Desktop/temporal/pdfpedidos/'+row['proveedor']+'-09-EG07-'+row['sliced_col']+'.pdf')
            merger.append('C:/Users/Raknaros/Desktop/temporal/pdfpedidos/'+row['proveedor']+'-09-EG07-'+row['sliced_col']+'.pdf')

    merger.write(adquiriente+".pdf")
    print(adquiriente)
    merger.close()









#print(pedidos.dtypes)
def unir ():

    merger = PdfMerger()
    for filename in pdfFiles:
        merger.append(filename)
    merger.write("combined.pdf")
    merger.close()
# Assigning the pdfWriter() function to pdfWriter.
#pdfWriter = pypdf.PdfWriter()
"""
for filename in pdfFiles:  # Starting a for loop.
    pdfFileObj = open(filename, 'rb')  # Opens each of the file paths in filename variable.
    pdfReader = pypdf.PdfReader(pdfFileObj)  # Reads each of the files in the new varaible you've created above and stores into memory.
    pageObj = pdfReader.pages[pageNum]  # Reads only those that are in the varaible.
    pdfWriter.add_page(pageObj)  # Adds each of the PDFs it's read to a new page.

pdfOutput = open('Power_BI_Test_Files.pdf', 'wb')

# Writing the output file using the pdfWriter function.
pdfWriter.write(pdfOutput)
pdfOutput.close()"""

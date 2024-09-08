import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np
import pypdf
from pypdf import PdfReader, PdfWriter, PdfMerger

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

warehouse = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                          ':5432/warehouse')

salessystem = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                            ':3306/salessystem')

pdfFiles = []  # variable '1', '2', '3'
#Iterar y capturar la ruta, el directorio y los archivos de la carpeta indicada
for root, dirs, filenames in os.walk(
        'C:\\Users\\Raknaros\\Desktop\\temporal\\pdfpedidosagosto'):  # Root and directory pathway.
    # Iterar por cada archivo
    for filename in filenames:
        #print(root.replace('\\', '/') + '/' + filename)
        #print(os.path.join(root, filename))
        # Condicion si el archivo termina en .pdf osea si tiene formato pdf
        if filename.lower().endswith('.pdf'):  # for loop for all files with .pdf in the name.
            #Agregar los archivos pdf a la lista pdfFiles
            pdfFiles.append(os.path.join(root, filename))
        # Appending files to root name from OS (operating system).

# LISTA DE FACTURAS Y GUIAS SEGUN ADQUIRIENTE
#TODO CAMBIAR EL ENCABEZADO GUIA A DOC_REFERENCIA
lista = pd.read_sql(
    "SELECT numero_documento AS adquiriente, ruc AS proveedor, numero_correlativo AS factura, (CASE tipo_documento_referencia WHEN 1 THEN null ELSE TRIM('|' FROM SPLIT_PART(numero_documento_referencia,'-',2))::INT END)::TEXT AS guia FROM facturas_noanuladas WHERE periodo_tributario = 202408 ORDER BY adquiriente, proveedor, factura",
    dtype={'proveedor': str, 'adquiriente': str, 'factura': str, 'guia': str}, con=warehouse)

proveedores = pd.read_sql("SELECT tipo_proveedor, numero_documento, alias FROM proveedores", con=salessystem, dtype_backend="pyarrow")

customers = pd.read_sql("SELECT ruc, alias FROM customers", con=salessystem, dtype_backend="pyarrow")

lista_filtrada = lista[~lista['adquiriente'].isin(proveedores['numero_documento'].astype(str))]

# Condición para strings que comienzan con 'EG07'
#condition = lista['guia'].str.startswith('EG')

# Aplicar slice solo a los que cumplen la condición
# TODO CAMBIAR EL ENCABEZADO SLICED COL A GUIA
#lista.loc[condition, 'sliced_col'] = lista.loc[condition, 'guia'].str.slice(7, -1)

# Quitar ceros iniciales a los valores que cumplen la condición
#lista.loc[condition, 'sliced_col'] = lista.loc[condition, 'sliced_col'].str.lstrip('0')

# Para los valores que no cumplen la condición, copiar la columna original
#pedidos.loc[~condition, 'sliced_col'] = pedidos.loc[~condition, 'guia']


adquirientes = lista['adquiriente'].unique()

# Iterar sobre cada valor único
for adquiriente in adquirientes:
    merger = PdfMerger()
    # Filtrar el DataFrame para obtener el sub DataFrame
    lista_filtrada_poradquiriente = lista_filtrada[lista_filtrada['adquiriente'] == adquiriente]

    # Crear una lista para almacenar los resultados de este sub DataFrame
    sub_list = []

    # Iterar sobre las filas del sub DataFrame y agregar los valores de las columnas 2 y 3 a la lista
    for index, row in lista_filtrada_poradquiriente.iterrows():

        print('C:/Users/Raknaros/Desktop/temporal/pdfpedidosagosto/' + 'PDF-DOC-E001' + row['factura'] + row[
            'proveedor'] + '.pdf')
        merger.append('C:/Users/Raknaros/Desktop/temporal/pdfpedidosagosto/' + 'PDF-DOC-E001' + row['factura'] + row[
            'proveedor'] + '.pdf')

        if row['guia'] is not None and row['guia'] != 'None' and not pd.isna(row['guia']):
            print('C:/Users/Raknaros/Desktop/temporal/pdfpedidosagosto/' + row['proveedor'] + '-09-EG07-' + row[
                'guia'] + '.pdf')
            merger.append('C:/Users/Raknaros/Desktop/temporal/pdfpedidosagosto/' + row['proveedor'] + '-09-EG07-' + row[
                'guia'] + '.pdf')

    merger.write("202408_" + adquiriente + '_' + customers.loc[customers['ruc'] == int(adquiriente), 'alias'].values[0] + ".pdf")
    print(adquiriente)
    merger.close()


#print(pedidos.dtypes)
def unir():
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

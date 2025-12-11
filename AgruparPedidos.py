import zipfile
import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np
import pypdf
from pypdf import PdfWriter
import fnmatch
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Conexión a Warehouse (Contabilidad)
warehouse_url = f"{os.getenv('DB_WAREHOUSE_DIALECT')}://{os.getenv('DB_WAREHOUSE_USER')}:{os.getenv('DB_WAREHOUSE_PASSWORD')}@{os.getenv('DB_WAREHOUSE_HOST')}:{os.getenv('DB_WAREHOUSE_PORT')}/{os.getenv('DB_WAREHOUSE_NAME')}"
warehouse = create_engine(warehouse_url)

# Conexión a SalesSystem (Facturación)
salessystem_url = f"{os.getenv('DB_SALESSYSTEM_DIALECT')}://{os.getenv('DB_SALESSYSTEM_USER')}:{os.getenv('DB_SALESSYSTEM_PASSWORD')}@{os.getenv('DB_SALESSYSTEM_HOST')}:{os.getenv('DB_SALESSYSTEM_PORT')}/{os.getenv('DB_SALESSYSTEM_NAME')}"
salessystem = create_engine(salessystem_url)

ruta = 'C:/Users/Raknaros/Downloads/pdfpedidosoctubre/pdfpedidosnoviembre'

periodo = "202511"

directorio = ('C:\\Users\\Raknaros\\Desktop\\temporal\\pdfpedidosnoviembre')

# Obtener lista de archivos PDF en el directorio
archivos = [archivo for archivo in os.listdir(directorio) if archivo.endswith('.pdf')]

facturas_noanuladas = pd.read_sql(
    "SELECT ruc AS proveedor,numero_serie AS serie, numero_correlativo AS correlativo, numero_documento AS adquiriente, tipo_documento_referencia, numero_documento_referencia FROM facturas_noanuladas_ventas WHERE periodo_tributario = " + periodo + " ORDER BY adquiriente, proveedor, correlativo",
    dtype={'proveedor': str, 'adquiriente': str, 'correlativo': str, 'numero_documento_referencia': str}, con=warehouse)


facturas_noanuladas['guia'] = np.where(
    (facturas_noanuladas['tipo_documento_referencia'] == 9) &
    (facturas_noanuladas['numero_documento_referencia'].str[:4] == 'EG07'),
    facturas_noanuladas['numero_documento_referencia'].astype(str).str.split('-', n=1).str[1].fillna('0').astype(str).str.replace('|', '', regex=False).astype(int),
    None
)

proveedores = pd.read_sql("SELECT numero_documento AS proveedor, alias FROM proveedores", con=salessystem,
                          dtype_backend="pyarrow")

customers = pd.read_sql("SELECT related_user, CAST(ruc AS CHAR) AS adquiriente, alias FROM customers", con=salessystem,
                        dtype_backend="pyarrow")

lista_filtrada = facturas_noanuladas[~facturas_noanuladas['adquiriente'].isin(proveedores['proveedor'].astype(str))]

adquirientes = lista_filtrada['adquiriente'].unique()

lista = pd.merge(lista_filtrada, customers, on='adquiriente', how='left')

for adquiriente in adquirientes:
    merger = PdfWriter()
    #Filtrar el Dataframe por adquiriente
    lista_adquiriente = lista[lista['adquiriente'] == adquiriente]
    print(f'Pedido de {lista_adquiriente['alias'].iloc[0]}')

    for index, row in lista_adquiriente.iterrows():
        expresiones = [
            f'PDF-DOC-{row['serie']}-{row['correlativo']}{row['proveedor']}*',
            f'PDF-DOC-{row['serie']}{row['correlativo']}{row['proveedor']}*',
            f'{row['proveedor']}-01-{row['serie']}-{row['correlativo']}*'
        ]

        factura = None
        for expresion in expresiones:
            coincidencias = fnmatch.filter(archivos, expresion)
            if coincidencias:
                factura = coincidencias[0]
                break

        if factura:
            merger.append(os.path.join(directorio, factura))
        else:
            print(f'Falta la factura {row['serie']}-{row['correlativo']} de {row['proveedor']}')

        """
        if row['guia'] is not None and row['guia'] != 'None' and not pd.isna(row['guia']):
            guia = fnmatch.filter(archivos, f'{row['proveedor']}-09-EG07-{row['guia']}*')
            if guia:
                merger.append(os.path.join(directorio, guia[0]))
            else:
                print(f'Falta la guia EG07-{row['guia']} de {row['proveedor']}')
        """
    merger.write(f'{periodo}_{adquiriente}_{lista_adquiriente['alias'].iloc[0]}.pdf')
    merger.close()
    
"""
# Carpeta que contiene los archivos PDF
carpeta_pdf = os.getcwd()

# Archivos PDF en la carpeta
pedidos = [archivo for archivo in os.listdir(carpeta_pdf) if archivo.endswith('.pdf')]

for contacto in customers['related_user'].unique().dropna():
    lista_por_contacto = customers[customers['related_user'] == contacto]
    print(f'Pedido de {contacto}')
    with zipfile.ZipFile(f'{periodo}_{contacto}.zip', "w") as zip_file:
        for index, row in lista_por_contacto.iterrows():
            pedido = fnmatch.filter(pedidos, f'{periodo}_{row['adquiriente']}_{row['alias']}*')
            if pedido:
                print(f'{periodo}_{row['adquiriente']}_{row['alias']}')
                zip_file.write(os.path.join(carpeta_pdf, pedido[0]))
"""

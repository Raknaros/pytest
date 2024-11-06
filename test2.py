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
        'C:/Users/Raknaros/Downloads/pdfpedidosoctubre/pdfpedidosoctubre'):  # Root and directory pathway.
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
facturas_noanuladas = pd.read_sql(
    "SELECT * FROM facturas_noanuladas WHERE periodo_tributario = 202410 ORDER BY numero_documento, ruc, numero_correlativo",
    dtype={'ruc': str, 'numero_documento': str, 'numero_correlativo': str, 'numero_documento_referencia': str}, con=warehouse)

facturas_noanuladas['guia'] = np.where(
    (facturas_noanuladas['tipo_documento_referencia'] == 9) &
    (facturas_noanuladas['numero_documento_referencia'].str[:4] == 'EG07'),
    facturas_noanuladas['numero_documento_referencia'].str.split('-').str[1].str.replace('|', '').astype(int).astype(str),
    None
)

proveedores = pd.read_sql("SELECT tipo_proveedor, numero_documento, alias FROM proveedores", con=salessystem, dtype_backend="pyarrow")

customers = pd.read_sql("SELECT ruc, alias FROM customers", con=salessystem, dtype_backend="pyarrow")

#lista_filtrada = lista[~lista['adquiriente'].isin(proveedores['numero_documento'].astype(str))]

#adquirientes = lista['adquiriente'].unique()




print(facturas_noanuladas)




















#TODO VERIFICAR LA CONSISTENCIA DE LA DATA DE CIERRE DE MES EN LOS SIGUIENTES SENTIDOS:


#SOBRE LAS ENTIDADES/PROVEEDORES
    #TODO CONSULTAR LISTA DE FACTURAS POR PERIODO TRIBUTARIO
    #QUE CANTIDAD DE VENTAS TOTALES TIENE CADA ENTIDAD/PROVEEDOR
    #TODO AGRUPAR POR ENTIDAD Y SUMAR TOTAL + IGV
    #QUE CANTIDAD DE VENTAS CORRESPONDEN DE CADA ENTIDAD/PROVEEDOR A PROVEEDORES TIPO 1
    #TODO CONSULTA PROVEEDORES TIPO 1 Y 2 Y CONSULTAR QUE FACTURAS CORRESPONDEN A COMPRAS DE ESOS PROVEEDORES
    #QUE CANTIDAD DE VENTAS CORRESPONDEN DE CADA ENTIDAD/PROVEEDOR A PROVEEDORES TIPO 3
    #TODO CONSULTAR PROVEEDORES TIPO 3 Y CONSULTA QUE FACTURAS CORRESPONDEN A COMPRAS DE ESOS PROVEEDORES
    #QUE CANTIDAD DE VENTAS CORRESPONDEN DE CADA ENTIDAD/PROVEEDOR A CUSTOMERS INTERNOS
    #TODO CONSULTAR CUSTOMERS INTERNOS Y CONSULTAR QUE FACTURAS CORRESPONDEN A COMPRAR DE ESOS CUSTOMERS
    #QUE CANTIDAD DE VENTAS CORRESPONDEN DE CADA ENTIDAD/PROVEEDOR A CUSTOMERS EXTERNOS
    #TODO CONSULTAR CUSTOMERS EXTERNOS Y CONSULTAR QUE FACTURAS CORRESPONDEN A COMPRAR DE ESOS CUSTOMERS

#SOBRE LOS COMPROBANTES
#QUE CANTIDAD DE COMPROBANTES NO TIENEN GUIA
#TODO CONSULTA Y CONTAR COMPROBANTES QUE NO TENGAN ASOCIADA UNA GUIA
#QUE CANTIDAD DE COMPROBANTES TIENEN GUIA
#TODO CONSULTA Y CONTAR COMPROBANTES QUE TENGAN ASOCIADA UNA GUIA
#EXISTEN COMPROBANTES DE LA MISMA ENTIDAD QUE TENGAN LA MISMA GUIA ASIGNADA?
#TODO CONSULTAR SEGUN LA ENTIDAD/PROVEEDOR SI ALGUNA GUIA ASOCIADA SE REPITE Y COLOCAR ESTADO OBSERVADO GUIA REPETIDA


#SOBRE LOS PEDIDOS
#IDENTIFICAR LOS PEDIDOS EXISTENTES DEL PERIODO CON LA SUMA DE LOS TOTALES DE LAS FACTURAS EMITIDAS EN ORDEN DE FECHA
#TODO VERIFICAR PEDIDOS ENTREGADOS, SUMAR COMPROBANTES DE ESE ADQUIRIENTE SEGUN FECHA DE EMISION ASCENDENTE HASTA LLEGAR AL TOTAL DEL PEDIDO ENTREGADO
#EXCLUIR ESOS PEDIDOS YA EXISTENTES EN LA TABLA PEDIDOS DE LA AUTOGENERACION
#TODO FILTRAR ESAS FACTURAS DE LA LISTA PARA TRASLADAR Y AUTOGENERAR PEDIDOS
#COMPARAR LA INFORMACIO EXISTENTE DE LAS FACTURAS POR SI HUBIESE ALGUNA QUE AGREGAR DESDE WAREHOUSE A SALESSYSTEM
#TODO DETERMINAR QUE INFORMACION DE LA TABLA _5 WAREHOUSE ES NECESARIA TRASLADAR A FACTURAS PARA COMPLEMENTAR
#TRANSFERIR ENTRE BASES DE DATOS SOLO LAS FACTURAS QUE NO EXISTEN YA Y VERIFICAR SI HAY FORMA DE IDENTIFICARLAS POR TOTAL O POR ITEMS Y COLOCARLES EL NUMERO DE FACTURA Y GUIA SI FUESE NECESARIO
#TODO BUSCAR FORMA DE IDENTIFICAR _5 CON FACTURAS QUE TIENEN PENDIENTE EL NUMERO DE CORRELATIVO Y DE GUIA SI FUESE NECESARIO
#TRANSFERIR TAMBIEN LAS GUIAS
#TODO TRANSFERIR LAS GUIAS TAMBIEN

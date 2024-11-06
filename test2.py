import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np
import pypdf
from pypdf import PdfReader, PdfWriter, PdfMerger
import fnmatch

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

warehouse = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                          ':5432/warehouse')

salessystem = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                            ':3306/salessystem')

ruta = 'C:/Users/Raknaros/Downloads/pdfpedidosoctubre/pdfpedidosoctubre'

periodo = "202410"

directorio = 'C:\\Users\\Raknaros\\Desktop\\temporal\\pdfpedidosoctubre'

# Obtener lista de archivos PDF en el directorio
archivos = [archivo for archivo in os.listdir(directorio) if archivo.endswith('.pdf')]

facturas_noanuladas = pd.read_sql(
    "SELECT ruc AS proveedor,numero_serie AS serie, numero_correlativo AS correlativo, numero_documento AS adquiriente, tipo_documento_referencia, numero_documento_referencia FROM facturas_noanuladas WHERE periodo_tributario = " + periodo + " ORDER BY adquiriente, proveedor, correlativo",
    dtype={'proveedor': str, 'adquiriente': str, 'correlativo': str, 'numero_documento_referencia': str}, con=warehouse)

facturas_noanuladas['guia'] = np.where(
    (facturas_noanuladas['tipo_documento_referencia'] == 9) &
    (facturas_noanuladas['numero_documento_referencia'].str[:4] == 'EG07'),
    facturas_noanuladas['numero_documento_referencia'].str.split('-').str[1].str.replace('|', '').astype(int).astype(
        str),
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

        if row['guia'] is not None and row['guia'] != 'None' and not pd.isna(row['guia']):
            guia = fnmatch.filter(archivos, f'{row['proveedor']}-09-EG07-{row['guia']}*')
            if guia:
                merger.append(os.path.join(directorio, guia[0]))
            else:
                print(f'Falta la guia EG07-{row['guia']} de {row['proveedor']}')

    merger.write(f'{periodo}_{adquiriente}_{lista_adquiriente['alias'].iloc[0]}.pdf')
    merger.close()

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

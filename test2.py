import zipfile
import os
import pandas as pd

from Querys import ventas

ventas_periodo = ventas[ventas['periodo_tributario'] == 202501]

ventas_periodo['nombre_xml'] = ventas_periodo.apply(lambda row: 'FACTURA' + row['numero_serie'] + '-' + str(row['numero_correlativo']) + str(row['ruc']) + '.xml', axis=1)


print(ventas_periodo['nombre_xml'].tolist())


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

"""
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Token del bot
BOT_TOKEN = "tu_token_aqui"

# Comando para enviar el chat ID
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"Tu chat ID es: {chat_id}")

def main():
    # Crear aplicación
    application = Application.builder().token(BOT_TOKEN).build()

    # Agregar manejador de comandos
    application.add_handler(CommandHandler("start", start))

    # Iniciar el bot
    application.run_polling()  # Usa run_polling directamente

if __name__ == "__main__":
    main()

"""
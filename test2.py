import pandas as pd
import os

import requests
import json
from datetime import datetime

def login():
    # URL de tu API en Spring Boot (ajusta la ruta según tu Controller en Java)
    url_api = "http://localhost:8080/api/contribuyentes"

    # 1. Aquí simularíamos la extracción de tu base de datos PostgreSQL/MySQL
    # cursor.execute("SELECT ruc, certificado_base64, ... FROM empresas WHERE id = 1")
    # datos_db = cursor.fetchone()

    # 2. El Payload estructurado exactamente como lo espera tu Java
    payload = {
        "ruc": "20606283858",
        "razonSocial": "IMPULSA OUTSOURCED INTREPRENEURSHIP S.A.C.",
        "nombreComercial": "IMPULSA OE",
        "direccion": "AV. A 225 URB LAS LOMAS",
        "ubigeo": "015094",
        "departamento": "LIMA",
        "provincia": "LIMA",
        "distrito": "RIMAC",
        "usuarioSol": "TONERTAT",
        "passwordSol": "rcavinsio",
        "certificadoP12Base64": "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",  # Tu string larguísimo
        "certPassword": "cAn995PBjZ8RXCs",
        "certVence": "2029-03-26",
        "greClientId": "",
        "greClientSecret": ""
    }

    # 3. Cabeceras HTTP estándar
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        print(f"[*] Enviando configuración a la API: {url_api}")

        # 4. Lanzar la petición POST
        response = requests.post(url_api, json=payload, headers=headers)

        # 5. Interceptar errores HTTP (400 Bad Request, 500 Internal Error)
        response.raise_for_status()

        # Si llega aquí, el código fue 200 o 201 (Éxito)
        print("[+] ¡Éxito! La API respondió:")
        print(json.dumps(response.json(), indent=2))

    except requests.exceptions.HTTPError as http_err:
        print(f"[-] Error HTTP devuelto por Spring Boot: {http_err}")
        # Imprime el mensaje de error exacto que generó Java
        print(f"Detalle: {response.text}")
    except Exception as err:
        print(f"[-] Error de conexión o sistema: {err}")

def configurar_empresa_en_api():
    # URL de tu API en Spring Boot (ajusta la ruta según tu Controller en Java)
    url_api = "http://localhost:8080/api/contribuyentes"

    # 1. Aquí simularíamos la extracción de tu base de datos PostgreSQL/MySQL
    # cursor.execute("SELECT ruc, certificado_base64, ... FROM empresas WHERE id = 1")
    # datos_db = cursor.fetchone()
       # 2. El Payload estructurado exactamente como lo espera tu Java
    payload = {
        "ruc": "20606283858",
        "razonSocial": "IMPULSA OUTSOURCED INTREPRENEURSHIP S.A.C.",
        "nombreComercial": "IMPULSA OE",
        "direccion": "AV. A 225 URB LAS LOMAS",
        "ubigeo": "015094",
        "departamento": "LIMA",
        "provincia": "LIMA",
        "distrito": "RIMAC",
        "usuarioSol": "TONERTAT",
        "passwordSol": "rcavinsio",
        "certificadoP12Base64": "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",  # Tu string larguísimo
        "certPassword": "cAn995PBjZ8RXCs",
        "certVence": "2029-03-26",
        "greClientId": "",
        "greClientSecret": ""
    }

    # 3. Cabeceras HTTP estándar
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        print(f"[*] Enviando configuración a la API: {url_api}")

        # 4. Lanzar la petición POST
        response = requests.post(url_api, json=payload, headers=headers)

        # 5. Interceptar errores HTTP (400 Bad Request, 500 Internal Error)
        response.raise_for_status()

        # Si llega aquí, el código fue 200 o 201 (Éxito)
        print("[+] ¡Éxito! La API respondió:")
        print(json.dumps(response.json(), indent=2))

    except requests.exceptions.HTTPError as http_err:
        print(f"[-] Error HTTP devuelto por Spring Boot: {http_err}")
        # Imprime el mensaje de error exacto que generó Java
        print(f"Detalle: {response.text}")
    except Exception as err:
        print(f"[-] Error de conexión o sistema: {err}")


def emitir_factura():
    # URL de tu API local apuntando al RUC de la empresa emisora
    url = "http://localhost:8080/api/20606283858/facturas"
    token = 'eyJhbGciOiJIUzM4NCJ9.eyJzdWIiOiIyMDYwNjI4Mzg1OCIsInNvbF91c2VyIjoiVE9ORVJUQVQiLCJpYXQiOjE3NzUxODc0NzksImV4cCI6MTc3NTIzMDY3OX0.4jonFkQuTl3hcw-aTM8_rMraDo7wfts5r5VbRnkKZRNG73Xme5viiK-Fucbgxjdn'

    # Cabeceras
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # El cuerpo de la factura (Payload)
    payload = {
        "serie": "F001",
        "fechaEmision": "2026-03-27",
        "moneda": "PEN",
        "receptor": {
            "tipoDocumento": "6",
            "nroDocumento": "20614301172",
            "razonSocial": "SERVICIOS VARIOS GSMT E.I.R.L.",
            "direccion": "AV. LOS HEROES KM. 1098 LIMA - LIMA - VILLA EL SALVADOR"
        },
        "items": [
            {
                "descripcion": "Servicio de consultoría",
                "cantidad": 1,
                "precioUnitario": 118.00,
                "tipoAfectacionIgv": "10"  # 10 = Gravado - Operación Onerosa
            }
        ]
    }

    try:
        print(f"[*] Enviando factura a la API de Spring Boot...")

        # El parámetro json=payload de requests hace automáticamente el json.dumps()
        # y asegura que el encoding sea correcto.
        response = requests.post(url, json=payload, headers=headers)

        # Validar si el código HTTP es de error (ej. 400 o 500)
        response.raise_for_status()

        print("[+] ¡Petición exitosa! Respuesta del servidor:")
        # Imprimir la respuesta formateada (útil para ver el XML Base64 o el hash)
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))

    except requests.exceptions.HTTPError as http_err:
        print(f"[-] Error HTTP devuelto por Spring Boot: {http_err}")
        print(f"[-] Detalle: {response.text}")
    except Exception as err:
        print(f"[-] Error de conexión: {err}")

if __name__ == "__main__":
    #configurar_empresa_en_api()
    emitir_factura()

"""
RETIRAR LOS XML QUE YA ESTAN ANALIZADOS Y SUBIDOS A LA BASE DE DATOS
ventas_periodo = ventas[ventas['periodo_tributario'] == 202501]

ventas_periodo['nombre_xml'] = ventas_periodo.apply(lambda row: 'FACTURA' + row['numero_serie'] + '-' + str(row['numero_correlativo']) + str(row['ruc']) + '.xml', axis=1)


print(ventas_periodo['nombre_xml'].tolist())

for a in ventas_periodo['nombre_xml'].tolist():
    os.remove('E:/TODOS LOS XML/'+a)
"""
#VERIFICAR FACTURAS BANCARIZADAS POR PROVEEDOR
#VERIFICAR PERIODO DE LAS FACTURAS BANCARIZADAS POR PROVEEDOR
#VERIFICAR ADQUIRIENTES DE LAS FACTURAS BANCARIZADAS
#VERIFICAR PEDIDOS DE ESOS PROVEEDORES BANCARIZADOS
#VERIFICAR OTROS PEDIDOS
#EMITIR LOS MANIFIESTAMENTE PENDIENTES Y URGENTES
#VERIFICAR LOS OTROS PEDIDOS

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

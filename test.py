import xml.etree.ElementTree as ET

# Función para parsear el XML
def parse_ubl_invoice(xml_file):
    # Cargar el archivo XML
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Definir los namespaces de UBL 2.1
    namespaces = {
        'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
        'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
        '': 'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2'
    }

    # Extraer información básica
    invoice_id = root.find('cbc:ID', namespaces).text
    issue_date = root.find('cbc:IssueDate', namespaces).text

    # Extraer RUC del emisor (AccountingSupplierParty)
    supplier_ruc = root.find(
        './/cac:AccountingSupplierParty/cac:Party/cac:PartyIdentification/cbc:ID',
        namespaces
    ).text

    # Extraer ítems de la factura
    items = []
    for invoice_line in root.findall('.//cac:InvoiceLine', namespaces):
        item_id = invoice_line.find('cbc:ID', namespaces).text
        quantity = invoice_line.find('cbc:InvoicedQuantity', namespaces).text
        description = invoice_line.find('.//cac:Item/cbc:Description', namespaces).text
        price = invoice_line.find('.//cac:Price/cbc:PriceAmount', namespaces).text
        items.append({
            'id': item_id,
            'quantity': quantity,
            'description': description,
            'price': price
        })

    # Extraer total de impuestos
    tax_total = root.find('.//cac:TaxTotal/cbc:TaxAmount', namespaces).text

    # Retornar los datos extraídos
    return {
        'invoice_id': invoice_id,
        'issue_date': issue_date,
        'supplier_ruc': supplier_ruc,
        'items': items,
        'tax_total': tax_total
    }

# Ejemplo de uso
xml_file = 'C:/Users/Raknaros/Desktop/xmlprueba/FACTURAE001-49220610737553.xml'  # Reemplaza con la ruta a tu archivo XML
try:
    invoice_data = parse_ubl_invoice(xml_file)
    print("Información extraída:")
    print(f"Número de factura: {invoice_data['invoice_id']}")
    print(f"Fecha de emisión: {invoice_data['issue_date']}")
    print(f"RUC del emisor: {invoice_data['supplier_ruc']}")
    print("Ítems:")
    for item in invoice_data['items']:
        print(f" - Ítem {item['id']}: {item['description']}, Cantidad: {item['quantity']}, Precio: {item['price']}")
    print(f"Total de impuestos: {invoice_data['tax_total']}")
except FileNotFoundError:
    print("El archivo XML no se encontró.")
except AttributeError:
    print("Error al parsear el XML. Verifica la estructura del archivo.")

"""
def job():
    print("I'm running")
    sleep(2)  # Simula una tarea que toma tiempo


scheduler = BlockingScheduler()

# Configura el ejecutor de hilos, si no lo especificas, usa uno por defecto con 10 hilos
scheduler.add_executor('threadpool', max_workers=20)

# Añade trabajos al scheduler
scheduler.add_job(job, 'interval', seconds=3, id='job1')
scheduler.add_job(job, 'interval', seconds=3, id='job2')

# Comienza la ejecución del scheduler
scheduler.start()
"""




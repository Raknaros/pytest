import os
from dotenv import load_dotenv
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account  # <-- 1. Importa la clase Account

load_dotenv()
# Configura tus credenciales
address = os.getenv("HL_WALLET_ADDRESS")
private_key_str = os.getenv("HL_API_WALLET_PRIVATE_KEY") # <-- Renombra a str

if not address or not private_key_str:
    raise ValueError("Configura HL_WALLET_ADDRESS y HL_API_WALLET_PRIVATE_KEY en tus variables de entorno.")

# --- INICIO DE LA CORRECCIÓN ---

# 2. Asegúrate de que la clave NO tenga el prefijo "0x"
#    La función from_key lo espera como bytes crudos o un string hex sin 0x
if private_key_str.startswith("0x"):
    private_key_str = private_key_str[2:]

# 3. Crea el objeto wallet (Account) a partir del string de la clave
try:
    wallet = Account.from_key(private_key_str)
except Exception as e:
    print(f"Error al crear la wallet desde la private key: {e}")
    print("Asegúrate de que HL_API_WALLET_PRIVATE_KEY sea una clave privada hexadecimal válida.")
    raise

# (Opcional pero recomendado) Verifica que la clave y la dirección coincidan
if wallet.address.lower() != address.lower():
    print(f"¡Advertencia! La clave privada genera una dirección ({wallet.address})")
    print(f"que no coincide con tu HL_WALLET_ADDRESS ({address}).")
    # Descomenta la siguiente línea si quieres que esto sea un error fatal
    # raise ValueError("La clave privada no corresponde a la dirección de la wallet.")

# --- FIN DE LA CORRECCIÓN ---


# Inicializa el cliente de Exchange (usa MAINNET_API_URL para producción, o TESTNET para pruebas)
base_url = constants.MAINNET_API_URL  # Cambia a constants.TESTNET_API_URL si es test

# 4. Pasa el *objeto wallet*, no el string de la clave privada
exchange = Exchange(address, base_url, None, wallet)

# Parámetros de la orden (cambia estos valores según tu trade)
coin = "BTC"  # Asset, ej. "BTC", "ETH", etc.
is_buy = True  # True para compra (long), False para venta (short)
sz = 0.0001  # Tamaño de la posición (en unidades del asset)
limit_px = 60000.0  # Precio limit para la entry order

# Calcula precios para TP y SL (asumiendo porcentaje)
tp_percentage = 0.003  # 0.3% de ganancia
sl_percentage = 0.005  # 0.5% de pérdida
if is_buy:
    tp_trigger_px = limit_px * (1 + tp_percentage)  # TP por encima del entry
    sl_trigger_px = limit_px * (1 - sl_percentage)  # SL por debajo del entry
    tp_sl_buy = False  # Para cerrar long, vendes
else:
    tp_trigger_px = limit_px * (1 - tp_percentage)  # TP por debajo para short
    sl_trigger_px = limit_px * (1 + sl_percentage)  # SL por encima para short
    tp_sl_buy = True  # Para cerrar short, compras

# 1. Coloca la limit order de entry
print("Colocando orden de entrada...")
entry_order_type = {"limit": {"tif": "Gtc"}}  # Good til canceled
entry_result = exchange.order(coin, is_buy, sz, limit_px, entry_order_type, reduce_only=False)
print("Resultado de la limit order de entry:", entry_result)

# ... (El resto de tu código de TP/SL sigue igual)

# Nota: Espera a que la entry se llene antes de colocar TP/SL (en un bot real, monitorea con info.user_state o websockets).
# Para simplicidad, asumimos que se coloca después.

# 2. Coloca el Take Profit (TP) como trigger market (con slippage ~10%)
tp_order_type = {"trigger": {"isMarket": True, "triggerPx": tp_trigger_px, "tpsl": "tp"}}
tp_limit_px = tp_trigger_px * (1.01 if is_buy else 0.99)  # Precio limit agresivo para slippage (ajusta)
tp_result = exchange.order(coin, tp_sl_buy, sz, tp_limit_px, tp_order_type, reduce_only=True)
print("Resultado del Take Profit:", tp_result)

# 3. Coloca el Stop Loss (SL) como trigger market
sl_order_type = {"trigger": {"isMarket": True, "triggerPx": sl_trigger_px, "tpsl": "sl"}}
sl_limit_px = sl_trigger_px * (0.99 if is_buy else 1.01)  # Precio limit agresivo para slippage
sl_result = exchange.order(coin, tp_sl_buy, sz, sl_limit_px, sl_order_type, reduce_only=True)
print("Resultado del Stop Loss:", sl_result)

"""
def parse_ubl_invoice(xml_content):
    # Parsear el XML desde una cadena
    root = ET.fromstring(xml_content)

    # Definir los namespaces usados en el XML
    namespaces = {
        'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
        'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
        'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
        'sac': 'urn:sunat:names:specification:ubl:peru:schema:xsd:SunatAggregateComponents-1',
        '': 'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2'
    }

    # Lista para almacenar los datos de cada línea
    rows = []

    # Información general de la factura
    general_data = {
        'ubl_version': root.find('cbc:UBLVersionID', namespaces).text or "",
        'customization_id': root.find('cbc:CustomizationID', namespaces).text or "",
        'invoice_id': root.find('cbc:ID', namespaces).text or "",
        'issue_date': root.find('cbc:IssueDate', namespaces).text or "",
        'issue_time': root.find('cbc:IssueTime', namespaces).text or "",
        'invoice_type_code': root.find('cbc:InvoiceTypeCode', namespaces).text or "",
        'note': root.find('cbc:Note', namespaces).text if root.find('cbc:Note', namespaces) is not None else "",
        'currency_code': root.find('cbc:DocumentCurrencyCode', namespaces).text or ""
    }

    # Documento de referencia (Guía de Remisión)
    despatch_ref = root.find('.//cac:DespatchDocumentReference', namespaces)
    despatch_data = {
        'despatch_document_id': despatch_ref.find('cbc:ID', namespaces).text if despatch_ref is not None else "",
        'despatch_document_type_code': despatch_ref.find('cbc:DocumentTypeCode',
                                                         namespaces).text if despatch_ref is not None else "",
        'despatch_document_type': despatch_ref.find('cbc:DocumentType',
                                                    namespaces).text if despatch_ref is not None else ""
    }

    # Emisor (AccountingSupplierParty)
    supplier = root.find('.//cac:AccountingSupplierParty/cac:Party', namespaces)
    supplier_data = {
        'supplier_ruc': supplier.find('.//cac:PartyIdentification/cbc:ID', namespaces).text or "",
        'supplier_name': supplier.find('.//cac:PartyLegalEntity/cbc:RegistrationName', namespaces).text or "",
        'supplier_address_type_code': "",
        'supplier_city_name': "",
        'supplier_country_subentity': "",
        'supplier_country_subentity_code': "",
        'supplier_district': "",
        'supplier_address_line': "",
        'supplier_country_code': ""
    }
    supplier_address = supplier.find('.//cac:RegistrationAddress', namespaces)
    if supplier_address is not None:
        supplier_data.update({
            'supplier_address_type_code': supplier_address.find('cbc:AddressTypeCode', namespaces).text or "",
            'supplier_city_name': supplier_address.find('cbc:CityName', namespaces).text or "",
            'supplier_country_subentity': supplier_address.find('cbc:CountrySubentity', namespaces).text or "",
            'supplier_country_subentity_code': supplier_address.find('cbc:CountrySubentityCode', namespaces).text or "",
            'supplier_district': supplier_address.find('cbc:District', namespaces).text or "",
            'supplier_address_line': supplier_address.find('.//cac:AddressLine/cbc:Line', namespaces).text or "",
            'supplier_country_code': supplier_address.find('.//cac:Country/cbc:IdentificationCode',
                                                           namespaces).text or ""
        })

    # Cliente (AccountingCustomerParty)
    customer = root.find('.//cac:AccountingCustomerParty/cac:Party', namespaces)
    customer_data = {
        'customer_ruc': customer.find('.//cac:PartyIdentification/cbc:ID', namespaces).text or "",
        'customer_name': customer.find('.//cac:PartyLegalEntity/cbc:RegistrationName', namespaces).text or "",
        'customer_address_type_code': "",
        'customer_city_name': "",
        'customer_country_subentity': "",
        'customer_country_subentity_code': "",
        'customer_district': "",
        'customer_address_line': "",
        'customer_country_code': ""
    }
    customer_address = customer.find('.//cac:RegistrationAddress', namespaces)
    if customer_address is not None:
        customer_data.update({
            'customer_address_type_code': customer_address.find('cbc:AddressTypeCode', namespaces).text or "",
            'customer_city_name': customer_address.find('cbc:CityName', namespaces).text or "",
            'customer_country_subentity': customer_address.find('cbc:CountrySubentity', namespaces).text or "",
            'customer_country_subentity_code': customer_address.find('cbc:CountrySubentityCode', namespaces).text or "",
            'customer_district': customer_address.find('cbc:District', namespaces).text or "",
            'customer_address_line': customer_address.find('.//cac:AddressLine/cbc:Line', namespaces).text or "",
            'customer_country_code': customer_address.find('.//cac:Country/cbc:IdentificationCode',
                                                           namespaces).text or ""
        })

    # Comprador (BuyerCustomerParty)
    buyer = root.find('.//cac:BuyerCustomerParty/cac:Party/cac:PartyLegalEntity/cac:RegistrationAddress', namespaces)
    buyer_data = {
        'buyer_city_name': "",
        'buyer_country_subentity': "",
        'buyer_country_subentity_code': "",
        'buyer_district': "",
        'buyer_address_line': "",
        'buyer_country_code': ""
    }
    if buyer is not None:
        buyer_data.update({
            'buyer_city_name': buyer.find('cbc:CityName', namespaces).text or "",
            'buyer_country_subentity': buyer.find('cbc:CountrySubentity', namespaces).text or "",
            'buyer_country_subentity_code': buyer.find('cbc:CountrySubentityCode', namespaces).text or "",
            'buyer_district': buyer.find('cbc:District', namespaces).text or "",
            'buyer_address_line': buyer.find('.//cac:AddressLine/cbc:Line', namespaces).text or "",
            'buyer_country_code': buyer.find('.//cac:Country/cbc:IdentificationCode', namespaces).text or ""
        })

    # Forma de pago
    payment_terms = root.findall('.//cac:PaymentTerms', namespaces)
    payment_data = {}
    for term in payment_terms:
        term_id = term.find('cbc:PaymentMeansID', namespaces).text
        payment_data[f'payment_{term_id}_id'] = term.find('cbc:ID', namespaces).text or ""
        payment_data[f'payment_{term_id}_means_id'] = term_id
        payment_data[f'payment_{term_id}_amount'] = term.find('cbc:Amount', namespaces).text or ""
        due_date = term.find('cbc:PaymentDueDate', namespaces)
        payment_data[f'payment_{term_id}_due_date'] = due_date.text if due_date is not None else ""

    # Totales de impuestos
    tax_total = root.find('.//cac:TaxTotal', namespaces)
    tax_total_data = {
        'tax_total_amount': tax_total.find('cbc:TaxAmount', namespaces).text or ""
    }
    tax_subtotal = tax_total.find('.//cac:TaxSubtotal', namespaces)
    tax_subtotal_data = {
        'tax_subtotal_taxable_amount': tax_subtotal.find('cbc:TaxableAmount', namespaces).text or "",
        'tax_subtotal_tax_amount': tax_subtotal.find('cbc:TaxAmount', namespaces).text or "",
        'tax_category_id': tax_subtotal.find('.//cac:TaxCategory/cbc:ID', namespaces).text or "",
        'tax_scheme_id': tax_subtotal.find('.//cac:TaxScheme/cbc:ID', namespaces).text or "",
        'tax_scheme_name': tax_subtotal.find('.//cac:TaxScheme/cbc:Name', namespaces).text or "",
        'tax_scheme_type_code': tax_subtotal.find('.//cac:TaxScheme/cbc:TaxTypeCode', namespaces).text or ""
    }

    # Totales monetarios
    legal_monetary = root.find('.//cac:LegalMonetaryTotal', namespaces)
    monetary_data = {
        'line_extension_amount': legal_monetary.find('cbc:LineExtensionAmount', namespaces).text or "",
        'allowance_total_amount': legal_monetary.find('cbc:AllowanceTotalAmount', namespaces).text or "",
        'charge_total_amount': legal_monetary.find('cbc:ChargeTotalAmount', namespaces).text or "",
        'prepaid_amount': legal_monetary.find('cbc:PrepaidAmount', namespaces).text or "",
        'payable_amount': legal_monetary.find('cbc:PayableAmount', namespaces).text or ""
    }

    # Líneas de la factura (ítems)
    for line in root.findall('.//cac:InvoiceLine', namespaces):
        item_data = {
            'item_id': line.find('cbc:ID', namespaces).text or "",
            'invoiced_quantity': line.find('cbc:InvoicedQuantity', namespaces).text or "",
            'unit_code': line.find('cbc:InvoicedQuantity', namespaces).attrib.get('unitCode', ""),
            'line_extension_amount': line.find('cbc:LineExtensionAmount', namespaces).text or "",
            'free_of_charge_indicator': line.find('cbc:FreeOfChargeIndicator', namespaces).text or ""
        }

        # Precio de referencia
        pricing_ref = line.find('.//cac:PricingReference/cac:AlternativeConditionPrice', namespaces)
        item_data.update({
            'pricing_reference_price_amount': pricing_ref.find('cbc:PriceAmount', namespaces).text or "",
            'pricing_reference_price_type_code': pricing_ref.find('cbc:PriceTypeCode', namespaces).text or ""
        })

        # Descuentos o cargos
        allowance_charge = line.find('.//cac:AllowanceCharge', namespaces)
        item_data.update({
            'allowance_charge_indicator': allowance_charge.find('cbc:ChargeIndicator', namespaces).text or "",
            'allowance_charge_amount': allowance_charge.find('cbc:Amount', namespaces).text or ""
        })

        # Impuestos por ítem
        item_tax_total = line.find('.//cac:TaxTotal', namespaces)
        item_tax_subtotal = item_tax_total.find('.//cac:TaxSubtotal', namespaces)
        item_data.update({
            'item_tax_total_amount': item_tax_total.find('cbc:TaxAmount', namespaces).text or "",
            'item_tax_subtotal_taxable_amount': item_tax_subtotal.find('cbc:TaxableAmount', namespaces).text or "",
            'item_tax_subtotal_tax_amount': item_tax_subtotal.find('cbc:TaxAmount', namespaces).text or "",
            'item_tax_category_id': item_tax_subtotal.find('.//cac:TaxCategory/cbc:ID', namespaces).text or "",
            'item_tax_category_percent': item_tax_subtotal.find('.//cac:TaxCategory/cbc:Percent',
                                                                namespaces).text or "",
            'item_tax_exemption_reason_code': item_tax_subtotal.find('.//cac:TaxCategory/cbc:TaxExemptionReasonCode',
                                                                     namespaces).text or "",
            'item_tax_scheme_id': item_tax_subtotal.find('.//cac:TaxScheme/cbc:ID', namespaces).text or "",
            'item_tax_scheme_name': item_tax_subtotal.find('.//cac:TaxScheme/cbc:Name', namespaces).text or "",
            'item_tax_scheme_type_code': item_tax_subtotal.find('.//cac:TaxScheme/cbc:TaxTypeCode',
                                                                namespaces).text or ""
        })

        # Descripción del ítem
        item_data.update({
            'item_description': line.find('.//cac:Item/cbc:Description', namespaces).text or "",
            'item_sellers_id': line.find('.//cac:SellersItemIdentification/cbc:ID', namespaces).text or ""
        })

        # Precio unitario
        item_data.update({
            'price_amount': line.find('.//cac:Price/cbc:PriceAmount', namespaces).text or ""
        })

        # Combinar todos los datos en una fila
        row = {
            **general_data,
            **despatch_data,
            **supplier_data,
            **customer_data,
            **buyer_data,
            **payment_data,
            **tax_total_data,
            **tax_subtotal_data,
            **monetary_data,
            **item_data
        }
        rows.append(row)

    return rows


def process_xml_folder(folder_path):
    all_rows = []

    # Recorrer la carpeta
    for root_dir, _, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root_dir, file_name)

            if file_name.lower().endswith('.xml'):
                # Procesar archivos XML sueltos
                try:
                    with open(file_path, 'r', encoding='ISO-8859-1') as f:
                        xml_content = f.read()
                    rows = parse_ubl_invoice(xml_content)
                    all_rows.extend(rows)
                    print(f"Procesado: {file_name}")
                except Exception as e:
                    print(f"Error al procesar {file_name}: {e}")

            elif file_name.lower().endswith('.zip'):
                # Procesar archivos ZIP
                try:
                    with zipfile.ZipFile(file_path, 'r') as z:
                        for zipped_file in z.namelist():
                            if zipped_file.lower().endswith('.xml'):
                                with z.open(zipped_file) as f:
                                    xml_content = f.read().decode('ISO-8859-1')
                                rows = parse_ubl_invoice(xml_content)
                                all_rows.extend(rows)
                                print(f"Procesado: {zipped_file} (dentro de {file_name})")
                except Exception as e:
                    print(f"Error al procesar {file_name}: {e}")

    # Crear DataFrame
    if all_rows:
        df = pd.DataFrame(all_rows)
        return df
    else:
        print("No se encontraron datos para procesar.")
        return pd.DataFrame()


# Configuración
folder_path = 'C:/Users/Raknaros/Desktop/xmlprueba'  # Reemplaza con la ruta a tu carpeta
output_file = 'facturas.csv'  # Archivo de salida

# Procesar y guardar
df = process_xml_folder(folder_path)
if not df.empty:
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Datos guardados en {output_file}")
    print(f"Total de filas: {len(df)}")
else:
    print("No se generó ningún DataFrame.")
"""

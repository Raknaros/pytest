import os
import xml.etree.ElementTree as ET
from datetime import datetime


# Define the XML namespaces used in SUNAT invoices
NAMESPACES = {
    'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
    'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
    'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
    'sac': 'urn:sunat:names:specification:ubl:peru:schema:xsd:SunatAggregateComponents-1',
    'ds': 'http://www.w3.org/2000/09/xmldsig#',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

def print_xml_structure(element, level=0):
    """Helper function to print XML structure"""
    print('  ' * level + f'<{element.tag}>')
    for child in element:
        print_xml_structure(child, level + 1)

def safe_find_text(element, xpath, namespaces):
    """Safely find text content of an element, returning None if not found"""
    found = element.find(xpath, namespaces)
    if found is None:
        print(f"Warning: Element not found for xpath: {xpath}")
        return None
    return found.text

def safe_find_attr(element, xpath, attr, namespaces):
    """Safely find attribute of an element, returning None if not found"""
    found = element.find(xpath, namespaces)
    if found is None:
        print(f"Warning: Element not found for xpath: {xpath}")
        return None
    return found.get(attr)

def parse_invoice(xml_file_path):
    try:
        print(f"\nProcessing file: {xml_file_path}")

        # Parse the XML file
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Print the root element to help debug namespace issues
        print("\nRoot element:", root.tag)
        print("Root attributes:", root.attrib)

        # Initialize invoice data dictionary
        invoice_data = {}

        # Basic invoice information
        invoice_data['invoice_number'] = safe_find_text(root, './/cbc:ID', NAMESPACES)
        invoice_data['issue_date'] = safe_find_text(root, './/cbc:IssueDate', NAMESPACES)
        invoice_data['invoice_type'] = safe_find_text(root, './/cbc:InvoiceTypeCode', NAMESPACES)

        # Supplier information
        supplier = root.find('.//cac:AccountingSupplierParty', NAMESPACES)
        if supplier is not None:
            invoice_data['supplier'] = {
                'ruc': safe_find_text(supplier, './/cbc:ID', NAMESPACES),
                'name': safe_find_text(supplier, './/cbc:Name', NAMESPACES),
                'address': {
                    'street': safe_find_text(supplier, './/cac:PostalAddress/cbc:StreetName', NAMESPACES),
                    'city': safe_find_text(supplier, './/cac:PostalAddress/cbc:CityName', NAMESPACES),
                    'country': safe_find_text(supplier, './/cac:PostalAddress/cbc:CountrySubentity', NAMESPACES)
                }
            }
        else:
            invoice_data['supplier'] = None
            print("Warning: Supplier information not found")

        # Customer information
        customer = root.find('.//cac:AccountingCustomerParty', NAMESPACES)
        if customer is not None:
            invoice_data['customer'] = {
                'ruc': safe_find_text(customer, './/cbc:ID', NAMESPACES),
                'name': safe_find_text(customer, './/cbc:Name', NAMESPACES),
                'address': {
                    'street': safe_find_text(customer, './/cac:PostalAddress/cbc:StreetName', NAMESPACES),
                    'city': safe_find_text(customer, './/cac:PostalAddress/cbc:CityName', NAMESPACES),
                    'country': safe_find_text(customer, './/cac:PostalAddress/cbc:CountrySubentity', NAMESPACES)
                }
            }
        else:
            invoice_data['customer'] = None
            print("Warning: Customer information not found")

        # Invoice lines
        invoice_lines = []
        for line in root.findall('.//cac:InvoiceLine', NAMESPACES):
            line_data = {
                'id': safe_find_text(line, './/cbc:ID', NAMESPACES),
                'quantity': safe_find_text(line, './/cbc:InvoicedQuantity', NAMESPACES),
                'unit_code': safe_find_attr(line, './/cbc:InvoicedQuantity', 'unitCode', NAMESPACES),
                'description': safe_find_text(line, './/cbc:Description', NAMESPACES),
                'price': safe_find_text(line, './/cac:Price/cbc:PriceAmount', NAMESPACES),
                'currency': safe_find_attr(line, './/cac:Price/cbc:PriceAmount', 'currencyID', NAMESPACES)
            }
            invoice_lines.append(line_data)
        invoice_data['lines'] = invoice_lines

        # Tax information
        tax_totals = root.findall('.//cac:TaxTotal', NAMESPACES)
        invoice_data['taxes'] = []
        for tax in tax_totals:
            tax_data = {
                'amount': safe_find_text(tax, './/cbc:TaxAmount', NAMESPACES),
                'currency': safe_find_attr(tax, './/cbc:TaxAmount', 'currencyID', NAMESPACES),
                'type': safe_find_text(tax, './/cac:TaxSubtotal/cac:TaxCategory/cac:TaxScheme/cbc:ID', NAMESPACES)
            }
            if any(tax_data.values()):  # Only add if at least one value is not None
                invoice_data['taxes'].append(tax_data)

        # Total amounts
        invoice_data['totals'] = {
            'total_igv': safe_find_text(root, './/cac:TaxTotal/cbc:TaxAmount', NAMESPACES),
            'total_amount': safe_find_text(root, './/cac:LegalMonetaryTotal/cbc:PayableAmount', NAMESPACES),
            'currency': safe_find_attr(root, './/cac:LegalMonetaryTotal/cbc:PayableAmount', 'currencyID', NAMESPACES)
        }

        return invoice_data

    except Exception as e:
        print(f"Error processing file {xml_file_path}: {str(e)}")
        print("Please check if the XML file follows the SUNAT format")
        return None

# Directory where XML files are located
xml_directory = 'C:/Users/Raknaros/Desktop/xmlprueba'

# Process all XML files in the directory
for filename in os.listdir(xml_directory):
    if filename.upper().endswith('.XML'):  # Case-insensitive check
        file_path = os.path.join(xml_directory, filename)
        invoice_data = parse_invoice(file_path)

        if invoice_data:
            print("\nInvoice Information:")
            print(f"Number: {invoice_data['invoice_number']}")
            print(f"Date: {invoice_data['issue_date']}")
            print(f"Type: {invoice_data['invoice_type']}")

            if invoice_data['supplier']:
                print("\nSupplier:")
                print(f"RUC: {invoice_data['supplier']['ruc']}")
                print(f"Name: {invoice_data['supplier']['name']}")

            if invoice_data['customer']:
                print("\nCustomer:")
                print(f"RUC: {invoice_data['customer']['ruc']}")
                print(f"Name: {invoice_data['customer']['name']}")

            print("\nTotals:")
            print(f"Total IGV: {invoice_data['totals']['total_igv']}")
            print(f"Total Amount: {invoice_data['totals']['total_amount']} {invoice_data['totals']['currency']}")

            print("\nNumber of lines:", len(invoice_data['lines']))
            print("Number of taxes:", len(invoice_data['taxes']))
            print("-" * 80)
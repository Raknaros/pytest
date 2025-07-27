from escpos.printer import Network

# Reemplaza "192.168.1.100" con la IP de tu impresora de red
# Y 9100 con el puerto si es diferente (9100 es común para Ethernet)
try:
    p = Network("192.168.123.100", port=9100)

    p.set(align='center', font='b', height=2, width=2)
    p.text("--- MI NEGOCIO ---\n")

    p.set(align='left', font='a', height=1, width=1)
    p.text("Direccion: Av. Siempreviva 123\n")
    p.text("Tel: 987654321\n")
    p.text("Fecha: 08/07/2025 16:00\n")
    p.text("--------------------------------\n")
    p.text("Producto        Cant.  Precio  Total\n")
    p.text("--------------------------------\n")
    p.text("Cafe Latte      1      3.50    3.50\n")
    p.text("Medialuna       2      1.20    2.40\n")
    p.text("--------------------------------\n")

    p.set(align='right', font='b', height=1, width=1)
    p.text("Total: S/ 5.90\n")
    p.text("--------------------------------\n")

    p.set(align='center')
    p.qr("https://tu-pagina-web.com/gracias", size=8)
    p.text("¡Gracias por tu compra!\n")
    p.text("Visitanos de nuevo\n")

    p.cut() # Corta el papel

except Exception as e:
    print(f"Error al imprimir: {e}")
    print("Asegúrate de que la impresora esté conectada por red y la IP sea correcta.")
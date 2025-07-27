import sqlite3
import os

DB_FILE = ('servicios_varios_gsmt.db')

def setup_database():
    """Crea la tabla de comandas si no existe."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_documento INTEGER NOT NULL,
            nombre_completo TEXT NOT NULL,
            direccion TEXT NOT NULL,
            telefono TEXT,
            correo TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS almacen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        cantidad INTEGER NOT NULL,
        unidad_medida TEXT,
        categoria TEXT,
        tipo_operacion TEXT NOT NULL,
        documento_relacionado TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos_terminados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            descripcion TEXT NOT NULL,
            categoria TEXT NOT NULL,
            cantidad INTEGER NOT NULL,
            observaciones TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pedidos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente INTEGER NOT NULL,
            fecha_hora TEXT NOT NULL,
            destino TEXT NOT NULL,
            forma_pago TEXT NOT NULL,
            observacion TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS detalle_pedido (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido INTEGER NOT NULL,
            descripcion INTEGER NOT NULL,
            cantidad TEXT NOT NULL,
            precio TEXT NOT NULL,
            telefono TEXT,
            FOREIGN KEY (pedido) REFERENCES pedidos(id)
        )
    ''')
    conn.commit()
    conn.close()
    print(f"Base de datos '{DB_FILE}' configurada.")

def agregar_comanda(mesa, item, cantidad):
    """Agrega una nueva comanda y la persiste."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    fecha_hora_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("INSERT INTO comandas (mesa, item, cantidad, fecha_hora) VALUES (?, ?, ?, ?)",
                   (mesa, item, cantidad, fecha_hora_actual))
    conn.commit() # ¡Aquí se guardan los cambios en el archivo .db!
    conn.close()
    print(f"Comanda agregada: Mesa {mesa}, {cantidad}x {item}")

def ver_comandas():
    """Muestra todas las comandas persistidas."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, mesa, item, cantidad, fecha_hora FROM comandas")
    comandas = cursor.fetchall()
    conn.close()

    if not comandas:
        print("No hay comandas registradas.")
        return

    print("\n--- Comandas Registradas ---")
    for comanda in comandas:
        print(f"ID: {comanda[0]}, Mesa: {comanda[1]}, Item: {comanda[2]}, Cantidad: {comanda[3]}, Fecha: {comanda[4]}")
    print("----------------------------")

# --- Lógica principal ---
if __name__ == "__main__":
    from datetime import datetime

    # Paso 1: Configurar la base de datos (se ejecutará solo la primera vez o si el archivo no existe)
    setup_database()

    # Paso 2: Agregar algunas comandas
    """print("\nAgregando comandas...")
    agregar_comanda("Mesa 1", "Pizza Peperoni", 1)
    agregar_comanda("Mesa 3", "Cerveza Artesanal", 2)
    agregar_comanda("Mesa 1", "Ensalada César", 1) # Se añade a la misma DB persistida

    # Paso 3: Ver las comandas (estarán persistidas)
    ver_comandas()

    print("\nLa aplicación ha terminado. Si la vuelves a iniciar y llamas a 'ver_comandas()', verás las mismas comandas.")
"""
    # Puedes ejecutar este script varias veces. Las comandas se seguirán añadiendo y persistiendo.
    # Para ver que los cambios se mantienen, comenta las llamadas a agregar_comanda() después de la primera ejecución.
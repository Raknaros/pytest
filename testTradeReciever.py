import asyncio
import websockets
import json
import time

# --- Configuración ---
TARGET_COINS = ["BTC", "ETH"]


async def hyperliquid_websocket_client():
    """
    Cliente WebSocket final que usa las claves de datos correctas para interpretar
    el flujo de trades de Hyperliquid.
    """
    uri = "wss://api.hyperliquid.xyz/ws"

    while True:
        try:
            # Conexión con ping_interval para mantenerla activa
            async with websockets.connect(uri, ping_interval=20) as websocket:
                print("✅ Conectado al WebSocket de Hyperliquid.")

                # Suscripción a todos los canales
                for coin in TARGET_COINS:
                    subscription_message = {
                        "method": "subscribe",
                        "subscription": {
                            "type": "trades",
                            "coin": coin
                        }
                    }
                    await websocket.send(json.dumps(subscription_message))
                    print(f"-> Suscripción enviada para {coin}")

                print("\n--- Flujo de datos en tiempo real ---")

                # Bucle de escucha de mensajes
                while True:
                    message = await asyncio.wait_for(websocket.recv(), timeout=60.0)
                    data = json.loads(message)

                    # Descomenta la siguiente línea si quieres ver el mensaje CRUDO para depurar
                    # print(data)

                    if data.get("channel") == "trades" and "data" in data:
                        for trade in data["data"]:
                            # --- CORRECCIÓN FINAL DE CLAVES ---
                            # Usamos 'px', 'sz', 'time', y 'side' según la API.
                            # Usamos .get() por seguridad, aunque aquí ya esperamos que las claves existan.

                            coin_traded = trade.get("coin", "N/A")
                            # La clave para el lado es 'side', que puede ser 'A' (Ask/Venta) o 'B' (Bid/Compra)
                            side = "COMPRA ✅" if trade.get('side') == 'B' else "VENTA 🔻"
                            price = float(trade.get('px', 0.0))
                            size = float(trade.get('sz', 0.0))
                            timestamp_ms = int(trade.get('time', 0))

                            trade_time = time.strftime('%H:%M:%S', time.localtime(timestamp_ms / 1000))

                            print(
                                f"[{trade_time}] {coin_traded: <4} | {side: <10} | Tamaño: {size:<8.4f} | Precio: ${price:,.2f}")

        except websockets.exceptions.ConnectionClosed as e:
            print(f"\n🔌 Conexión cerrada. Reconectando en 5 segundos... Razón: {e}")
        except asyncio.TimeoutError:
            print("\n⌛ Timeout: No se recibieron datos en 60s. Reconectando para asegurar la conexión...")
        except Exception as e:
            print(f"\n❌ Error inesperado. Reconectando en 5 segundos... Error: {e}")

        await asyncio.sleep(5)


# --- Punto de entrada del programa ---
if __name__ == "__main__":
    try:
        print("✅ Iniciando cliente para Hyperliquid...")
        print(f"--- Monitoreando: {', '.join(TARGET_COINS)} ---")
        asyncio.run(hyperliquid_websocket_client())
    except KeyboardInterrupt:
        print("\n👋 Programa detenido por el usuario.")
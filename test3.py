import os
import time
import sys
from dotenv import load_dotenv
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account

# --- Cargar Variables de Entorno ---
load_dotenv()
HL_WALLET_ADDRESS = os.getenv("HL_WALLET_ADDRESS")
HL_API_WALLET_PRIVATE_KEY = os.getenv("HL_API_WALLET_PRIVATE_KEY")


# --- Función para manejar respuestas de la API ---
def handle_api_response(response, action_name):
    if isinstance(response, dict) and response.get("status") == "ok":
        print(f"✅ {action_name}: Éxito.")
        # Imprimir la respuesta completa para debug
        print(f"📄 Respuesta completa: {response}")
        statuses = response.get("data", {}).get("statuses", [])
        if statuses and isinstance(statuses[0], dict):
            status_info = statuses[0]
            if "oid" in status_info:
                return status_info
            elif "error" in status_info or "message" in status_info:
                print(f"❌ Error en '{action_name}': {status_info.get('error', status_info.get('message', 'Desconocido'))}")
                sys.exit(1)
        return statuses[0] if statuses else {}

    print(f"❌ Error en '{action_name}': La API devolvió -> {response}")
    sys.exit(1)


# --- Función Principal de Prueba ---
def place_test_order():
    if not HL_WALLET_ADDRESS or not HL_API_WALLET_PRIVATE_KEY:
        print("❌ Error: Asegúrate de que las variables de entorno estén en tu archivo .env")
        return
    print("✅ Credenciales cargadas correctamente.")

    try:
        # --- CORRECCIÓN: Convertir clave privada a objeto de cuenta ---
        account = Account.from_key(HL_API_WALLET_PRIVATE_KEY)
        print("🔐 Inicializando conexión...")
        info = Info(constants.MAINNET_API_URL, skip_ws=True)
        exchange = Exchange(
            account,  # Usar el objeto account en lugar de la string
            base_url=constants.MAINNET_API_URL,
            vault_address=None
        )

        # 3. Definir parámetros
        COIN = "BTC"
        LEVERAGE = 2
        MIN_ORDER_SIZE_BTC = 0.0001
        TAKE_PROFIT_PERCENT = 0.5
        STOP_LOSS_PERCENT = 0.3

        # --- PASO 1: CONFIGURAR APALANCAMIENTO ---
        print(f"\n⚙️ Configurando apalancamiento para {COIN} a {LEVERAGE}x en modo AISLADO...")
        leverage_response = exchange.update_leverage(LEVERAGE, COIN, is_cross=False)
        handle_api_response(leverage_response, "Configuración de Apalancamiento")

        # --- PASO 2: COLOCAR LA ORDEN ---
        all_market_data = info.all_mids()
        current_price = float(all_market_data[COIN])

        min_order_usd_value = MIN_ORDER_SIZE_BTC * current_price
        print(f"\n📈 Precio actual de {COIN}: ${current_price:,.2f}")
        print(f"ℹ️ Tamaño mínimo de orden: {MIN_ORDER_SIZE_BTC} BTC (aprox. ${min_order_usd_value:,.2f})")

        order_size = MIN_ORDER_SIZE_BTC

        entry_price = round(current_price * 0.9995 * 10) / 10  # Asegura múltiplo de 0.1 (tick size)
        print(f"\n⏳ Colocando orden de COMPRA para {order_size} {COIN} a un precio límite de ${entry_price:,.2f}...")

        order_response = exchange.order(COIN, True, order_size, entry_price, {"limit": {"tif": "Gtc"}})
        order_status = handle_api_response(order_response, "Colocación de Orden Principal")

        # --- PASO 3: COLOCAR TP/SL ---
        if not order_status or "oid" not in order_status:
            print(f"❌ Detalles de la respuesta: {order_status}")
            raise Exception("La orden principal no se colocó correctamente, no se pudo obtener el OID.")

        take_profit_price = round(entry_price * (1 + TAKE_PROFIT_PERCENT / 100), 1)
        stop_loss_price = round(entry_price * (1 - STOP_LOSS_PERCENT / 100), 1)

        print(f"\n🎯 Take Profit (TP) a: ${take_profit_price:,.2f}")
        print(f"🛡️ Stop Loss (SL) a: ${stop_loss_price:,.2f}")

        time.sleep(2)

        tp_order = {"trigger": {"triggerPx": take_profit_price, "isMarket": True, "tpsl": "tp"}}
        print("\n⏳ Colocando orden Take Profit...")
        tp_response = exchange.order(COIN, False, order_size, None, tp_order)
        handle_api_response(tp_response, "Colocación de Take Profit")

        time.sleep(2)

        sl_order = {"trigger": {"triggerPx": stop_loss_price, "isMarket": True, "tpsl": "sl"}}
        print("\n⏳ Colocando orden Stop Loss...")
        sl_response = exchange.order(COIN, False, order_size, None, sl_order)
        handle_api_response(sl_response, "Colocación de Stop Loss")

        print("\n🎉 ¡Prueba completada! Revisa tu cuenta en Hyperliquid.")

    except Exception as e:
        print(f"\n❌ Ocurrió un error irrecuperable durante la prueba: {e}")


# --- Ejecutar la Prueba ---
if __name__ == "__main__":
    place_test_order()
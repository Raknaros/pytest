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


# --- Función para redondear al tick_size correcto ---
def round_to_tick(price, tick_size):
    """Redondea un precio al múltiplo más cercano del tick_size."""
    return round(price / tick_size) * tick_size


# --- Función para manejar respuestas de la API (Robusta) ---
def handle_api_response(response, action_name):
    """Verifica si la respuesta de la API fue exitosa. Si no, imprime el error y detiene el script."""
    if isinstance(response, dict) and response.get("status") == "ok":
        print(f"✅ {action_name}: Éxito.")

        response_data = response.get("response", {})

        if response_data.get("type") == "default":
            print(f"📄 Respuesta de {action_name}: {response_data}")
            return [{}]

        data = response_data.get("data", {})
        statuses = data.get("statuses", [{}])

        if statuses and isinstance(statuses[0], dict) and "error" in statuses[0]:
            error_msg = statuses[0]["error"]
            print(f"❌ Error en la respuesta de '{action_name}': {error_msg}")
            sys.exit(1)

        print(f"📄 Respuesta completa de {action_name}: {response}")
        return statuses

    print(f"❌ Error en '{action_name}': La API devolvió -> {response}")
    sys.exit(1)


# --- Función Principal de Prueba ---
def place_test_order():
    if not HL_WALLET_ADDRESS or not HL_API_WALLET_PRIVATE_KEY:
        print("❌ Error: Asegúrate de que las variables de entorno estén en tu archivo .env")
        return
    print("✅ Credenciales cargadas correctamente.")

    try:
        # --- Inicialización Correcta ---
        account = Account.from_key(HL_API_WALLET_PRIVATE_KEY)
        print("🔐 Inicializando conexión...")
        info = Info(constants.MAINNET_API_URL, skip_ws=True)
        exchange = Exchange(
            account,
            base_url=constants.MAINNET_API_URL,
            vault_address=None
        )

        # --- Definir Parámetros ---
        COIN = "BTC"
        LEVERAGE = 2
        MIN_ORDER_SIZE_BTC = 0.0001
        TAKE_PROFIT_PERCENT = 0.5
        STOP_LOSS_PERCENT = 0.3
        TICK_SIZE = 0.5  # Tick size correcto para BTC
        print(f"ℹ️ Usando Tick Size (paso de precio) de: ${TICK_SIZE} para {COIN}")

        # --- PASO 1: CONFIGURAR APALANCAMIENTO ---
        #print(f"\n⚙️ Configurando apalancamiento para {COIN} a {LEVERAGE}x en modo AISLADO...")
        #leverage_response = exchange.update_leverage(LEVERAGE, COIN, is_cross=False)
        #handle_api_response(leverage_response, "Configuración de Apalancamiento")

        # --- PASO 2: CALCULAR Y REDONDEAR PRECIOS ---
        all_market_data = info.all_mids()
        current_price = float(all_market_data[COIN])
        print(f"\n📈 Precio actual de {COIN}: ${current_price:,.2f}")

        min_order_usd_value = MIN_ORDER_SIZE_BTC * current_price
        print(f"ℹ️ Tamaño mínimo de orden: {MIN_ORDER_SIZE_BTC} BTC (aprox. ${min_order_usd_value:,.2f})")

        order_size = MIN_ORDER_SIZE_BTC

        # Calculamos los precios como FLOAT, pero redondeados al tick_size
        entry_price = round_to_tick(current_price * 0.9995, TICK_SIZE)
        take_profit_price = round_to_tick(entry_price * (1 + TAKE_PROFIT_PERCENT / 100), TICK_SIZE)
        stop_loss_price = round_to_tick(entry_price * (1 - STOP_LOSS_PERCENT / 100), TICK_SIZE)

        print(f"\nCalculando precios redondeados al tick de {TICK_SIZE}:")
        print(f"  Precio de Entrada: ${entry_price:,.2f}")
        print(f"  Take Profit (TP):  ${take_profit_price:,.2f}")
        print(f"  Stop Loss (SL):    ${stop_loss_price:,.2f}")

        # --- PASO 3: COLOCAR ÓRDENES EN BATCH (Lote) ---
        print("\n⏳ Preparando lote de órdenes (Principal + TP + SL)...")

        order_requests = [
            # 1. Orden Principal (Límite, GTC)
            #    'limit_px' es un FLOAT
            {
                "coin": COIN,
                "is_buy": True,
                "sz": order_size,
                "limit_px": entry_price,
                "order_type": {"limit": {"tif": "Gtc"}},
                "reduce_only": False
            },

            # 2. Orden Take Profit (Trigger, Reduce-Only)
            #    NO tiene 'limit_px', 'triggerPx' es un FLOAT
            {
                "coin": COIN,
                "is_buy": False,
                "sz": order_size,
                "limit_px": 0,
                "order_type": {"trigger": {"triggerPx": take_profit_price, "isMarket": True, "tpsl": "tp"}},
                "reduce_only": True
            },

            # 3. Orden Stop Loss (Trigger, Reduce-Only)
            #    NO tiene 'limit_px', 'triggerPx' es un FLOAT
            {
                "coin": COIN,
                "is_buy": False,
                "sz": order_size,
                "limit_px": 0,
                "order_type": {"trigger": {"triggerPx": stop_loss_price, "isMarket": True, "tpsl": "sl"}},
                "reduce_only": True
            }
        ]

        # Usar 'bulk_orders' (el nombre de método correcto)
        batch_response = exchange.bulk_orders(order_requests)
        handle_api_response(batch_response, "Colocación de Lote de Órdenes")

        print("\n🎉 ¡Prueba completada! Revisa tu cuenta en Hyperliquid para ver las órdenes pendientes.")

    except Exception as e:
        print(f"\n❌ Ocurrió un error irrecuperable durante la prueba: {e}")


# --- Ejecutar la Prueba ---
if __name__ == "__main__":
    place_test_order()
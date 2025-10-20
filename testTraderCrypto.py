import os
import time
import sys
from dotenv import load_dotenv
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account
from decimal import Decimal, ROUND_HALF_UP

# --- (Cargar Variables de Entorno) ---
load_dotenv()
HL_WALLET_ADDRESS = os.getenv("HL_WALLET_ADDRESS")
HL_API_WALLET_PRIVATE_KEY = os.getenv("HL_API_WALLET_PRIVATE_KEY")


# --- Función round_to_tick ---
def round_to_tick(price, tick_size):
    price_decimal = Decimal(str(price))
    tick_decimal = Decimal(str(tick_size))
    if tick_decimal == 0: return price_decimal
    rounded_price = (price_decimal / tick_decimal).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_decimal
    return rounded_price


# --- Obtener tick size dinámicamente ---
def get_tick_size(info, coin):
    """Obtiene el tick size real del activo desde la API"""
    try:
        meta = info.meta()
        universe = meta.get('universe', [])

        for asset in universe:
            if asset.get('name') == coin:
                tick_size_str = asset.get('szDecimals')
                if tick_size_str is not None:
                    tick_size = Decimal('10') ** (-int(tick_size_str))
                    return tick_size

        print(f"⚠️ No se encontró tick size para {coin}, usando valor por defecto")
        return Decimal('0.1')
    except Exception as e:
        print(f"⚠️ Error obteniendo tick size: {e}, usando valor por defecto")
        return Decimal('0.1')


# --- Función handle_api_response ---
def handle_api_response(response, action_name):
    if not isinstance(response, dict):
        print(f"❌ Error inesperado en '{action_name}': La API devolvió -> {response}")
        sys.exit(1)

    if response.get("status") == "ok":
        print(f"✅ {action_name}: Éxito.")
        response_data = response.get("response", {})
        if response_data.get("type") == "default":
            print(f"📄 Respuesta simple de {action_name}: {response_data}")
            return [{}]
        data = response_data.get("data", {})
        statuses = data.get("statuses", [])
        has_error = False
        for i, status in enumerate(statuses):
            if isinstance(status, dict) and "error" in status:
                error_msg = status["error"]
                print(f"❌ Error en la Orden #{i + 1} del lote '{action_name}': {error_msg}")
                has_error = True
        if has_error:
            print(f"📄 Respuesta completa (CON ERROR): {response}")
            sys.exit(1)
        print(f"📄 Respuesta completa de {action_name}: {response}")
        return statuses

    error_detail = response.get("error", "No details provided.")
    if error_detail == "No details provided.":
        error_detail = response.get("response", "No details provided.")
    print(f"❌ Error en '{action_name}': La API devolvió status '{response.get('status')}' -> {error_detail}")
    sys.exit(1)


# --- Función Principal de Prueba ---
def place_test_order():
    if not HL_WALLET_ADDRESS or not HL_API_WALLET_PRIVATE_KEY:
        print("❌ Error: Asegúrate de que las variables de entorno estén en tu archivo .env")
        return
    print("✅ Credenciales cargadas correctamente.")

    try:
        account = Account.from_key(HL_API_WALLET_PRIVATE_KEY)
        print("🔐 Inicializando conexión...")
        info = Info(constants.MAINNET_API_URL, skip_ws=True)
        exchange = Exchange(account, base_url=constants.MAINNET_API_URL)

        COIN = "BTC"
        LEVERAGE = 2
        MIN_ORDER_SIZE_BTC = 0.0001
        TAKE_PROFIT_PERCENT = 0.5
        STOP_LOSS_PERCENT = 0.3

        # Obtener tick size dinámicamente
        TICK_SIZE = get_tick_size(info, COIN)
        print(f"ℹ️ Tick Size obtenido de la API para {COIN}: {TICK_SIZE}")

        all_market_data = info.all_mids()
        current_price = float(all_market_data[COIN])
        print(f"\n📈 Precio actual de {COIN}: ${current_price:,.1f}")

        min_order_usd_value = MIN_ORDER_SIZE_BTC * current_price
        print(f"ℹ️ Tamaño mínimo de orden: {MIN_ORDER_SIZE_BTC} BTC (aprox. ${min_order_usd_value:,.2f})")

        order_size = MIN_ORDER_SIZE_BTC

        # --- Calcular precios usando Decimal ---
        entry_price_decimal = round_to_tick(Decimal(str(current_price)) * Decimal('0.999'), TICK_SIZE)

        take_profit_trigger_price_decimal = round_to_tick(
            entry_price_decimal * (Decimal('1') + Decimal(str(TAKE_PROFIT_PERCENT)) / Decimal('100')), TICK_SIZE)
        stop_loss_trigger_price_decimal = round_to_tick(
            entry_price_decimal * (Decimal('1') - Decimal(str(STOP_LOSS_PERCENT)) / Decimal('100')), TICK_SIZE)

        # ¡CORRECCIÓN CRÍTICA! Para BTC en Hyperliquid, TODOS los precios deben ser ENTEROS
        # Redondear a enteros independientemente del tick_size técnico
        entry_price_decimal = round(float(entry_price_decimal))
        take_profit_trigger_price_decimal = round(float(take_profit_trigger_price_decimal))
        stop_loss_trigger_price_decimal = round(float(stop_loss_trigger_price_decimal))

        # Para órdenes trigger market, limit_px debe estar MUY cerca del triggerPx
        # Usar el mismo precio que triggerPx para evitar problemas de validación
        take_profit_limit_px_decimal = take_profit_trigger_price_decimal
        stop_loss_limit_px_decimal = stop_loss_trigger_price_decimal

        print(f"\nCalculando precios (redondeados a enteros para BTC):")
        print(f"  Precio de Entrada:        ${entry_price_decimal:,.0f}")
        print(f"  Trigger Take Profit (TP): ${take_profit_trigger_price_decimal:,.0f}")
        print(f"  Límite TP (market):       ${take_profit_limit_px_decimal:,.0f}")
        print(f"  Trigger Stop Loss (SL):   ${stop_loss_trigger_price_decimal:,.0f}")
        print(f"  Límite SL (market):       ${stop_loss_limit_px_decimal:,.0f}")

        print("\n⏳ Preparando lote de órdenes (Principal + TP + SL)...")

        order_requests = [
            # 1. Orden Principal (Límite, GTC)
            {
                "coin": COIN,
                "is_buy": True,
                "sz": order_size,
                "limit_px": float(entry_price_decimal),
                "order_type": {"limit": {"tif": "Gtc"}},
                "reduce_only": False
            },
            # 2. Orden Take Profit (Trigger, Market con límite de slippage)
            {
                "coin": COIN,
                "is_buy": False,
                "sz": order_size,
                "limit_px": float(take_profit_limit_px_decimal),
                "order_type": {
                    "trigger": {
                        "triggerPx": float(take_profit_trigger_price_decimal),
                        "isMarket": True,
                        "tpsl": "tp"
                    }
                },
                "reduce_only": True
            },
            # 3. Orden Stop Loss (Trigger, Market con límite de slippage)
            {
                "coin": COIN,
                "is_buy": False,
                "sz": order_size,
                "limit_px": float(stop_loss_limit_px_decimal),
                "order_type": {
                    "trigger": {
                        "triggerPx": float(stop_loss_trigger_price_decimal),
                        "isMarket": True,
                        "tpsl": "sl"
                    }
                },
                "reduce_only": True
            }
        ]

        batch_response = exchange.bulk_orders(order_requests)
        statuses = handle_api_response(batch_response, "Colocación de Lote de Órdenes")

        print("\n🎉 ¡Prueba completada! Las órdenes han sido enviadas.")
        print("⚠️ IMPORTANTE: Las órdenes TP/SL son independientes y no OCO.")
        print("   Revisa tu cuenta en Hyperliquid para ver las órdenes pendientes.")

    except Exception as e:
        print(f"\n❌ Ocurrió un error irrecuperable durante la prueba: {e}")
        import traceback
        traceback.print_exc()


# --- Ejecutar la Prueba ---
if __name__ == "__main__":
    place_test_order()

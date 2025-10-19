import os
import time
import sys
from dotenv import load_dotenv
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from eth_account import Account
from decimal import Decimal, ROUND_HALF_UP

# --- Cargar Variables de Entorno ---
load_dotenv()
HL_WALLET_ADDRESS = os.getenv("HL_WALLET_ADDRESS")
HL_API_WALLET_PRIVATE_KEY = os.getenv("HL_API_WALLET_PRIVATE_KEY")


def round_to_tick(price, tick_size):
    price_decimal = Decimal(str(price))
    tick_decimal = Decimal(str(tick_size))
    if tick_decimal == 0: return price_decimal
    rounded_price = (price_decimal / tick_decimal).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_decimal
    return rounded_price


def get_tick_size(info, coin):
    """Obtiene el tick size real del activo desde la API"""
    try:
        meta = info.meta()
        universe = meta.get('universe', [])

        for asset in universe:
            if asset.get('name') == coin:
                # Intentar primero tickSize directo
                tick_size = asset.get('tickSize')
                if tick_size is not None:
                    return Decimal(str(tick_size))
                # Si no, usar pxDecimals si existe
                px_decimals = asset.get('pxDecimals')
                if px_decimals is not None:
                    return Decimal('10') ** (-int(px_decimals))
                # Si no, usar szDecimals como fallback (aunque es para size)
                sz_decimals = asset.get('szDecimals')
                if sz_decimals is not None:
                    return Decimal('10') ** (-int(sz_decimals))

        print(f"⚠️ No se encontró tick size para {coin}, usando valor por defecto")
        return Decimal('0.1')  # Valor por defecto conservador para BTC
    except Exception as e:
        print(f"⚠️ Error obteniendo tick size: {e}, usando valor por defecto")
        return Decimal('0.1')


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


def place_limit_order_with_tp_sl(coin, is_buy, order_size, limit_price, take_profit_percent, stop_loss_percent, leverage=1):
    """
    Coloca una limit order con take profit y stop loss en Hyperliquid.

    Parámetros:
    - coin: str, símbolo del activo (ej. "BTC")
    - is_buy: bool, True para long, False para short
    - order_size: float, tamaño de la orden
    - limit_price: float, precio límite de entrada
    - take_profit_percent: float, porcentaje de take profit (ej. 0.5 para 0.5%)
    - stop_loss_percent: float, porcentaje de stop loss (ej. 0.3 para 0.3%)
    - leverage: int, apalancamiento (por defecto 1)

    Retorna:
    - dict con resultados de las órdenes o None si falla
    """
    if not HL_WALLET_ADDRESS or not HL_API_WALLET_PRIVATE_KEY:
        print("❌ Error: Asegúrate de que las variables de entorno estén en tu archivo .env")
        return None
    print("✅ Credenciales cargadas correctamente.")

    try:
        account = Account.from_key(HL_API_WALLET_PRIVATE_KEY)
        print("🔐 Inicializando conexión...")
        info = Info(constants.MAINNET_API_URL, skip_ws=True)
        exchange = Exchange(account, base_url=constants.MAINNET_API_URL)

        # Obtener tick size dinámicamente
        tick_size = get_tick_size(info, coin)
        print(f"ℹ️ Tick Size obtenido de la API para {coin}: {tick_size}")

        # Para Hyperliquid BTC: siempre redondear a enteros independientemente del tick_size técnico
        # Esto es un requerimiento específico de la plataforma
        limit_price_decimal = round(Decimal(str(limit_price)))

        # Calcular TP y SL basados en el precio de entrada
        # Para Hyperliquid BTC: siempre redondear a enteros
        if is_buy:
            tp_trigger_price_decimal = round(
                float(limit_price_decimal) * (1 + take_profit_percent / 100))
            sl_trigger_price_decimal = round(
                float(limit_price_decimal) * (1 - stop_loss_percent / 100))
        else:
            tp_trigger_price_decimal = round(
                float(limit_price_decimal) * (1 - take_profit_percent / 100))
            sl_trigger_price_decimal = round(
                float(limit_price_decimal) * (1 + stop_loss_percent / 100))

        # Para TP/SL market: el limit_px debe ser válido según las reglas de Hyperliquid
        # Para trigger orders, limit_px puede ser None o un precio válido
        # Intentar con limit_px = None primero (market order pura)
        take_profit_limit_px_decimal = None
        stop_loss_limit_px_decimal = None

        print(f"\nCalculando precios redondeados al tick de {tick_size}:")
        print(f"  Precio de Entrada (Limit): ${float(limit_price_decimal):,.2f}")
        print(f"  Trigger Take Profit (TP): ${float(tp_trigger_price_decimal):,.2f}")
        print(f"  Límite TP: Market order (sin slippage)")
        print(f"  Trigger Stop Loss (SL):   ${float(sl_trigger_price_decimal):,.2f}")
        print(f"  Límite SL: Market order (sin slippage)")

        # PASO 1: Colocar la limit order de entrada
        print("\n📌 PASO 1: Colocando orden de ENTRADA (Limit)...")

        entry_response = exchange.order(
            coin,
            is_buy,
            order_size,
            float(limit_price_decimal),
            {"limit": {"tif": "Gtc"}},  # Good 'Til Canceled para limit orders
            reduce_only=False
        )
        entry_statuses = handle_api_response(entry_response, "Orden de Entrada")

        # Verificar que la orden se colocó (para limit orders, puede no ejecutarse inmediatamente)
        if not entry_statuses:
            print("⚠️ La orden de entrada no se colocó correctamente.")
            return None

        print(f"✅ Orden de entrada colocada a ${float(limit_price_decimal):,.2f}")

        # PASO 2: Esperar un momento para que la orden se registre
        print("\n⏳ Esperando 2 segundos para confirmar la orden...")
        time.sleep(2)

        # PASO 3: Colocar TP y SL como trigger orders individuales (ya que bulk_orders puede fallar)
        print("\n📌 PASO 2: Colocando órdenes TP/SL...")

        # Take Profit - Usar triggerPx como limit_px para evitar errores de validación
        tp_response = exchange.order(
            coin,
            not is_buy,  # Invertir dirección para cerrar
            order_size,
            float(tp_trigger_price_decimal),  # Usar triggerPx como limit_px
            {
                "trigger": {
                    "triggerPx": float(tp_trigger_price_decimal),
                    "isMarket": True,
                    "tpsl": "tp"
                }
            },
            reduce_only=True
        )
        tp_statuses = handle_api_response(tp_response, "Orden Take Profit")

        # Stop Loss - Usar triggerPx como limit_px para evitar errores de validación
        sl_response = exchange.order(
            coin,
            not is_buy,  # Invertir dirección para cerrar
            order_size,
            float(sl_trigger_price_decimal),  # Usar triggerPx como limit_px
            {
                "trigger": {
                    "triggerPx": float(sl_trigger_price_decimal),
                    "isMarket": True,
                    "tpsl": "sl"
                }
            },
            reduce_only=True
        )
        sl_statuses = handle_api_response(sl_response, "Orden Stop Loss")

        print("\n🎉 ¡Órdenes colocadas exitosamente!")
        print(f"   - Entrada (Limit): ${float(limit_price_decimal):,.2f}")
        print(f"   - Take Profit: ${float(tp_trigger_price_decimal):,.2f} ({'+' if is_buy else '-'}{take_profit_percent}%)")
        print(f"   - Stop Loss: ${float(sl_trigger_price_decimal):,.2f} ({'-' if is_buy else '+'}{stop_loss_percent}%)")
        print("\n⚠️ IMPORTANTE: Las órdenes TP/SL son independientes (no OCO).")
        print("   Si una se ejecuta, debes cancelar manualmente la otra.")

        return {
            "entry": entry_statuses,
            "tp": tp_statuses,
            "sl": sl_statuses
        }

    except Exception as e:
        print(f"\n❌ Ocurrió un error irrecuperable: {e}")
        import traceback
        traceback.print_exc()
        return None


def place_test_order():
    # Obtener precio actual del mercado
    account = Account.from_key(HL_API_WALLET_PRIVATE_KEY)
    info = Info(constants.MAINNET_API_URL, skip_ws=True)

    coin = "BTC"
    all_market_data = info.all_mids()
    current_price = float(all_market_data[coin])
    print(f"Precio actual de {coin}: ${current_price:,.2f}")

    # Redondear precio actual a 0 decimales para evitar problemas de tick size
    rounded_current_price = round(current_price)
    print(f"Precio redondeado a 0 decimales: ${rounded_current_price:,.0f}")

    # Calcular precios dinámicos basados en precio redondeado
    # Para long: limit_price ligeramente por encima del precio redondeado
    limit_price = rounded_current_price + 1  # +1 para asegurar que sea por encima

    # Calcular TP y SL con porcentajes razonables para scalping
    take_profit_percent = 0.5  # 0.5% TP
    stop_loss_percent = 0.3    # 0.3% SL

    # Calcular precios absolutos de TP/SL
    tp_price = rounded_current_price * (1 + take_profit_percent / 100)
    sl_price = rounded_current_price * (1 - stop_loss_percent / 100)

    # Redondear TP/SL a 0 decimales también
    tp_price_rounded = round(tp_price)
    sl_price_rounded = round(sl_price)

    print(f"Precios calculados (redondeados a 0 decimales):")
    print(f"  Limit Price: ${limit_price:,.0f}")
    print(f"  Take Profit: ${tp_price_rounded:,.0f} (+{take_profit_percent}%)")
    print(f"  Stop Loss: ${sl_price_rounded:,.0f} (-{stop_loss_percent}%)")

    # Ejemplo de uso de la función con precios dinámicos
    result = place_limit_order_with_tp_sl(
        coin=coin,
        is_buy=True,  # Long
        order_size=0.0001,
        limit_price=float(limit_price),
        take_profit_percent=take_profit_percent,
        stop_loss_percent=stop_loss_percent,
        leverage=1  # Leverage conservador para scalping
    )
    if result:
        print("Resultado:", result)
    else:
        print("Fallo en colocar las órdenes.")


if __name__ == "__main__":
    place_test_order()
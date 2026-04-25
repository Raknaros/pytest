import logging
import os
from dotenv import load_dotenv
import uvicorn
from contextlib import asynccontextmanager

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters
from fastapi import FastAPI, Request

# --- 1. Configuración ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
if not TOKEN:
    raise ValueError("No se encontró el TELEGRAM_BOT_TOKEN en el archivo .env")

# Creamos la aplicación del bot aquí para que esté disponible globalmente
# Usamos 'Application' en lugar de 'ApplicationBuilder' para un control más fino
bot_app = Application.builder().token(TOKEN).build()


# --- 2. Lógica del Bot (Handlers) ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Esta función se ejecuta cuando llega un mensaje de texto."""
    print("--- ¡La función handle_message ha sido llamada! ---")
    await update.message.reply_text("Recibido (vía Webhook)")


# Registramos el handler en la aplicación del bot
message_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message)
bot_app.add_handler(message_handler)


# --- 3. Lógica de FastAPI (Ciclo de Vida y Webhook) ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Esta función gestiona el arranque y apagado.
    Se ejecuta una sola vez cuando Uvicorn arranca.
    """
    logging.info("Iniciando la aplicación del bot...")
    await bot_app.initialize()  # Prepara el bot (inicia colas, etc.)
    # Webhook setup is now done here if not set manually
    # await bot_app.bot.set_webhook(url=f"{WEBHOOK_URL}/api/telegram/webhook}")

    yield  # La aplicación está viva y corriendo en este punto

    logging.info("Apagando la aplicación del bot...")
    await bot_app.shutdown()  # Limpia los recursos del bot al apagar


# Creamos la app de FastAPI y le asignamos el gestor de ciclo de vida
app = FastAPI(lifespan=lifespan)


@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    """Este es el endpoint que Telegram llamará."""
    print("\n--- ¡Webhook recibido! ---")
    try:
        update_data = await request.json()
        print("1. Datos JSON recibidos de Telegram:")
        print(update_data)

        update = Update.de_json(data=update_data, bot=bot_app.bot)
        print("2. Objeto 'Update' de Telegram creado con éxito.")

        await bot_app.process_update(update)
        print("3. La actualización fue procesada por la aplicación del bot.")

        return {"status": "ok"}
    except Exception as e:
        print(f"!!! OCURRIÓ UN ERROR DENTRO DEL WEBHOOK: {e}")
        logging.error("Error al procesar el webhook", exc_info=True)
        return {"status": "error"}


# --- 4. Ejecución del Servidor ---
if __name__ == "__main__":
    # Asegúrate de que el host sea "127.0.0.1"
    uvicorn.run(app, host="127.0.0.1", port=8001)
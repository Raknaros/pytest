from telegram import Bot
import asyncio

# Token de tu bot
BOT_TOKEN = "7563751301:AAHwAqNvDLcF3OWWvPoElpwuPGU3DSKg__o"
# Tu chat ID
CHAT_ID = "1340578043"

# Mensaje a enviar
MESSAGE = "Hola Gordinfla"

async def send_message():
    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.send_message(chat_id=CHAT_ID, text=MESSAGE)
        print("Mensaje enviado correctamente!")
    except Exception as e:
        print(f"Error al enviar mensaje: {e}")

# Ejecutar la función asíncrona
asyncio.run(send_message())
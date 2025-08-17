from google import genai
from google.genai import types
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

# Configura el cliente con la API Key desde variables de entorno
client = genai.Client(api_key=os.getenv('GOOGLE_API_KEY'))

# Define el modelo a usar desde variables de entorno
model_name = os.getenv('GOOGLE_AI_MODEL')

print(f"¡Hola! Soy Gemini ({model_name}). Comencemos a conversar. Escribe 'salir' para terminar.")

# Lista para mantener el historial de la conversación.
# Cada elemento será un diccionario con 'role' y 'parts'.
history = []

while True:
    user_message = input("Tú: ")
    if user_message.lower() == 'salir':
        print("Gemini: ¡Hasta luego!")
        break

    # Añade el mensaje del usuario al historial
    history.append(types.Content(parts=[types.Part(text=user_message)], role="user"))

    try:
        # Envía el historial completo de la conversación al modelo
        # para que tenga contexto.
        response = client.models.generate_content(
            model=model_name,
            contents=history,  # Pasamos todo el historial aquí
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_budget=0)  # Disables thinking
            )
        )

        # Accede al texto de la respuesta.
        # Puede variar si la respuesta tiene múltiples partes o no.
        # Aquí asumimos una respuesta de texto simple.
        model_response_text = ""
        if response.text:
            model_response_text = response.text
        elif response.candidates and response.candidates[0].content.parts:
            # Si no hay 'response.text', intenta acceder a las partes del primer candidato
            model_response_text = response.candidates[0].content.parts[0].text

        print(f"Gemini: {model_response_text}")

        # Añade la respuesta del modelo al historial
        history.append(types.Content(parts=[types.Part(text=model_response_text)], role="model"))

    except Exception as e:
        print(f"Ocurrió un error al enviar el mensaje: {e}")
        # En caso de error, es posible que el historial se haya corrompido,
        # o que la conexión fallara. Puedes decidir cómo manejarlo.
        # Aquí, simplemente imprimimos el error y permitimos seguir,
        # pero para robustez, podrías querer reiniciar el chat o salir.

# Opcional: Puedes acceder al historial de la conversación al final
print("\n--- Historial completo de la conversación ---")
for message_content in history:
    # `message_content.parts` es una lista, y cada parte puede ser de diferentes tipos.
    # Para simplicidad, asumimos que la primera parte es texto.
    text_part = message_content.parts[0].text if message_content.parts else "[No text part]"
    print(f"{message_content.role}: {text_part}")

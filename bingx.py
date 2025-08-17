#from telegram import Bot

import time
import requests
import hmac
from hashlib import sha256
from os import getenv
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Obtener variables de entorno
APIURL = getenv('BINGX_API_URL')
APIKEY = getenv('BINGX_API_KEY')
SECRETKEY = getenv('BINGX_SECRET_KEY')

def demo():
    payload = {}
    path = '/openApi/spot/v1/ticker/price'
    method = "GET"
    paramsMap = {
    "symbol": "BTC-USDT"
    }
    paramsStr = parseParam(paramsMap)
    return send_request(method, path, paramsStr, payload)

def get_sign(api_secret, payload):
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), digestmod=sha256).hexdigest()
    print("sign=" + signature)
    return signature


def send_request(method, path, urlpa, payload):
    url = "%s%s?%s&signature=%s" % (APIURL, path, urlpa, get_sign(SECRETKEY, urlpa))
    print(url)
    headers = {
        'X-BX-APIKEY': APIKEY,
    }
    response = requests.request(method, url, headers=headers, data=payload)
    return response.text

def parseParam(paramsMap):
    sortedKeys = sorted(paramsMap)
    paramsStr = "&".join(["%s=%s" % (x, paramsMap[x]) for x in sortedKeys])
    if paramsStr != "":
     return paramsStr+"&timestamp="+str(int(time.time() * 1000))
    else:
     return paramsStr+"timestamp="+str(int(time.time() * 1000))


if __name__ == '__main__':
    print("demo:", demo())



"""
# Configura tu bot
bot = Bot(token="TU_CLAVE_API_TELEGRAM")
chat_id = "TU_CHAT_ID"

# Envía una alerta
bot.send_message(chat_id=chat_id, text="¡Alerta! RSIfrom telegram import Bot

# Configura tu bot
bot = Bot(token="TU_CLAVE_API_TELEGRAM")
chat_id = "TU_CHAT_ID"

# Envía una alerta
bot.send_message(chat_id=chat_id, text="¡Alerta! RSI está en sobrecompra.") está en sobrecompra.")
"""

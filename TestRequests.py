import asyncio
import httpx

# Configuración base
API_URL = "https://s1api1.giumarchan.com/api/v1/sire/descargar"
PERIODO = "202605"

# Tu lista de RUCs
LISTA_RUCS = [
'20614161028']

TIPOS = ["ventas", "compras"]

async def solicitar_descarga(client, ruc, tipo):    
    """Realiza una única petición POST asíncrona."""
    payload = {
        "ruc": str(ruc),
        "periodo": PERIODO,
        "tipo": tipo
    }
    
    try:
        # Timeout extendido a 2 minutos por tratarse de scraping/descarga
        response = await client.post(API_URL, json=payload, timeout=120.0)
        
        if response.status_code in [200, 201, 202]:
            print(f"✅ [OK] RUC: {ruc} | Tipo: {tipo} | Respuesta: {response.status_code}")
        else:
            # Añadimos response.text al final para ver el log real del servidor
            print(f"⚠️ [ERROR] RUC: {ruc} | Tipo: {tipo} | Status: {response.status_code} | Detalle: {response.text}")
            
    except Exception as e:
        print(f"❌ [FALLO CRÍTICO] RUC: {ruc} | Tipo: {tipo} | Error: {str(e)}")

async def proceso_masivo():
    """Orquestador de las tareas paralelas."""
    # Usamos un solo cliente para reutilizar conexiones (Keep-Alive)
    async with httpx.AsyncClient() as client:
        tareas = []
        
        for ruc in LISTA_RUCS:
            for tipo in TIPOS:
                # Creamos la tarea pero no la ejecutamos aún
                tarea = solicitar_descarga(client, ruc, tipo)
                tareas.append(tarea)
        
        # Ejecutar todas las peticiones en paralelo
        print(f"🚀 Disparando {len(tareas)} peticiones asíncronas...")
        await asyncio.gather(*tareas)
        print("🏁 Todas las solicitudes han sido enviadas.")

if __name__ == "__main__":
    asyncio.run(proceso_masivo())
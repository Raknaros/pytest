from apscheduler.schedulers.blocking import BlockingScheduler
from time import sleep


def job():
    print("I'm running")
    sleep(2)  # Simula una tarea que toma tiempo


scheduler = BlockingScheduler()

# Configura el ejecutor de hilos, si no lo especificas, usa uno por defecto con 10 hilos
scheduler.add_executor('threadpool', max_workers=20)

# Añade trabajos al scheduler
scheduler.add_job(job, 'interval', seconds=3, id='job1')
scheduler.add_job(job, 'interval', seconds=3, id='job2')

# Comienza la ejecución del scheduler
scheduler.start()

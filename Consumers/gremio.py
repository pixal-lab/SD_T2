import time, BDD
import threading
from kafka import KafkaConsumer, TopicPartition
import asyncio
import json

servidores_bootstrap = 'kafka:9092'

topic_inscripcion = 'inscripcion'
topic_ventas = 'ventas'
topic_reposicion = 'reposicion'

master_header = ["nombre", "correo", "premium"]
venta_header = ["correo", "valor"]
repo_header = ["correo"]

insc_msgs = []
vent_msgs = []
repo_msgs = []


def leer_mensajes_no_leidos(topic):
    consumer = KafkaConsumer(
        bootstrap_servers=servidores_bootstrap,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) 
        )
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])

    for message in consumer:
        
        print(message.value)
        if topic == topic_inscripcion:
            insc_msgs.append(message.value)
        if topic == topic_ventas:
            vent_msgs.append(message.value)
        if topic == topic_reposicion:
            repo_msgs.append(message.value)
    consumer.close()

def aceptar_inscripciones():
    msgs = []
    l = len(insc_msgs)
    for i in range(l):
        msgs.append(insc_msgs.pop(0))
    inscripcion_premium = []
    inscripcion = []
    
    print(f"Leyendo inscripciones: {msgs}")
    for msg in msgs:
        # Utilizamos el método get() para evitar KeyError en caso de que la clave no exista en el mensaje
        nombre = msg["nombre"]
        correo = msg["correo"]
        premium = msg["premium"]

        # Verificamos los datos presentes antes de intentar usarlos
        if nombre is not None and correo is not None and premium is not None:
            if premium:
                inscripcion_premium.append([nombre, correo, premium])
            else:
                inscripcion.append([nombre, correo])
        else:
            print("Mensaje incompleto recibido.")


    # insertar en base de datos
    for i in range(len(inscripcion_premium)):
        nuevo_registro = {'nombre': inscripcion_premium[i][0], 'correo': inscripcion_premium[i][1], 'premium': True}
        BDD.agregar_registro("masters.csv", master_header, nuevo_registro)

    # insertar en base de datos
    for i in range(len(inscripcion)):
        # insertar en base de datos
        nuevo_registro = {'nombre': inscripcion[i][0], 'correo': inscripcion[i][1], 'premium': False}
        BDD.agregar_registro("masters.csv", master_header, nuevo_registro)


def leer_reposiciones():
    msgs = []
    l = len(repo_msgs)
    for i in range(l):
        msgs.append(repo_msgs.pop(0))
    print(f"Leyendo reposiciones: {msgs}")

    for msg in msgs:
        correo = msg["correo"]
        # Verificamos los datos presentes antes de intentar usarlos
        if correo is not None :
            #insertar en base de datos
            nueva_repo = {'correo': correo}
            BDD.agregar_registro("repos.csv", repo_header, nueva_repo)


def leer_ventas():
    msgs = []
    l = len(vent_msgs)
    for i in range(l):
        msgs.append(dict(vent_msgs.pop(0)))
    print(f"Leyendo ventas: {msgs}")
    for msg in msgs:
        # Utilizamos el método get() para evitar KeyError en caso de que la clave no exista en el mensaje
        correo = msg["correo"]
        valor = msg["valor"]
        # Verificamos los datos presentes antes de intentar usarlos
        if correo is not None and valor is not None:
            #insertar en base de datos
            nueva_venta = {'correo': correo, 'valor': valor}
            BDD.agregar_registro("ventas.csv", venta_header, nueva_venta)

def recuento_ventas_i(correo):
    cant_ventas = 0
    ganancias = 0
    # sumar las ventas
    ventas = BDD.get_ventas()

    if ventas:
        for venta in ventas:
            if venta[1] == correo:
                cant_ventas += 1
                ganancias += int(venta[2])
        return [cant_ventas, ganancias]

def recontar():
    data = []
    masters = BDD.get_masters()
    if masters:
        for master in masters:
            data.append(master + [recuento_ventas_i(master[2])])


        for d in data:
            print(f"Enviando correo a: {d[1]}, con correo {d[2]} y ventas y ganancias {d[4]}")
        # enviar por correo
    else:
        print("No hay maestros registrados")
    pass

def run():
    tiempo = 1
    while tiempo < 120:
        print(f"Tiempo: {tiempo}")
        aceptar_inscripciones()
        leer_reposiciones()
        leer_ventas()

        if tiempo % 60 == 0:
            recontar()
        tiempo += 1
        time.sleep(1)
    



if __name__ == "__main__":

    threads = []
    front = threading.Thread(target=run)
    front.start()
    threads.append(front)

    topics = [topic_inscripcion, topic_reposicion, topic_ventas]

    for topic in topics:
        t = threading.Thread(target=leer_mensajes_no_leidos, args=(topic,))
        t.start()
        threads.append(t)

    # Esperar a que todos los hilos finalicen
    for t in threads:
        t.join()


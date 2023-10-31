import time, BDD
from kafka import KafkaConsumer
import json

servidores_bootstrap = 'kafka:9092'
topic_inscripcion = 'inscripcion'
topic_ventas = 'ventas'
topic_reposicion = 'reposicion'

insc = KafkaConsumer(
    topic_inscripcion,
    bootstrap_servers=[servidores_bootstrap],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Se utiliza json.loads para deserializar el mensaje JSON
)
vent = KafkaConsumer(
    topic_ventas,
    bootstrap_servers=[servidores_bootstrap],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Se utiliza json.loads para deserializar el mensaje JSON
)
repo = KafkaConsumer(
    topic_reposicion,
    bootstrap_servers=[servidores_bootstrap],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Se utiliza json.loads para deserializar el mensaje JSON
)

master_header = ["nombre", "correo", "premium"]
venta_header = ["correo", "valor"]
repo_header = ["correo"]

def aceptar_inscripciones():
    # consumir del topic pagado
    inscripcion_premium = []
    inscripcion = []

    for msg in insc:
        # Utilizamos el método get() para evitar KeyError en caso de que la clave no exista en el mensaje
        nombre = msg.value.get("nombre")
        correo = msg.value.get("correo")
        premium = msg.value.get("premium")

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
    # consumir del topic reposiciones
    for msg in repo:
        # Utilizamos el método get() para evitar KeyError en caso de que la clave no exista en el mensaje
        correo = msg.value.get("correo")

        # Verificamos los datos presentes antes de intentar usarlos
        if correo is not None :
            #insertar en base de datos
            nueva_repo = {'correo': correo}
            BDD.agregar_registro("repos.csv", repo_header, nueva_repo)

def leer_ventas():
    # consumir del topic ventas
    for msg in vent:
        # Utilizamos el método get() para evitar KeyError en caso de que la clave no exista en el mensaje
        correo = msg.value.get("correo")
        valor = msg.value.get("valor")

        # Verificamos los datos presentes antes de intentar usarlos
        if correo is not None and valor is not None:
            #insertar en base de datos
            nueva_venta = {'correo': correo, 'valor': valor}
            BDD.agregar_registro("ventas.csv", venta_header, nueva_venta)

def recuento_ventas_i(id):
    cant_ventas = 0
    ganancias = 0
    # sumar las ventas
    ventas = BDD.get_ventas()

    if ventas:
        for venta in ventas:
            if venta[1] == id:
                cant_ventas += 1
                ganancias += int(venta[2])
        return [cant_ventas, ganancias]


def recontar():
    data = []
    masters = BDD.get_masters()
    if masters:
        for master in masters:
            data.append(master + [recuento_ventas_i(master[0])])


        for d in data:
            print(f"Enviando correo a: {d[1]}, con correo {d[2]} y ventas y ganancias {d[4]}")
        # enviar por correo
    else:
        print("No hay maestros registrados")
    pass

def main():
    tiempo = 0
    while tiempo < 120:
        print(f"Tiempo: {tiempo}")
        leer_reposiciones()
        aceptar_inscripciones()
        leer_ventas()

        if tiempo % 60:
            recontar()
        time.sleep(1)
        tiempo += 1
    
main()

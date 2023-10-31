from kafka import KafkaProducer
from json import dumps
import threading
import argparse
import time
import random
import string

servidores_bootstrap = 'kafka:9092'
topic_inscripcion = 'inscripcion'
topic_ventas = 'ventas'
topic_reposicion = 'reposicion'


productor = KafkaProducer(bootstrap_servers=[servidores_bootstrap])

def generar_id():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(1, 20)))

# Definición de la clase Productor
class Productor:
    def __init__(self, frecuencia_ventas, max_stock, nombre, correo, premium):
        # Constructor de la clase Productor
        self.frecuencia_ventas = frecuencia_ventas  # Frecuencia de ventas
        self.stock = 10  # Stock inicial
        self.max_stock = max_stock  # Stock máximo
        self.premium = premium  # Bandera para gratis o pagado (no se utiliza en este ejemplo)
        self.nombre = nombre
        self.correo = correo

    def solicitar_inscripcion(self):
        topic = topic_inscripcion
        mensaje = {
            "nombre": self.nombre,
            "correo": self.correo,
            "premium": self.premium
        }
        json_mensaje = dumps(mensaje).encode('utf-8')
        productor.send(topic, json_mensaje)

    def vender(self, valor):

        # Método para simular una venta
        if self.stock > 0:
            topic = topic_ventas
            mensaje = {
                "correo": self.correo,
                "valor": 30000
            }
            json_mensaje = dumps(mensaje).encode('utf-8')
            productor.send(topic, json_mensaje)
            self.stock -= 1
            print("Venta realizada. Stock restante:", self.stock)
        else:
            print("No hay suficiente stock para vender.")
            self.solicitar_reposicion()


    def solicitar_reposicion(self):
        topic = topic_reposicion 
        mensaje = {
            "correo": self.correo,
        }
        json_mensaje = dumps(mensaje).encode('utf-8')
        productor.send(topic, json_mensaje)

        # Método que simula el proceso de solicitud de reposición de stock
        print("Solicitando reposición...")
        self.stock = self.max_stock  # Reponer el stock al máximo


    def leer_correo(self):
        # Método que simula la lectura de correos y revisa estadísticas
        print("Leyendo correo y revisando estadísticas...")
        #recibe el nombre del correo y algun metodo para identificarlo.

    def run(self):
        # Método principal que ejecuta las acciones del Productor
        self.solicitar_inscripcion()  # Solicitar inscripción
        contador = 0
        while True:
            self.leer_correo()  # Leer el correo y revisar estadísticas
            if contador % self.frecuencia_ventas == 0:
                self.vender()  # Realizar una venta si es el momento
            if self.stock == 0:
                self.solicitar_reposicion()  # Solicitar reposición si no hay stock
            contador += 1
            time.sleep(1)  # Esperar 1 segundo entre iteraciones








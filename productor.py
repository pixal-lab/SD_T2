import time
import random

# Definición de la clase Productor
class Productor:
    def __init__(self, frecuencia_ventas, max_stock):
        # Constructor de la clase Productor
        self.frecuencia_ventas = int  # Frecuencia de ventas
        self.stock = 10  # Stock inicial
        self.max_stock = max_stock  # Stock máximo
        self.gratis_pagado = False  # Bandera para gratis o pagado (no se utiliza en este ejemplo)

    def solicitar_inscripcion(self):
        # Método que simula el proceso de solicitud de inscripción
        print("Solicitando inscripción...")
        # Supongamos que aquí se enviaría la solicitud y se esperaría la respuesta.

    def vender(self):
        # Método para simular una venta
        if self.stock > 0:
            self.stock -= 1
            print("Venta realizada. Stock restante:", self.stock)
        else:
            print("No hay suficiente stock para vender.")

    def solicitar_reposicion(self):
        # Método que simula el proceso de solicitud de reposición de stock
        print("Solicitando reposición...")
        self.stock = self.max_stock  # Reponer el stock al máximo

    def leer_correo(self):
        # Método que simula la lectura de correos y revisa estadísticas
        print("Leyendo correo y revisando estadísticas...")

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

# Uso del Productor
frecuencia_ventas = 5
max_stock = 10
productor = Productor(frecuencia_ventas, max_stock)
productor.run()






from kafka import KafkaProducer
from json import dumps


servidores_bootstrap = 'kafka:9092'
topic_inscripcion = 'inscripcion'
topic_ventas = 'ventas'
topic_reposicion = 'reposicion'

productor = KafkaProducer(bootstrap_servers=[servidores_bootstrap])

for i in range(10):
    mensaje = {
    "nombre": "nombre",
    "correo": "self.correo",
    "premium": "self.premium"
    }
    json_mensaje = dumps(mensaje).encode('utf-8')

    productor.send(topic_inscripcion, json_mensaje, timestamp_ms=604800000)

productor.close()
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

def asignar_particiones(topic, particiones):
    try:
        # Configuración del consumidor
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=None, auto_offset_reset='earliest')

        # Crear objetos de partición para el tema y las particiones específicas que deseas asignar


        # Asignar las particiones al consumidor
        consumer.assign([TopicPartition(topic, 0)])

        # Leer y mostrar los mensajes de las particiones asignadas
        for message in consumer:
            print(f'Mensaje recibido: {message.value.decode("utf-8")}')

    except NoBrokersAvailable:
        print('Error: No se pueden encontrar brokers de Kafka. Asegúrate de que Kafka esté en funcionamiento.')

if __name__ == '__main__':
    topic = 'ventas'  # Reemplaza 'mi-tema' con el nombre de tu tema
    particiones = 0  # Reemplaza con las particiones que desees asignar

    asignar_particiones(topic, particiones)

import productor as pd
import time
# una inscripción gratuita, que vende 1 producto cada 2 segundos y luego envía el comprobante.
master1 = pd.Productor(frecuencia_ventas=2, max_stock=10, nombre="Juan", correo="a", premium=False)
# Pasan 10 segundos
# y nuevamente llega una inscripción gratuita, la cual vende 1 producto cada 5 segundos y envía el comprobante,
master2 = pd.Productor(frecuencia_ventas=5, max_stock=10, nombre="Pedro", correo="b", premium=False)
# luego inmediatamente llega otra inscripción gratuita que vende 1 producto cada 4 segundos y envía el comprobante
master3 = pd.Productor(frecuencia_ventas=4, max_stock=10, nombre="Laura", correo="c", premium=False)
# y finalmente llega una inscripción pagado que vende 1 producto cada 3 segundos y envía el comprobante.
master4 = pd.Productor(frecuencia_ventas=3, max_stock=10, nombre="Carlos", correo="d", premium=True)

for i in range(10):
    print(f"tiempo:{i}")
    master1.run(i)
    master2.run(i)
    master3.run(i)
    master4.run(i)
    time.sleep(1)
    
master1.close()
master2.close()
master3.close()
master4.close()


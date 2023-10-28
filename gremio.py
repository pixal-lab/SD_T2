import time, BDD

master_header = ["nombre", "correo", "tipo"]
venta_header = ["id_master", "valor"]

def aceptar_inscripciones():
    # consumir del topic pagado
    inscripcion = []
    
    # insertar en base de datos
    nuevo_registro = {'nombre': 'Juan', 'correo':'abc@mail.com', 'tipo': 'premium'}
    BDD.agregar_registro("masters.csv", master_header, nuevo_registro)

    # consumir del topic gratuito

    # insertar en base de datos
    nuevo_registro = {'nombre': 'Juan2', 'correo':'abc2@mail.com', 'tipo': 'gratis'}
    BDD.agregar_registro("masters.csv", master_header, nuevo_registro)

    pass

def leer_reposiciones():
    # consumir del topic reposiciones
    pass

def leer_ventas():
    # consumir del topic ventas

    #insertar en base de datos Ejemplo:

    nueva_venta = {'id_master': '0', 'valor': 100}
    BDD.agregar_registro("ventas.csv", venta_header, nueva_venta)
    
    pass

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
        leer_ventas()
        # aceptar_inscripciones()

        if tiempo % 60:
            recontar()
        time.sleep(1)
        tiempo += 1
    





# main()
recontar()
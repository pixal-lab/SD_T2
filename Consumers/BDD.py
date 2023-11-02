import csv
def gestor(bdd):
    try:
        with open("gestor.csv", 'x', newline='') as archivo_csv:
            escritor_csv = csv.DictWriter(archivo_csv, fieldnames=["nombre_bdd", "last_id"])
            escritor_csv.writeheader()
        agregar_registro_gestor("gestor.csv",["nombre_bdd", "last_id"],{'nombre_bdd': bdd, 'last_id': 0})
    except FileExistsError:
        agregar_registro_gestor("gestor.csv",["nombre_bdd", "last_id"],{'nombre_bdd': bdd, 'last_id': 0})

# Función para agregar un registro a la base de datos
def agregar_registro_gestor(BD_master, encabezados, nuevo_registro):
    with open(BD_master, 'a', newline='') as archivo_csv:
        escritor_csv = csv.DictWriter(archivo_csv, fieldnames=encabezados)
        escritor_csv.writerow(nuevo_registro)
    add_index(BD_master)

# Función para agregar un registro a la base de datos
def agregar_registro(BD_master, encabezados, nuevo_registro):
    enc = ["id"] + encabezados
    try:
        with open(BD_master, 'x', newline='') as archivo_csv:
            enc = ["id"] + encabezados
            escritor_csv = csv.DictWriter(archivo_csv, fieldnames=enc)
            escritor_csv.writeheader()
        print(f'Se ha creado la base de datos: {BD_master}')
        gestor(BD_master)
        agregar_registro(BD_master, encabezados, nuevo_registro)
    except FileExistsError:
        with open(BD_master, 'a', newline='') as archivo_csv:
            escritor_csv = csv.DictWriter(archivo_csv, fieldnames=enc)
            new_reg = {'id': get_id(BD_master), **nuevo_registro}
            escritor_csv.writerow(new_reg)
        add_index(BD_master)
        print(f'Registro agregado con éxito: {nuevo_registro}')
        

# Función para consultar todos los registros en la base de datos
def consultar_registro(BD_master, campo_busqueda, valor_busqueda):
    try:
        with open(BD_master, 'r', newline='') as archivo_csv:
            lector_csv = csv.DictReader(archivo_csv)
            for row in lector_csv:
                if row[campo_busqueda] == valor_busqueda:
                    return row
    except FileNotFoundError:
        print(f'La base de datos en {BD_master} no existe.')

def add_index(bdd):
    try:
        with open("gestor.csv", 'r+', newline='') as archivo_csv:
            lector_csv = csv.reader(archivo_csv)
            lineas = list(lector_csv)
            modificado = False

            for i, fila in enumerate(lineas):
                if fila[1] != "last_id":
                    nombre, entero = fila[0], int(fila[1])
                    if nombre == bdd:
                        entero += 1
                        lineas[i] = [nombre, entero]
                        modificado = True
            if modificado:
                archivo_csv.seek(0)
                escritor_csv = csv.writer(archivo_csv)
                escritor_csv.writerows(lineas)
                archivo_csv.truncate()
                return True
            else:
                return False
    except FileNotFoundError:
        return False
    

def get_masters():
    try:
        with open("masters.csv", 'r+', newline='') as archivo_csv:
            lector_csv = csv.reader(archivo_csv)
            return(list(lector_csv)[1:])
    except FileNotFoundError:
        return False
def get_ventas():
    try:
        with open("ventas.csv", 'r+', newline='') as archivo_csv:
            lector_csv = csv.reader(archivo_csv)
            return(list(lector_csv)[1:])
    except FileNotFoundError:
        return False

def get_id(BD_master):
    row = consultar_registro("gestor.csv", "nombre_bdd", BD_master)
    return row['last_id']

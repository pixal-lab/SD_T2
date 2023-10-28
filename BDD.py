import csv
def gestor(bdd):
    try:
        print("a1")
        with open("gestor.csv", 'x', newline='') as archivo_csv:
            print("a2")
            escritor_csv = csv.DictWriter(archivo_csv, fieldnames=["nombre_bdd", "last_id"])
            escritor_csv.writeheader()
        agregar_registro_gestor("gestor.csv",["nombre_bdd", "last_id"],{'nombre_bdd': bdd, 'last_id': 0})
    except FileExistsError:
        print("a3")
        agregar_registro_gestor("gestor.csv",["nombre_bdd", "last_id"],{'nombre_bdd': bdd, 'last_id': 0})

# Función para agregar un registro a la base de datos
def agregar_registro_gestor(BD_master, encabezados, nuevo_registro):
    print("a6")
    with open(BD_master, 'a', newline='') as archivo_csv:
        escritor_csv = csv.DictWriter(archivo_csv, fieldnames=encabezados)
        escritor_csv.writerow(nuevo_registro)
    print('Registro agregado con éxito.')
    add_index(BD_master)

# Función para agregar un registro a la base de datos
def agregar_registro(BD_master, encabezados, nuevo_registro):
    enc = ["id"] + encabezados
    try:
        with open(BD_master, 'x', newline='') as archivo_csv:
            enc = ["id"] + encabezados
            escritor_csv = csv.DictWriter(archivo_csv, fieldnames=enc)
            escritor_csv.writeheader()
        print(f'Se ha creado la base de datos en {BD_master}')
        gestor(BD_master)
    except FileExistsError:
        with open(BD_master, 'a', newline='') as archivo_csv:
            escritor_csv = csv.DictWriter(archivo_csv, fieldnames=enc)
            new_reg = {'id': get_id(BD_master), **nuevo_registro}
            escritor_csv.writerow(new_reg)
        print('Registro agregado con éxito.')
        add_index(BD_master)
        

# Función para consultar todos los registros en la base de datos
def consultar_registro(BD_master, campo_busqueda, valor_busqueda):
    try:
        with open(BD_master, 'r', newline='') as archivo_csv:
            lector_csv = csv.DictReader(archivo_csv)
            for row in lector_csv:
                if row[campo_busqueda] == valor_busqueda:
                    print('Registro encontrado:')
                    print(row)
                    return row
                
            print('Registro no encontrado.')

    except FileNotFoundError:
        print(f'La base de datos en {BD_master} no existe.')

def add_index(bdd):
    print("a7")
    try:
        with open("gestor.csv", 'r+', newline='') as archivo_csv:
            print("a8")
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
            print("a9")
            if modificado:
                archivo_csv.seek(0)
                escritor_csv = csv.writer(archivo_csv)
                escritor_csv.writerows(lineas)
                archivo_csv.truncate()
                return True
            else:
                return False
    except FileNotFoundError:
        print("a10")
        return False
    
def get_id(BD_master):
    row = consultar_registro("gestor.csv", "nombre_bdd", BD_master)
    return row['last_id']
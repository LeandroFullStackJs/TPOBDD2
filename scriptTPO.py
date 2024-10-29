from pymongo import MongoClient
from neo4j import GraphDatabase
import pyodbc
import datetime

# Conexión a MongoDB
client_mongo = MongoClient("mongodb://localhost:27017/")
db_mongo = client_mongo['Rappi']
pedidos_mongo = db_mongo['pedidos']

# Conexión a Neo4j
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "admin123"))

# Conexión a SQL Server
conn_sql = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=rappi;UID=admin;PWD=admin')
cursor_sql = conn_sql.cursor()

# Función para registrar datos
def registrar_pedido_mongodb(pedido):
    pedidos_mongo.insert_one(pedido)
    print(f"Pedido registrado en MongoDB: {pedido}")

def registrar_entrega_neo4j(pedido_id, estado_entrega, tiempo_entrega):
    with neo4j_driver.session() as session:
        session.run(
            "CREATE (e:Entrega {pedido_id: $pedido_id, estado: $estado, tiempo: $tiempo})",
            pedido_id=pedido_id, estado=estado_entrega, tiempo=tiempo_entrega
        )
        print(f"Entrega registrada en Neo4j: Pedido {pedido_id}")

def registrar_pedido_sqlserver(pedido_id, total, tiempo_entrega):
    cursor_sql.execute(
        "INSERT INTO Pedidos (PedidoID, Total, TiempoEntrega) VALUES (?, ?, ?)",
        pedido_id, total, tiempo_entrega
    )
    conn_sql.commit()
    print(f"Pedido registrado en SQL Server: Pedido {pedido_id}")

# Casos de Uso
# 1. ¿Cuántos pedidos se realizan diariamente en diferentes ciudades?
def pedidos_diarios_por_ciudad():
    resultado = pedidos_mongo.aggregate([
        {"$group": {"_id": "$ciudad", "total_pedidos": {"$sum": 1}}},
        {"$sort": {"total_pedidos": -1}}
    ])
    for doc in resultado:
        print(f"Ciudad: {doc['_id']}, Total Pedidos: {doc['total_pedidos']}")

# 2. ¿Qué tipos de productos son más solicitados por los usuarios?
def productos_mas_solicitados():
    resultado = pedidos_mongo.aggregate([
        {"$unwind": "$productos"},
        {"$group": {"_id": "$productos.tipo", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ])
    for doc in resultado:
        print(f"Tipo de Producto: {doc['_id']}, Total: {doc['total']}")

# 3. ¿Cuáles son los restaurantes más populares entre los clientes?
def restaurantes_mas_populares():
    resultado = pedidos_mongo.aggregate([
        {"$group": {"_id": "$restaurante", "total_pedidos": {"$sum": 1}}},
        {"$sort": {"total_pedidos": -1}}
    ])
    for doc in resultado:
        print(f"Restaurante: {doc['_id']}, Total Pedidos: {doc['total_pedidos']}")

# 4. ¿Qué categorías de productos tienen mayor demanda durante los fines de semana?
def categorias_fin_semana():
    resultado = pedidos_mongo.aggregate([
        {"$match": {"fecha_pedido": {"$in": ["Saturday", "Sunday"]}}},
        {"$unwind": "$productos"},
        {"$group": {"_id": "$productos.categoria", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ])
    for doc in resultado:
        print(f"Categoría: {doc['_id']}, Total: {doc['total']}")

# 5. ¿Cuántos pedidos han superado los $50 y han sido entregados en menos de 30 minutos?
def pedidos_rapidos_mayores_50():
    cursor_sql.execute(
        "SELECT COUNT(*) FROM Pedidos WHERE Total > 50 AND TiempoEntrega < 30"
    )
    resultado = cursor_sql.fetchone()
    print(f"Pedidos > $50 y entregados en menos de 30 minutos: {resultado[0]}")

# 6. ¿Qué productos son solicitados más de 100 veces o tienen una calificación promedio superior a 4.5?
def productos_populares():
    resultado = pedidos_mongo.aggregate([
        {"$unwind": "$productos"},
        {"$group": {
            "_id": "$productos.nombre",
            "total_solicitudes": {"$sum": 1},
            "promedio_calificacion": {"$avg": "$productos.calificacion"}
        }},
        {"$match": {
            "$or": [{"total_solicitudes": {"$gt": 100}}, {"promedio_calificacion": {"$gt": 4.5}}]
        }},
        {"$sort": {"total_solicitudes": -1}}
    ])
    for doc in resultado:
        print(f"Producto: {doc['_id']}, Total Solicitudes: {doc['total_solicitudes']}, Calificación Promedio: {doc['promedio_calificacion']}")

# Menú principal
def menu_principal():
    while True:
        print("\n=== Menú Principal ===")
        print("1. Registrar Pedido")
        print("2. Consultar Pedidos Diarios por Ciudad")
        print("3. Consultar Productos Más Solicitados")
        print("4. Consultar Restaurantes Más Populares")
        print("5. Consultar Categorías de Productos con Mayor Demanda en Fin de Semana")
        print("6. Consultar Pedidos > $50 entregados en < 30 minutos")
        print("7. Consultar Productos solicitados > 100 veces o calificación > 4.5")
        print("8. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            pedido_id = input("Ingrese el ID del pedido: ")
            cliente = input("Ingrese el nombre del cliente: ")
            ciudad = input("Ingrese la ciudad del cliente: ")
            restaurante = input("Ingrese el nombre del restaurante: ")
            total = float(input("Ingrese el total del pedido: "))
            tiempo_entrega = int(input("Ingrese el tiempo de entrega (en minutos): "))
            productos = []

            while True:
                nombre_producto = input("Ingrese el nombre del producto: ")
                tipo_producto = input("Ingrese el tipo del producto: ")
                categoria_producto = input("Ingrese la categoría del producto: ")
                calificacion_producto = float(input("Ingrese la calificación del producto: "))
                productos.append({
                    "nombre": nombre_producto,
                    "tipo": tipo_producto,
                    "categoria": categoria_producto,
                    "calificacion": calificacion_producto
                })
                agregar_mas = input("¿Desea agregar otro producto? (s/n): ")
                if agregar_mas.lower() != "s":
                    break

            fecha_pedido = datetime.datetime.now().strftime("%A")
            nuevo_pedido = {
                "pedido_id": pedido_id,
                "cliente": cliente,
                "ciudad": ciudad,
                "restaurante": restaurante,
                "productos": productos,
                "fecha_pedido": fecha_pedido,
                "total": total,
                "tiempo_entrega": tiempo_entrega
            }

            registrar_pedido_mongodb(nuevo_pedido)
            registrar_entrega_neo4j(pedido_id, "entregado", tiempo_entrega)
            registrar_pedido_sqlserver(pedido_id, total, tiempo_entrega)

        elif opcion == "2":
            pedidos_diarios_por_ciudad()
        elif opcion == "3":
            productos_mas_solicitados()
        elif opcion == "4":
            restaurantes_mas_populares()
        elif opcion == "5":
            categorias_fin_semana()
        elif opcion == "6":
            pedidos_rapidos_mayores_50()
        elif opcion == "7":
            productos_populares()
        elif opcion == "8":
            print("Saliendo del programa...")
            break
        else:
            print("Opción no válida. Por favor, selecciona una opción válida.")

# Ejecución del menú principal
menu_principal()

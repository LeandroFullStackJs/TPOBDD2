from pymongo import MongoClient
import pyodbc
from neo4j import GraphDatabase

# Conexión MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["rappi"]
mongo_pedidos = mongo_db["pedidos"]

# Conexión SQL Server
sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=localhost;'
                          'DATABASE=rappi;'
                          'UID=admin;'
                          'PWD=admin')

# Conexión Neo4j
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "admin123"))

# Función para ingresar múltiples productos por teclado
def ingresar_productos():
    productos = []
    while True:
        nombre = input("Ingrese nombre del producto: ")
        cantidad = int(input(f"Ingrese la cantidad de {nombre}: "))
        precio = float(input("Ingrese precio del producto: "))
        calificacion = float(input("Ingrese calificación del producto (1-5): "))
        producto = {"nombre": nombre, "cantidad": cantidad, "precio": precio, "calificacion": calificacion}
        productos.append(producto)

        continuar = input("¿Desea agregar otro producto? (s/n): ").strip().lower()
        if continuar != 's':
            break
    return productos

# Función para calcular el total del pedido
def calcular_total_pedido(productos):
    total = sum(p["cantidad"] * p["precio"] for p in productos)
    return total

# Función para registrar productos y categorías en Neo4j
def registrar_producto_neo4j(producto):
    with neo4j_driver.session() as session:
        query = """
        MERGE (p:Producto {nombre: $nombre})
        SET p.precio = $precio, p.calificacion = $calificacion
        MERGE (c:Categoria {nombre: $categoria})
        MERGE (p)-[:PERTENECE_A]->(c)
        """
        session.run(query, 
                    nombre=producto["nombre"], 
                    precio=producto["precio"], 
                    calificacion=producto["calificacion"], 
                    categoria=producto["categoria"])
        print(f"Producto '{producto['nombre']}' registrado en Neo4j.")

 # Función para registrar un pedido con estatus y múltiples productos en SQL Server y MongoDB
def registrar_pedido(pedido_id, cliente_id, establecimiento_id, repartidor_id, 
                     direccion_id, productos, ciudad, fecha_pedido, estado, tiempo_entrega):
    total = calcular_total_pedido(productos)

    # Guardar en SQL Server
    cursor = sql_conn.cursor()
    cursor.execute("""
    INSERT INTO Pedidos (pedido_id, cliente_id, establecimiento_id, repartidor_id, direccion_id, total, estado, fecha, tiempo_entrega)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (pedido_id, cliente_id, establecimiento_id, repartidor_id, direccion_id, total, estado, fecha_pedido, tiempo_entrega))
    sql_conn.commit()

    # Guardar detalles de productos
    for producto in productos:
        cursor.execute("""
        INSERT INTO DetallePedido (pedido_id, nombre_producto, cantidad, precio_unitario)
        VALUES (?, ?, ?, ?)
        """, (pedido_id, producto["nombre"], producto["cantidad"], producto["precio"]))
    sql_conn.commit()

    print(f"Pedido {pedido_id} registrado en SQL Server con estado '{estado}'.")

    # Guardar cada producto en Neo4j
    for producto in productos:
        registrar_producto_neo4j(producto)

    # Guardar en MongoDB
    pedido_mongo = {
        "pedido_id": pedido_id,
        "cliente_id": cliente_id,
        "productos": productos,
        "total": total,
        "ciudad": ciudad,
        "fecha_pedido": fecha_pedido,
        "estado": estado,
        "tiempo_entrega": tiempo_entrega
    }
    mongo_pedidos.insert_one(pedido_mongo)
    print(f"Pedido {pedido_id} registrado en MongoDB.")

# Función para registrar un pedido en SQL Server y MongoDB con datos diferenciados
# def registrar_pedido(pedido_id, cliente_id, establecimiento_id, repartidor_id, direccion_id, producto, precio, ciudad, fecha_pedido, tiempo_entrega):
    
    # Guardar en SQL Server con todos los campos
  #  cursor = sql_conn.cursor()
   # cursor.execute("""
   # INSERT INTO Pedidos (cliente_id, establecimiento_id, repartidor_id, direccion_id, total, estado, fecha)
  #  VALUES (?, ?, ?, ?, ?, 'Pendiente', ?)
  #  """, (cliente_id, establecimiento_id, repartidor_id, direccion_id, precio, fecha_pedido))
   # sql_conn.commit()
   # print(f"Pedido {pedido_id} registrado en SQL Server.")
    
    # Guardar en MongoDB con campos limitados
   # pedido_mongo = {
      #  "pedido_id": pedido_id,
      #  "cliente_id": cliente_id,
     #   "producto": producto,  # Guardar la lista completa de productos
     #   "precio": precio,
     #   "ciudad": ciudad,
     #   "fecha_pedido": fecha_pedido,
     #   "tiempo_entrega": tiempo_entrega
    # }
    # mongo_pedidos.insert_one(pedido_mongo)
    # print(f"Pedido {pedido_id} registrado en MongoDB con múltiples productos.")



# Función para registrar un pedido en MongoDB y SQL Server
#def registrar_pedido_mongo_sql(pedido_id, cliente_id, producto, precio, ciudad, fecha_pedido, tiempo_entrega):
    # MongoDB
 #   pedido = {
  #      "pedido_id": pedido_id,
   #     "cliente_id": cliente_id,
    #    "producto": producto,
     #   "precio": precio,
      #  "ciudad": ciudad,
       # "fecha_pedido": fecha_pedido,
       # "tiempo_entrega": tiempo_entrega
   # }
   # mongo_pedidos.insert_one(pedido)
    
    # SQL Server
   # cursor = sql_conn.cursor()
   # cursor.execute("""
   # INSERT INTO Pedidos (PedidoID, ClienteID, Producto, Precio, Ciudad, FechaPedido, TiempoEntrega)
   # VALUES (?, ?, ?, ?, ?, ?, ?)
   # """, (pedido_id, cliente_id, producto, precio, ciudad, fecha_pedido, tiempo_entrega))
   # sql_conn.commit()
   # print(f"Pedido {pedido_id} registrado en MongoDB y SQL Server.")

# Función para registrar restaurante en Neo4j
def registrar_restaurante_neo4j(restaurante_id, nombre_restaurante, ciudad):
    with neo4j_driver.session() as session:
        query = """
        MERGE (r:Restaurante {restaurante_id: $restaurante_id})
        SET r.nombre = $nombre_restaurante, r.ciudad = $ciudad
        """
        session.run(query, restaurante_id=restaurante_id, nombre_restaurante=nombre_restaurante, ciudad=ciudad)
        print(f"Restaurante {nombre_restaurante} registrado en Neo4j.")

# Función para registrar entrega en Neo4j
def registrar_entrega_neo4j(pedido_id, estado_entrega, tiempo_entrega):
    with neo4j_driver.session() as session:
        query = """
        MATCH (p:Pedido {pedido_id: $pedido_id})
        SET p.estado = $estado_entrega, p.tiempo_entrega = $tiempo_entrega
        RETURN p
        """
        session.run(query, pedido_id=pedido_id, estado_entrega=estado_entrega, tiempo_entrega=tiempo_entrega)
        print(f"Entrega de pedido {pedido_id} registrada en Neo4j con estado '{estado_entrega}'.") 

# Funciones para las consultas

# 1. Pedidos diarios por ciudad (MongoDB)
def pedidos_diarios_por_ciudad_mongo():
    result = mongo_pedidos.aggregate([
        {"$group": {"_id": "$ciudad", "totalPedidos": {"$sum": 1}}}
    ])
    for r in result:
        print(f"Ciudad: {r['_id']}, Total Pedidos: {r['totalPedidos']}")

# 2. Productos más solicitados (MongoDB)
def productos_mas_solicitados_mongo():
    result = mongo_pedidos.aggregate([
        {"$group": {"_id": "$producto", "totalPedidos": {"$sum": 1}}},
        {"$sort": {"totalPedidos": -1}},
        {"$limit": 10}
    ])
    for r in result:
        print(f"Producto: {r['_id']}, Total Pedidos: {r['totalPedidos']}")

# 3. Restaurantes más populares (Neo4j)
def restaurantes_populares_neo4j():
    with neo4j_driver.session() as session:
        query = """
        MATCH (r:Restaurante)<-[:REALIZA_PEDIDO_EN]-(p:Pedido)
        RETURN r.nombre AS restaurante, COUNT(p) AS totalPedidos
        ORDER BY totalPedidos DESC LIMIT 10
        """
        result = session.run(query)
        for record in result:
            print(f"Restaurante: {record['restaurante']}, Total Pedidos: {record['totalPedidos']}")

# 4. Categorías populares fin de semana (Neo4j)
def categorias_populares_fin_semana_neo4j():
   with neo4j_driver.session() as session:
        query = """
        MATCH (p:Producto)-[:PERTENECE_A]->(c:Categoria)
        WHERE date().dayOfWeek IN [6, 7]  // 6 = Sábado, 7 = Domingo
        RETURN c.nombre AS categoria, COUNT(p) AS total_productos
        ORDER BY total_productos DESC
        LIMIT 5
        """
        result = session.run(query)
        for record in result:
            print(f"Categoría: {record['categoria']}, Total Pedidos: {record['totalPedidos']}")

# 4. Categorías populares fin de semana (Neo4j)
#def categorias_populares_fin_semana_neo4j():
 #   with neo4j_driver.session() as session:
  #      query = """
   #     MATCH (p:Producto)-[:PERTENECE_A]->(c:Categoria)
    #    WHERE p.fecha_pedido IN ['Sábado', 'Domingo']
     #   RETURN c.nombre AS categoria, COUNT(p) AS totalPedidos
      #  ORDER BY totalPedidos DESC LIMIT 10
      #  """
      #  result = session.run(query)
       # for record in result:
        #    print(f"Categoría: {record['categoria']}, Total Pedidos: {record['totalPedidos']}")

# 5. Pedidos mayores a $50 entregados en menos de 30 minutos (SQL Server)
def pedidos_mayores_50_rapidos_sql():
    cursor = sql_conn.cursor()
    cursor.execute("""
    SELECT PedidoID, Producto, Precio, TiempoEntrega
    FROM Pedidos
    WHERE Precio > 50 AND TiempoEntrega < 30
    """)
    for row in cursor:
        print(f"PedidoID: {row.PedidoID}, Producto: {row.Producto}, Precio: {row.Precio}, TiempoEntrega: {row.TiempoEntrega}")

# 6. Productos solicitados más de 100 veces o con calificación promedio mayor a 4.5 (MongoDB)
def productos_populares_mongo():
    result = mongo_pedidos.aggregate([
        {"$group": {"_id": "$producto", "totalPedidos": {"$sum": 1}, "calificacionPromedio": {"$avg": "$calificacion"}}},
        {"$match": {"$or": [{"totalPedidos": {"$gt": 100}}, {"calificacionPromedio": {"$gt": 4.5}}]}}
    ])
    for r in result:
        print(f"Producto: {r['_id']}, Total Pedidos: {r['totalPedidos']}, Calificación Promedio: {r['calificacionPromedio']}")

# Menú principal
def menu_principal():
    while True:
        print("\n--- Menú Principal ---")
        print("1. Registrar Pedido (MongoDB y SQL Server)")
        print("2. Registrar Restaurante (Neo4j)")
        print("3. Consultar Pedidos Diarios por Ciudad (MongoDB)")
        print("4. Consultar Productos más Solicitados (MongoDB)")
        print("5. Consultar Restaurantes más Populares (Neo4j)")
        print("6. Consultar Categorías Populares Fin de Semana (Neo4j)")
        print("7. Consultar Pedidos > $50 y Entrega en < 30 min (SQL Server)")
        print("8. Consultar Productos Populares (MongoDB)")
        print("9. Salir")
        
        opcion = input("Seleccione una opción: ")
        
        if opcion == "1":
            pedido_id = input("Ingrese ID del pedido: ")
            cliente_id = input("Ingrese ID del cliente: ")
            establecimiento_id = input("Ingrese ID del establecimiento: ")
            repartidor_id = input("Ingrese ID del repartidor: ")
            direccion_id = input("Ingrese ID de la dirección: ")
            ciudad = input("Ingrese ciudad: ")
            estado = input("Ingrese el estado del pedido (Pendiente, En preparación, En camino, Entregado): ")
            fecha_pedido = input("Ingrese fecha del pedido (YYYY-MM-DD): ")
            tiempo_entrega = int(input("Ingrese tiempo de entrega en minutos: "))
            
            # Ingresar múltiples productos
            productos = ingresar_productos()

            # Llama a la función para guardar el pedido en SQL Server y MongoDB
            registrar_pedido(pedido_id, cliente_id, establecimiento_id, repartidor_id, direccion_id, productos, ciudad, fecha_pedido, estado , tiempo_entrega)

     #   if opcion == "1":
      #      pedido_id = input("Ingrese ID del pedido: ")
       #     cliente_id = input("Ingrese ID del cliente: ")
        #    producto = input("Ingrese nombre del producto: ")
         #   precio = float(input("Ingrese precio del producto: "))
          #  ciudad = input("Ingrese ciudad: ")
           # fecha_pedido = input("Ingrese fecha del pedido: ")
           # tiempo_entrega = int(input("Ingrese tiempo de entrega en minutos: "))
           # registrar_pedido_mongo_sql(pedido_id, cliente_id, producto, precio, ciudad, fecha_pedido, tiempo_entrega)

        elif opcion == "2":
            restaurante_id = input("Ingrese ID del restaurante: ")
            nombre_restaurante = input("Ingrese nombre del restaurante: ")
            ciudad = input("Ingrese ciudad: ")
            registrar_restaurante_neo4j(restaurante_id, nombre_restaurante, ciudad)

        elif opcion == "3":
            pedidos_diarios_por_ciudad_mongo()

        elif opcion == "4":
            productos_mas_solicitados_mongo()

        elif opcion == "5":
            restaurantes_populares_neo4j()

        elif opcion == "6":
            categorias_populares_fin_semana_neo4j()

        elif opcion == "7":
            pedidos_mayores_50_rapidos_sql()

        elif opcion == "8":
            productos_populares_mongo()

        elif opcion == "9":
            print("Saliendo del programa.")
            break
        else:
            print("Opción inválida, por favor intente nuevamente.")

# Iniciar la aplicación
if __name__ == "__main__":
    menu_principal()

# Otras formas de las consultas 

# Funciones para las consultas

# 1. ¿Cuántos pedidos se realizan diariamente en diferentes ciudades? (MongoDB)
# def pedidos_diarios_por_ciudad():
  #  db = conectar_mongodb()
   # pipeline = [
    #    {"$group": {"_id": "$ciudad", "totalPedidos": {"$sum": 1}}},
     #   {"$sort": {"totalPedidos": -1}}
   # ]
   # resultados = db.pedidos.aggregate(pipeline)
   # for resultado in resultados:
   #     print(f"Ciudad: {resultado['_id']}, Pedidos diarios: {resultado['totalPedidos']}")

# 2. ¿Cuáles son los restaurantes más populares entre los clientes? (Neo4j)
# def restaurantes_populares_neo4j(driver):
  #  query = """
   # MATCH (r:Restaurante)
   # RETURN r.nombre AS nombre, r.calificacion AS calificacion
   # ORDER BY r.calificacion DESC LIMIT 10
   # """
   # with driver.session() as session:
   #     resultados = session.run(query)
   #     for record in resultados:
   #         print(f"Restaurante: {record['nombre']}, Calificación: {record['calificacion']}")

# 3. ¿Cuántos pedidos han superado los $50 Y han sido entregados en menos de 30 minutos? (SQL Server)
# def pedidos_rapidos_y_costosos_sqlserver():
  #  conn = conectar_sqlserver()
   # cursor = conn.cursor()
   # cursor.execute(
   #     "SELECT COUNT(*) FROM Pedidos WHERE Monto > 50 AND TiempoEntrega < 30"
   # )
   # resultado = cursor.fetchone()
   # print(f"Número de pedidos que superaron $50 y fueron entregados en menos de 30 minutos: {resultado[0]}")

# 4. ¿Qué tipos de productos son más solicitados por los usuarios? (MongoDB)
# def productos_mas_solicitados_mongodb():
  #  db = conectar_mongodb()
  #  pipeline = [
   #     {"$unwind": "$productos"},
    #    {"$group": {"_id": "$productos.nombre", "totalPedidos": {"$sum": 1}}},
     #   {"$sort": {"totalPedidos": -1}}
#    ]
 #   resultados = db.pedidos.aggregate(pipeline)
  #  for resultado in resultados:
   #     print(f"Producto: {resultado['_id']}, Total pedidos: {resultado['totalPedidos']}")

# 5. ¿Qué categorías de productos tienen mayor demanda durante los fines de semana? (Neo4j)
# def categorias_populares_fin_semana_neo4j(driver):
 #   query = """
  #  MATCH (p:Producto)-[:PERTENECE_A]->(c:Categoria)
  #  WHERE p.fecha_pedido IN ['Sábado', 'Domingo']
  #  RETURN c.nombre AS categoria, COUNT(p) AS totalPedidos
  #  ORDER BY totalPedidos DESC LIMIT 10
  #  """
  #  with driver.session() as session:
   #     resultados = session.run(query)
    #    for record in resultados:
     #       print(f"Categoría: {record['categoria']}, Pedidos: {record['totalPedidos']}")

# 6. ¿Qué productos son solicitados más de 100 veces o tienen una calificación promedio superior a 4.5? (MongoDB)
# def productos_populares_mongodb():
 #   db = conectar_mongodb()
  #  pipeline = [
   #     {"$unwind": "$productos"},
    #    {"$group": {"_id": "$productos.nombre", "totalPedidos": {"$sum": 1}, "calificacionPromedio": {"$avg": "$productos.calificacion"}}},
     #   {"$match": {"$or": [{"totalPedidos": {"$gt": 100}}, {"calificacionPromedio": {"$gt": 4.5}}]}},
      #  {"$sort": {"totalPedidos": -1}}
   # ]
   # resultados = db.pedidos.aggregate(pipeline)
   # for resultado in resultados:
   #     print(f"Producto: {resultado['_id']}, Total pedidos: {resultado['totalPedidos']}, Calificación promedio: {resultado['calificacionPromedio']}") 
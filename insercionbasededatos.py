
import pandas as pd
import psycopg2
from datetime import datetime
import calendar
import hashlib
import unicodedata

# Configuración de la base de datos
DB_CONFIG = {
    'host': 'localhost',
    'database': 'gestion',
    'user': 'postgres',
    'password': 'Entrar0212',
    'port': '5432'
}

def conectar_db():
    """Conecta a la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return None

def normalizar_texto(texto):
    """Normaliza texto removiendo tildes y caracteres especiales"""
    if pd.isna(texto) or texto is None:
        return texto
    
    # Convertir a string si no lo es
    texto = str(texto)
    
    # Remover tildes y normalizar
    texto_normalizado = unicodedata.normalize('NFD', texto)
    texto_sin_tildes = ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')
    
    return texto_sin_tildes

def normalizar_columnas(df):
    """Normaliza los nombres de las columnas del DataFrame"""
    columnas_normalizadas = {}
    for col in df.columns:
        col_normalizada = normalizar_texto(col)
        columnas_normalizadas[col] = col_normalizada
    
    # Renombrar columnas
    df_normalizado = df.rename(columns=columnas_normalizadas)
    
    print("=== NORMALIZACIÓN DE COLUMNAS ===")
    for original, normalizada in columnas_normalizadas.items():
        if original != normalizada:
            print(f"'{original}' -> '{normalizada}'")
    
    return df_normalizado

def diagnosticar_excel(archivo_excel):
    """Diagnostica el archivo Excel para ver sus columnas"""
    try:
        df = pd.read_excel(archivo_excel)
        print("=== DIAGNÓSTICO DEL ARCHIVO EXCEL ===")
        print(f"Número de filas: {len(df)}")
        print(f"Número de columnas: {len(df.columns)}")
        print("\nColumnas disponibles (originales):")
        for i, col in enumerate(df.columns):
            print(f"{i+1:2d}. {col}")
        
        # Normalizar columnas
        df_normalizado = normalizar_columnas(df)
        
        print("\nColumnas después de normalización:")
        for i, col in enumerate(df_normalizado.columns):
            print(f"{i+1:2d}. {col}")
        
        print("\nPrimeras 3 filas:")
        print(df_normalizado.head(3))
        
        return df_normalizado
    except Exception as e:
        print(f"Error leyendo el archivo: {e}")
        return None

def crear_tablas(conn):
    """Crea todas las tablas necesarias si no existen"""
    cursor = conn.cursor()
    
    # SQL para crear las tablas
    tablas_sql = [
        """
        CREATE TABLE IF NOT EXISTS tiempo (
            fecha DATE PRIMARY KEY,
            año VARCHAR(255),
            añomes VARCHAR(255),
            añotrimestre VARCHAR(255),
            añodia VARCHAR(255),
            dianum VARCHAR(255),
            dia VARCHAR(255),
            diasemananum VARCHAR(255),
            semana VARCHAR(255),
            mes VARCHAR(255),
            mesnum VARCHAR(255),
            trimestre VARCHAR(255),
            semestre VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ubicacion (
            id_ubicacion INT PRIMARY KEY,
            nombre_region VARCHAR(255),
            codigo_region VARCHAR(255),
            nombre_comuna VARCHAR(255),
            tienda VARCHAR(255),
            zonal VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS categoria (
            id_categoria INT PRIMARY KEY,
            nombre_categoria VARCHAR(255),
            descripcion TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS producto (
            codigo_producto INT PRIMARY KEY,
            nombre_producto VARCHAR(255),
            id_categoria INT,
            linea VARCHAR(255),
            seccion VARCHAR(255),
            negocio VARCHAR(255),
            abastecimiento VARCHAR(255),
            FOREIGN KEY (id_categoria) REFERENCES categoria(id_categoria)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS motivos_detalle (
            id_motivo INT PRIMARY KEY,
            motivo VARCHAR(255),
            ubicacion_motivo VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS mermas (
            id_merma VARCHAR(255) PRIMARY KEY,
            merma_unidad INT,
            merma_monto DECIMAL(10,2),
            id_motivo VARCHAR(255),
            codigo_producto INT,
            fecha DATE,
            id_comuna INT,
            FOREIGN KEY (codigo_producto) REFERENCES producto(codigo_producto),
            FOREIGN KEY (fecha) REFERENCES tiempo(fecha),
            FOREIGN KEY (id_comuna) REFERENCES ubicacion(id_ubicacion)
        )
        """
    ]
    
    # Crear índices
    indices_sql = [
        "CREATE INDEX IF NOT EXISTS idx_mermas_fecha ON mermas(fecha)",
        "CREATE INDEX IF NOT EXISTS idx_mermas_producto ON mermas(codigo_producto)",
        "CREATE INDEX IF NOT EXISTS idx_mermas_ubicacion ON mermas(id_comuna)",
        "CREATE INDEX IF NOT EXISTS idx_producto_categoria ON producto(id_categoria)"
    ]
    
    try:
        print("Creando tablas...")
        
        # Crear tablas
        for tabla_sql in tablas_sql:
            cursor.execute(tabla_sql)
            tabla_nombre = tabla_sql.split('TABLE IF NOT EXISTS')[1].split('(')[0].strip()
            print(f"Tabla creada/verificada: {tabla_nombre}")
        
        # Crear índices
        print("Creando índices...")
        for indice_sql in indices_sql:
            cursor.execute(indice_sql)
        
        conn.commit()
        print("Todas las tablas e índices creados exitosamente!")
        
    except Exception as e:
        print(f"Error creando tablas: {e}")
        conn.rollback()
        raise e
    
    finally:
        cursor.close()

def calcular_datos_tiempo(fecha):
    """Calcula todos los campos de tiempo basado en una fecha"""
    if pd.isna(fecha):
        return None
    
    # Convertir a datetime si es string
    if isinstance(fecha, str):
        try:
            fecha = pd.to_datetime(fecha)
        except:
            return None
    
    año = fecha.year
    mes = fecha.month
    dia = fecha.day
    
    # Calcular trimestre
    trimestre = (mes - 1) // 3 + 1
    
    # Calcular semestre
    semestre = 1 if mes <= 6 else 2
    
    # Calcular semana del año
    semana = fecha.isocalendar()[1]
    
    # Día de la semana (1=Lunes, 7=Domingo)
    dia_semana = fecha.isoweekday()
    
    return {
        'fecha': fecha.date(),
        'año': str(año),
        'añomes': f"{año}-{mes:02d}",
        'añotrimestre': f"{año}-Q{trimestre}",
        'añodia': f"{año}-{fecha.dayofyear:03d}",
        'dianum': str(dia),
        'dia': fecha.strftime('%A'),
        'diasemananum': str(dia_semana),
        'semana': str(semana),
        'mes': fecha.strftime('%B'),
        'mesnum': str(mes),
        'trimestre': f"Q{trimestre}",
        'semestre': f"S{semestre}"
    }

def insertar_tiempo(conn, fecha_data):
    """Inserta datos en la tabla Tiempo"""
    if not fecha_data:
        return
    
    cursor = conn.cursor()
    
    try:
        # Verificar si ya existe
        cursor.execute("SELECT fecha FROM tiempo WHERE fecha = %s", (fecha_data['fecha'],))
        if cursor.fetchone():
            return
        
        # Insertar
        query = """
        INSERT INTO tiempo (fecha, año, añomes, añotrimestre, añodia, dianum, dia, 
                           diasemananum, semana, mes, mesnum, trimestre, semestre)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        valores = (
            fecha_data['fecha'], fecha_data['año'], fecha_data['añomes'],
            fecha_data['añotrimestre'], fecha_data['añodia'], fecha_data['dianum'],
            fecha_data['dia'], fecha_data['diasemananum'], fecha_data['semana'],
            fecha_data['mes'], fecha_data['mesnum'], fecha_data['trimestre'],
            fecha_data['semestre']
        )
        
        cursor.execute(query, valores)
    except Exception as e:
        print(f"Error insertando fecha {fecha_data['fecha']}: {e}")
    finally:
        cursor.close()

def insertar_ubicacion(conn, ubicaciones_df):
    """Inserta datos únicos en la tabla Ubicacion"""
    cursor = conn.cursor()
    
    # Verificar que las columnas existan
    columnas_requeridas = ['region', 'comuna', 'tienda', 'zonal']
    columnas_faltantes = [col for col in columnas_requeridas if col not in ubicaciones_df.columns]
    
    if columnas_faltantes:
        print(f"Advertencia: Columnas faltantes en ubicaciones: {columnas_faltantes}")
        cursor.close()
        return
    
    # Obtener ubicaciones únicas
    ubicaciones_unicas = ubicaciones_df[columnas_requeridas].drop_duplicates()
    
    for _, row in ubicaciones_unicas.iterrows():
        try:
            # Verificar que no haya valores nulos
            if any(pd.isna(val) for val in row):
                continue
                
            # Generar ID único (hash de los datos)
            id_ubicacion = abs(hash(f"{row['region']}-{row['comuna']}-{row['tienda']}-{row['zonal']}")) % 1000000
            
            # Verificar si ya existe
            cursor.execute("SELECT id_ubicacion FROM ubicacion WHERE id_ubicacion = %s", (id_ubicacion,))
            if cursor.fetchone():
                continue
            
            # Insertar
            query = """
            INSERT INTO ubicacion (id_ubicacion, nombre_region, codigo_region, nombre_comuna, tienda, zonal)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            valores = (
                id_ubicacion,
                str(row['region']),
                str(row['region'])[:3].upper(),  # Código de región (primeras 3 letras)
                str(row['comuna']),
                str(row['tienda']),
                str(row['zonal'])
            )
            
            cursor.execute(query, valores)
        except Exception as e:
            print(f"Error insertando ubicación: {e}")
            continue
    
    cursor.close()

def insertar_categoria(conn, categorias_df):
    """Inserta datos únicos en la tabla Categoria"""
    cursor = conn.cursor()
    
    # Verificar que la columna exista
    if 'categoria' not in categorias_df.columns:
        print("Advertencia: Columna 'categoria' no encontrada")
        cursor.close()
        return
    
    # Obtener categorías únicas
    categorias_unicas = categorias_df['categoria'].dropna().unique()
    
    for i, categoria in enumerate(categorias_unicas, 1):
        try:
            # Verificar si ya existe
            cursor.execute("SELECT id_categoria FROM categoria WHERE nombre_categoria = %s", (str(categoria),))
            if cursor.fetchone():
                continue
            
            # Insertar
            query = """
            INSERT INTO categoria (id_categoria, nombre_categoria, descripcion)
            VALUES (%s, %s, %s)
            """
            
            cursor.execute(query, (i, str(categoria), f"Categoría {categoria}"))
        except Exception as e:
            print(f"Error insertando categoría {categoria}: {e}")
            continue
    
    cursor.close()

def insertar_producto(conn, productos_df):
    """Inserta datos únicos en la tabla Producto"""
    cursor = conn.cursor()
    
    # Verificar que las columnas existan
    columnas_requeridas = ['codigo_producto', 'descripcion', 'categoria', 'linea', 'seccion', 'negocio', 'abastecimiento']
    columnas_faltantes = [col for col in columnas_requeridas if col not in productos_df.columns]
    
    if columnas_faltantes:
        print(f"Advertencia: Columnas faltantes en productos: {columnas_faltantes}")
        cursor.close()
        return
    
    # Obtener productos únicos
    productos_unicos = productos_df[columnas_requeridas].drop_duplicates()
    
    for _, row in productos_unicos.iterrows():
        try:
            # Verificar que no haya valores nulos críticos
            if pd.isna(row['codigo_producto']) or pd.isna(row['descripcion']):
                continue
                
            # Verificar si ya existe
            cursor.execute("SELECT codigo_producto FROM producto WHERE codigo_producto = %s", (int(row['codigo_producto']),))
            if cursor.fetchone():
                continue
            
            # Obtener ID de categoría
            cursor.execute("SELECT id_categoria FROM categoria WHERE nombre_categoria = %s", (str(row['categoria']) if pd.notna(row['categoria']) else '',))
            categoria_result = cursor.fetchone()
            id_categoria = categoria_result[0] if categoria_result else None
            
            # Insertar
            query = """
            INSERT INTO producto (codigo_producto, nombre_producto, id_categoria, linea, seccion, negocio, abastecimiento)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            valores = (
                int(row['codigo_producto']),
                str(row['descripcion']),
                id_categoria,
                str(row['linea']) if pd.notna(row['linea']) else '',
                str(row['seccion']) if pd.notna(row['seccion']) else '',
                str(row['negocio']) if pd.notna(row['negocio']) else '',
                str(row['abastecimiento']) if pd.notna(row['abastecimiento']) else ''
            )
            
            cursor.execute(query, valores)
        except Exception as e:
            print(f"Error insertando producto {row['codigo_producto']}: {e}")
            continue
    
    cursor.close()

def insertar_motivo_detalle(conn, motivos_df):
    """Inserta datos únicos en la tabla Motivos_Detalle"""
    cursor = conn.cursor()
    
    # Verificar que las columnas existan
    columnas_requeridas = ['motivo', 'ubicacion_motivo']
    columnas_faltantes = [col for col in columnas_requeridas if col not in motivos_df.columns]
    
    if columnas_faltantes:
        print(f"Advertencia: Columnas faltantes en motivos: {columnas_faltantes}")
        cursor.close()
        return
    
    # Obtener motivos únicos
    motivos_unicos = motivos_df[columnas_requeridas].dropna().drop_duplicates()
    
    for i, (_, row) in enumerate(motivos_unicos.iterrows(), 1):
        try:
            # Verificar si ya existe
            cursor.execute("SELECT id_motivo FROM motivos_detalle WHERE motivo = %s AND ubicacion_motivo = %s", 
                          (str(row['motivo']), str(row['ubicacion_motivo'])))
            if cursor.fetchone():
                continue
            
            # Insertar
            query = """
            INSERT INTO motivos_detalle (id_motivo, motivo, ubicacion_motivo)
            VALUES (%s, %s, %s)
            """
            
            cursor.execute(query, (i, str(row['motivo']), str(row['ubicacion_motivo'])))
        except Exception as e:
            print(f"Error insertando motivo {row['motivo']}: {e}")
            continue
    
    cursor.close()

def insertar_mermas(conn, df):
    """Inserta datos en la tabla Mermas"""
    cursor = conn.cursor()
    
    # Verificar que las columnas existan
    columnas_requeridas = ['codigo_producto', 'fecha', 'motivo', 'ubicacion_motivo', 'region', 'comuna', 'tienda', 'zonal']
    columnas_opcionales = ['merma_unidad_p', 'merma_monto_p']
    
    columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
    
    if columnas_faltantes:
        print(f"Error: Columnas requeridas faltantes: {columnas_faltantes}")
        cursor.close()
        return
    
    # Verificar columnas opcionales
    for col in columnas_opcionales:
        if col not in df.columns:
            print(f"Advertencia: Columna opcional '{col}' no encontrada, se usará 0 como valor por defecto")
    
    filas_procesadas = 0
    filas_saltadas = 0
    
    for _, row in df.iterrows():
        try:
            # Verificar que los valores críticos no sean nulos
            if (pd.isna(row['codigo_producto']) or pd.isna(row['fecha']) or 
                pd.isna(row['motivo']) or pd.isna(row['ubicacion_motivo'])):
                filas_saltadas += 1
                continue
            
            # Generar ID único para merma
            id_merma = f"{int(row['codigo_producto'])}-{row['fecha']}-{abs(hash(str(row['motivo']) + str(row['ubicacion_motivo']))) % 10000}"
            
            # Verificar si ya existe
            cursor.execute("SELECT id_merma FROM mermas WHERE id_merma = %s", (id_merma,))
            if cursor.fetchone():
                continue
            
            # Obtener ID de motivo
            cursor.execute("SELECT id_motivo FROM motivos_detalle WHERE motivo = %s AND ubicacion_motivo = %s", 
                          (str(row['motivo']), str(row['ubicacion_motivo'])))
            motivo_result = cursor.fetchone()
            id_motivo = str(motivo_result[0]) if motivo_result else None
            
            # Obtener ID de ubicación
            cursor.execute("""
            SELECT id_ubicacion FROM ubicacion 
            WHERE nombre_region = %s AND nombre_comuna = %s AND tienda = %s AND zonal = %s
            """, (str(row['region']), str(row['comuna']), str(row['tienda']), str(row['zonal'])))
            ubicacion_result = cursor.fetchone()
            id_comuna = ubicacion_result[0] if ubicacion_result else None
            
            # Obtener valores de merma (con valores por defecto si no existen)
            merma_unidad = 0
            merma_monto = 0.0
            
            if 'merma_unidad_p' in df.columns and pd.notna(row['merma_unidad_p']):
                merma_unidad = int(row['merma_unidad_p'])
            
            if 'merma_monto_p' in df.columns and pd.notna(row['merma_monto_p']):
                merma_monto = float(row['merma_monto_p'])
            
            # Insertar
            query = """
            INSERT INTO mermas (id_merma, merma_unidad, merma_monto, id_motivo, codigo_producto, fecha, id_comuna)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            valores = (
                id_merma,
                merma_unidad,
                merma_monto,
                id_motivo,
                int(row['codigo_producto']),
                pd.to_datetime(row['fecha']).date(),
                id_comuna
            )
            
            cursor.execute(query, valores)
            filas_procesadas += 1
            
        except Exception as e:
            print(f"Error insertando merma en fila {filas_procesadas + filas_saltadas}: {e}")
            filas_saltadas += 1
            continue
    
    print(f"Mermas procesadas: {filas_procesadas}, Filas saltadas: {filas_saltadas}")
    cursor.close()

def procesar_excel(archivo_excel):
    """Función principal para procesar el archivo Excel"""
    try:
        # Leer Excel
        print("Leyendo archivo Excel...")
        df_original = pd.read_excel(archivo_excel)
        
        # Normalizar nombres de columnas (quitar tildes)
        df = normalizar_columnas(df_original)
        
        print(f"Archivo leído exitosamente: {len(df)} filas, {len(df.columns)} columnas")
        
        # Conectar a la base de datos
        print("Conectando a la base de datos...")
        conn = conectar_db()
        if not conn:
            return
        
        # Crear tablas si no existen
        crear_tablas(conn)
        
        # Procesar datos de tiempo
        print("Procesando datos de tiempo...")
        if 'fecha' in df.columns:
            fechas_unicas = df['fecha'].dropna().unique()
            for fecha in fechas_unicas:
                fecha_data = calcular_datos_tiempo(fecha)
                insertar_tiempo(conn, fecha_data)
        else:
            print("Advertencia: Columna 'fecha' no encontrada")
        
        # Insertar ubicaciones
        print("Insertando ubicaciones...")
        insertar_ubicacion(conn, df)
        
        # Insertar categorías
        print("Insertando categorías...")
        insertar_categoria(conn, df)
        
        # Insertar productos
        print("Insertando productos...")
        insertar_producto(conn, df)
        
        # Insertar motivos
        print("Insertando motivos...")
        insertar_motivo_detalle(conn, df)
        
        # Insertar mermas
        print("Insertando mermas...")
        insertar_mermas(conn, df)
        
        # Confirmar cambios
        conn.commit()
        print("¡Datos insertados exitosamente!")
        
    except Exception as e:
        print(f"Error procesando el archivo: {e}")
        if conn:
            conn.rollback()
    
    finally:
        if conn:
            conn.close()

# Función para solo diagnosticar sin procesar
def solo_diagnosticar(archivo_excel):
    """Solo diagnostica el archivo sin procesar los datos"""
    return diagnosticar_excel(archivo_excel)

# Uso del script
if __name__ == "__main__":
    # Cambiar por la ruta de tu archivo Excel
    archivo_excel = "mermas.xlsx"
    
    # Opción 1: Solo diagnosticar (recomendado primero)
    print("=== MODO DIAGNÓSTICO ===")
    df_diagnostico = solo_diagnosticar(archivo_excel)
    
    if df_diagnostico is not None:
        respuesta = input("\n¿Deseas proceder con la carga de datos? (s/n): ")
        if respuesta.lower() in ['s', 'si', 'yes', 'y']:
            print("\n=== PROCESANDO DATOS ===")
            procesar_excel(archivo_excel)
        else:
            print("Carga cancelada por el usuario.")
    else:
        print("No se pudo leer el archivo Excel.")
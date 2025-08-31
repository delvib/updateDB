import pandas as pd
import sqlite3
import tkinter as tk
from tkinter import filedialog, messagebox

# --- LÓGICA DE LIMPIEZA Y ESTANDARIZACIÓN ---

def limpiar_y_estandarizar_datos(df):
    """
    Procesa un DataFrame para estandarizar los nombres de las columnas
    y asegurar que coincidan con la tabla de destino `EntidadesExternas`.
    
    Esta función es crucial para el proceso de consolidación de datos.
    Detecta si el archivo de origen es de 'Donantes' o de 'Proveedores'
    basándose en la presencia de columnas específicas. Una vez identificado,
    mapea los nombres de las columnas del DataFrame de entrada a los nombres
    de las columnas de la tabla final.
    
    Para las columnas que no existen en el archivo de origen (por ejemplo,
    columnas de proveedores en un archivo de donantes), la función las agrega
    al DataFrame y les asigna un valor nulo (`None`) para evitar errores
    durante la inserción en la base de datos.

    Args:
        df (pandas.DataFrame): El DataFrame de datos que se va a procesar.
                               Debe ser leído de un archivo CSV o Excel.

    Returns:
        pandas.DataFrame: Un nuevo DataFrame con las columnas estandarizadas
                          y en el orden correcto, listo para ser insertado
                          en la base de datos.

    Raises:
        ValueError: Si el DataFrame de entrada no contiene las columnas clave
                    para ser identificado como un archivo de Donantes o Proveedores.
    """
    columnas_consolidadas = [
        'numero_identificador', 'nombre_entidad', 'razon_social', 'cuit',
        'tipo', 'condicion_iva', 'contacto', 'email', 'telefono', 'observaciones',
        'fecha', 'baja', 'activo', 'categoria_entidad', 'importe', 'nro_cuenta',
        'ciudad', 'pais', 'tipo_cuenta', 'importe_resultados', 'tipo_contribuyente',
        'frecuencia', 'f_donacion', 'fecha_a', 'fecha_b', 'detalle_ing', 'detalle_egreso',
        'cargo'
    ]

    # Verifica si es un archivo de Donantes
    if 'Número Proveedor' in df.columns:
        print("Detectado: Archivo de Donantes.")
        df.rename(columns={
            'Número Proveedor': 'numero_identificador',
            'Nombre Proveedor': 'nombre_entidad',
            'Razón Social': 'razon_social',
            'CUIT': 'cuit',
            'tipo': 'tipo',
            'Contacto': 'contacto',
            'Correo Electrónico': 'email',
            'Teléfono': 'telefono',
            'Observaciones': 'observaciones',
            'Fecha': 'fecha',
            'baja': 'baja',
            'activo': 'activo',
            'Categor/a Proveedor': 'categoria_entidad',
            'Importe': 'importe',
            'Nro_Cuenta': 'nro_cuenta',
            'Ciudad': 'ciudad',
            'Pais': 'pais',
            'tipoCta': 'tipo_cuenta',
            'importeResultados': 'importe_resultados',
            'Tipo de Contribuyente': 'tipo_contribuyente',
            'Frecuencia': 'frecuencia',
            'f_donacion': 'f_donacion',
            'fecha_a': 'fecha_a',
            'fecha_b': 'fecha_b',
            'detalle_ing': 'detalle_ing',
            'Cargo': 'cargo'
        }, inplace=True)
        
        # Agrega las columnas que solo existen en Proveedores y asigna NULL
        for col in ['condicion_iva', 'detalle_egreso']:
            if col not in df.columns:
                df[col] = None

    # Verifica si es un archivo de Proveedores
    elif 'CodProv' in df.columns:
        print("Detectado: Archivo de Proveedores.")
        df.rename(columns={
            'CodProv': 'numero_identificador',
            'Nombre': 'nombre_entidad',
            'Cuit': 'cuit',
            'Tipo': 'tipo',
            'CondIVA': 'condicion_iva',
            'contacto': 'contacto',
            'email': 'email',
            'tel': 'telefono',
            'obs': 'observaciones',
            'fecha': 'fecha',
            'categoria': 'categoria_entidad',
            'importe': 'importe',
            'Ncuenta': 'nro_cuenta',
            'ciudad': 'ciudad',
            'pais': 'pais',
            'TipoCta': 'tipo_cuenta',
            'importeResultados': 'importe_resultados',
            'DetalleEgreso': 'detalle_egreso',
        }, inplace=True)
        
        # Agrega las columnas que solo existen en Donantes y asigna NULL
        for col in ['razon_social', 'baja', 'activo', 'tipo_contribuyente', 'frecuencia', 'f_donacion', 'fecha_a', 'fecha_b', 'detalle_ing', 'cargo']:
            if col not in df.columns:
                df[col] = None
    
    else:
        raise ValueError("Error: El archivo no es un formato de Donantes ni de Proveedores conocido.")

    # Reordena las columnas para que coincidan con la tabla EntidadesExternas
    return df[columnas_consolidadas]

def actualizar_base_de_datos(df, db_path):
    """
    Sincroniza un DataFrame de pandas con una tabla de base de datos SQLite.
    
    Esta función implementa un proceso de "upsert" (UPDATE + INSERT). Si un
    registro en el DataFrame ya existe en la base de datos (basándose en el
    'numero_identificador'), lo actualiza con los nuevos datos. Si el registro
    es nuevo, lo inserta en la tabla.

    Args:
        df (pandas.DataFrame): El DataFrame estandarizado que se va a insertar.
        db_path (str): La ruta al archivo de la base de datos SQLite.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Crea una tabla temporal con los nuevos datos.
    # Esto nos permite comparar los nuevos registros con los existentes.
    df.to_sql('temp_nuevos_datos', conn, if_exists='replace', index=False)

    # Prepara la lista de columnas para la actualización, excluyendo el ID.
    columnas = [col for col in df.columns if col != 'numero_identificador']
    set_statements = ", ".join([f"{col} = (SELECT {col} FROM temp_nuevos_datos WHERE temp_nuevos_datos.numero_identificador = EntidadesExternas.numero_identificador)" for col in columnas])

    # Ejecuta la operación de UPDATE.
    # Actualiza los registros existentes en EntidadesExternas.
    update_query = f"""
        UPDATE EntidadesExternas
        SET {set_statements}
        WHERE numero_identificador IN (SELECT numero_identificador FROM temp_nuevos_datos);
    """
    cursor.execute(update_query)

    # Ejecuta la operación de INSERT.
    # Inserta los registros nuevos en EntidadesExternas.
    insert_query = f"""
        INSERT INTO EntidadesExternas ({', '.join(df.columns)})
        SELECT {', '.join(df.columns)}
        FROM temp_nuevos_datos
        WHERE numero_identificador NOT IN (SELECT numero_identificador FROM EntidadesExternas);
    """
    cursor.execute(insert_query)

    conn.commit()
    conn.close()

# --- LÓGICA DE LA INTERFAZ GRÁFICA (tkinter) ---

class App(tk.Tk):
    """
    Clase principal que gestiona la interfaz gráfica y la lógica de la aplicación.
    
    Hereda de tk.Tk y se encarga de:
    - Crear la ventana de la aplicación.
    - Configurar los botones y etiquetas.
    - Manejar la selección de archivos.
    - Llamar a las funciones de limpieza y actualización de datos.
    - Mostrar mensajes de estado y errores al usuario.
    """
    def __init__(self):
        # Llama al constructor de la clase padre
        super().__init__()
        
        # Configura la ventana principal
        self.title("Actualizador de Base de Datos")
        self.geometry("500x200")

        # Variables de instancia
        self.db_path = 'fundacion.db'
        self.archivo_a_actualizar = None

        # Crea los widgets de la interfaz
        self.label_path = tk.Label(self, text="Selecciona un archivo CSV o Excel para actualizar:", wraplength=450)
        self.label_path.pack(pady=10)

        self.btn_select = tk.Button(self, text="Seleccionar archivo", command=self.select_file)
        self.btn_select.pack(pady=5)

        self.btn_update = tk.Button(self, text="Actualizar base de datos", command=self.update_db, state=tk.DISABLED)
        self.btn_update.pack(pady=10)

        self.status_label = tk.Label(self, text="", fg="blue")
        self.status_label.pack(pady=5)

    def select_file(self):
        """Abre un cuadro de diálogo para que el usuario seleccione un archivo."""
        file_path = filedialog.askopenfilename(
            filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx *.xls")]
        )
        if file_path:
            self.archivo_a_actualizar = file_path
            self.label_path.config(text=f"Archivo seleccionado: {file_path}")
            self.btn_update.config(state=tk.NORMAL)
            self.status_label.config(text="")

    def update_db(self):
        """
        Método que se ejecuta al hacer clic en el botón de "Actualizar base de datos".
        Maneja la lógica de lectura, limpieza y actualización de datos, y muestra
        mensajes de éxito o error al usuario.
        """
        # Verifica si un archivo ha sido seleccionado
        if not self.archivo_a_actualizar:
            messagebox.showerror("Error", "Por favor, selecciona un archivo primero.")
            return

        try:
            # Muestra un mensaje de "Procesando" y actualiza la interfaz
            self.status_label.config(text="Procesando...", fg="orange")
            self.update_idletasks()
            
            # Lee el archivo de origen (CSV o Excel)
            if self.archivo_a_actualizar.endswith('.csv'):
                df_original = pd.read_csv(self.archivo_a_actualizar, encoding='latin1')
            else:
                df_original = pd.read_excel(self.archivo_a_actualizar)

            # Llama a las funciones de limpieza y actualización
            df_limpio = limpiar_y_estandarizar_datos(df_original)
            actualizar_base_de_datos(df_limpio, self.db_path)
            
            # Muestra un mensaje de éxito
            self.status_label.config(text="¡Actualización completada con éxito!", fg="green")
            messagebox.showinfo("Éxito", "La base de datos se ha actualizado correctamente.")
        except Exception as e:
            # Muestra un mensaje de error si algo falla
            self.status_label.config(text="Ocurrió un error.", fg="red")
            messagebox.showerror("Error", f"Ocurrió un error durante la actualización: {e}")

# --- PUNTO DE ENTRADA PRINCIPAL ---

if __name__ == "__main__":
    """
    Este es el punto de entrada principal del programa.
    Verifica que el script se está ejecutando directamente y no se
    está importando como un módulo. Crea la instancia de la aplicación
    y la ejecuta.
    """
    app = App()
    app.mainloop()

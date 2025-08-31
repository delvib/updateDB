import pandas as pd
import sqlite3
import tkinter as tk
from tkinter import filedialog, messagebox

# --- LÓGICA DE LIMPIEZA Y ACTUALIZACIÓN ---

def limpiar_y_estandarizar_datos(df):
    """
    Detecta si el archivo es de donantes o proveedores y renombra las columnas
    para que coincidan con la tabla EntidadesExternas.
    """
    # Lista de todas las columnas en la tabla EntidadesExternas
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
    # ... (El resto de la función es el mismo) ...
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    df.to_sql('temp_nuevos_datos', conn, if_exists='replace', index=False)

    columnas = [col for col in df.columns if col != 'numero_identificador']
    set_statements = ", ".join([f"{col} = (SELECT {col} FROM temp_nuevos_datos WHERE temp_nuevos_datos.numero_identificador = EntidadesExternas.numero_identificador)" for col in columnas])

    update_query = f"""
        UPDATE EntidadesExternas
        SET {set_statements}
        WHERE numero_identificador IN (SELECT numero_identificador FROM temp_nuevos_datos);
    """
    cursor.execute(update_query)

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
    def __init__(self):
        super().__init__()
        self.title("Actualizador de Base de Datos")
        self.geometry("500x200")

        self.db_path = 'fundacion.db'
        self.archivo_a_actualizar = None

        self.label_path = tk.Label(self, text="Selecciona un archivo CSV o Excel para actualizar:", wraplength=450)
        self.label_path.pack(pady=10)

        self.btn_select = tk.Button(self, text="Seleccionar archivo", command=self.select_file)
        self.btn_select.pack(pady=5)

        self.btn_update = tk.Button(self, text="Actualizar base de datos", command=self.update_db, state=tk.DISABLED)
        self.btn_update.pack(pady=10)

        self.status_label = tk.Label(self, text="", fg="blue")
        self.status_label.pack(pady=5)

    def select_file(self):
        file_path = filedialog.askopenfilename(
            filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx *.xls")]
        )
        if file_path:
            self.archivo_a_actualizar = file_path
            self.label_path.config(text=f"Archivo seleccionado: {file_path}")
            self.btn_update.config(state=tk.NORMAL)
            self.status_label.config(text="")

    def update_db(self):
        if not self.archivo_a_actualizar:
            messagebox.showerror("Error", "Por favor, selecciona un archivo primero.")
            return

        try:
            self.status_label.config(text="Procesando...", fg="orange")
            self.update_idletasks()
            
            if self.archivo_a_actualizar.endswith('.csv'):
                df_original = pd.read_csv(self.archivo_a_actualizar, encoding='latin1')
            else:
                df_original = pd.read_excel(self.archivo_a_actualizar)

            df_limpio = limpiar_y_estandarizar_datos(df_original)
            actualizar_base_de_datos(df_limpio, self.db_path)
            
            self.status_label.config(text="¡Actualización completada con éxito!", fg="green")
            messagebox.showinfo("Éxito", "La base de datos se ha actualizado correctamente.")
        except Exception as e:
            self.status_label.config(text="Ocurrió un error.", fg="red")
            messagebox.showerror("Error", f"Ocurrió un error durante la actualización: {e}")

if __name__ == "__main__":
    app = App()
    app.mainloop()
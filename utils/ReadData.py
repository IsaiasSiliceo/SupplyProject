import os
from pyspark.sql import SparkSession
import pandas as pd

class ReadData:

    def __init__(self,data_path):
        # Ruta de acceso a la carpeta data
        self.data_path = data_path
        

    # Función para crear sesión de pyspark y leer 00_Datos_Modelar.txt
    def read_data(self):
        # Inicializar una sesión de Spark
        spark = SparkSession.builder.appName("Datos Modelar").getOrCreate()
        data_path_joined = os.path.join(self.data_path, "00_Datos_Modelar.txt")

        # Lectura de los datos a data
        data = spark.read.option("delimiter", "|").option("header", True).option("inferSchema", True).csv(data_path_joined)


        return data
    
    # Función para leer los datos de CatLoc
    def read_CatLoc(self):
        CatLoc = pd.read_excel(self.data_path+'CIMAT_BaseDatos.xlsx',sheet_name="CatLoc")
        return CatLoc
    
    # Función para leer los datos de Inventario
    def read_Inventario(self):
        Inventario = pd.read_excel(self.data_path+'CIMAT_BaseDatos.xlsx',sheet_name="Inventario")
        return Inventario
    
    # Función para leer los datos de CatSku
    def read_CatSku(self):
        CatSku = pd.read_excel(self.data_path+'CIMAT_BaseDatos.xlsx',sheet_name="CatSku")
        return CatSku
    

if __name__ == "__main__":
    print('You read succesfully!')
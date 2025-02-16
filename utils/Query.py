from pyspark.sql.functions import col, sum
import pandas as pd

class Query:

    def __init__(self,data,CatLoc):
        # Esta función recibe los datos y el dataframe con regiones/plazas CatLoc
        self.data = data
        self.CatLoc = CatLoc

    # Función para agrupar los datos por region
    def groupby_region(self,region,export=False,path='/home/chay/Abasto_CIMAT/data/'):
        # Se seleccionan todas las tiendas de la región en una lista
        Tiendas = self.CatLoc[self.CatLoc['REGION'] == region]['LOC'].tolist()
        
        # Consulta y agrupamiento por tipo de producto y operación de suma sobre las unidades
        SeriesByRegion = self.data.filter(col('Loc').isin(Tiendas))
        SeriesByRegion = SeriesByRegion.groupBy('Sku','Fecha').agg(sum('Uni'))

        if export:
            SeriesByRegion.write.option('header','true').csv(path+'SeriesByRegion'+str(region)+'.csv')
        
        return SeriesByRegion
    

    # Función para agrupar los datos por plaza
    def groupby_plaza(self,region,plaza,export=False,path='/home/chay/Abasto_CIMAT/data/'):
        # Se seleccionan todas las tiendas de la región en una lista
        Tiendas = self.CatLoc[self.CatLoc['REGION']==region]
        Locs = Tiendas[Tiendas['PLAZA'] == plaza]['LOC'].to_list()

        # Consulta y agrupamiento por tipo de producto y operación de suma sobre las unidades
        SeriesByPlaza= self.data.filter(col('Loc').isin(Locs))
        SeriesByPlaza = SeriesByPlaza.groupBy('Sku','Fecha').agg(sum('Uni'))

        if export:
            SeriesByPlaza.write.option('header','true').csv(path+'SeriesByPlaza'+plaza+'.csv')
        
        return SeriesByPlaza


if __name__ == "__main__":
    print('You read succesfully!')
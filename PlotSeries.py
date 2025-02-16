import plotly
import plotly.graph_objs as go
from IPython.display import display, HTML
import pandas as pd

class PlotSeries:

    def __init__(self,data):
        
        self.data = data.toPandas()
        self.data['Fecha'] = pd.to_datetime(self.data["Fecha"])
        self.data = self.data.sort_values(by=["Fecha","Sku"])
        
        plotly.offline.init_notebook_mode()
        display(HTML(
            '<script type="text/javascript" async src="https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.1/MathJax.js?config=TeX-MML-AM_SVG"></script>'
            ))
        
    # Función para graficar las series
    def plot_series(self,region,plaza='',format='png',path='/home/chay/Abasto_CIMAT/plots/'):
        # Crear figura
        fig = go.Figure()

        # Agregar trazas para cada Sku
        for sku in self.data["Sku"].unique():
            subset = self.data[self.data["Sku"] == sku]
            fig.add_trace(go.Scatter(
                x=subset["Fecha"], 
                y=subset["sum(Uni)"], 
                mode='lines', 
                name=f'{sku}'
            ))

        # Editar el diseño
        fig.update_layout(
            template='ggplot2',
            title=dict(text='Ventas por Producto Región: 1'),
            xaxis=dict(title=dict(text='Fecha')),
            yaxis=dict(title=dict(text='Total de Unidades')),
            xaxis_tickformat="%Y-%m-%d",  # Formato de fecha
        )

        if plaza=='':
            name = '' + plaza


        if format == 'png':
            fig.write_image(path+"Reg"+str(region)+'_Plaza'+plaza+".png",width=1200, height=700,scale=1)
        else:
            fig.write_html(path+"Reg"+str(region)+'_Plaza'+plaza+".html")


if __name__ == "__main__":
    print('You read succesfully!')
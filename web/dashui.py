import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import pandas as pd
import psycopg2
#import dash_table_experiments as dt


dataframe = pd.DataFrame()

def get_unique_cities():
    query = """
    select distinct city from distance
    """
    return fetch(query)
    
def get_interests_from_city(city):
    query = """
    select distinct uc.name
    from uniquecategories uc,yelpcategory yc, yelpbusiness yb
    where uc.id = yc.category_id and yc.yelp_id = yb.business_id
    and yb.city = '{}'
    """.format(city)
    return fetch(query)

def get_results(city,interest):
    query = """
    select distinct b.listing_id,b.name as bnbname,b.price as bnbprice,d.distance as distance
    from uniquecategories uc, yelpcategory yc, yelpbusiness yb, distance d, bnbsum b
    where b.listing_id = d.listing_id and yb.business_id = d.business_id and 
    yb.business_id = yc.yelp_id and yc.category_id = uc.id
    and b.city = '{}' and uc.name='{}' order by d.distance desc limit 5
        """.format(city,interest)
    return fetch(query)

def fetch(query):
    connection = psycopg2.connect(database='tripperdump', user='tripper',password='tripper', host='localhost', port = 5431)
    df = pd.read_sql(query,connection)
    connection.close()
    return df

def build_table(dataframe):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe)))]
    )

dfcity = get_unique_cities()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(
                children=[
                        
                        
                    html.Div(
                    [
                    html.Label('Select City'),
                    dcc.Dropdown(
                            id = 'city_dropdown',
                            options = [{"label": l, "value": l} for l in dfcity.city],
                            placeholder = "Select a city")
                    ])
                    ,
                    
                    
                    html.Div(
                    [
                    html.Label('Select Interest'),
                    dcc.Dropdown(
                            id = 'interest_dropdown',
                            placeholder = "Select an Interest"
                            )
                    ]),
                    
                    
                    html.Div(
                    
                    html.H4('The Best Places you can stay at are : '),
                    id = 'result_table'
                    build_table(dataframe)
                    )
    ])

#df = get_results(city,interest)    



@app.callback(
    Output("interest_dropdown","options"),
    [Input("city_dropdown",'value')])
def interest_dropdown(city):
    dfinterests = get_interests_from_city(city)
    return[{"label": l, "value": l} for l in dfinterests.name]
             
@app.callback(
     Output("result_table","figure"),
     [Input("city_dropdown","value"),Input("interest_dropdown","value")])
def generate_table(city,interest):
    dataframe = get_results(city,interest)
    #build_table(dataframe)

if __name__ == '__main__':
    app.run_server(debug=True,host='0.0.0.0')
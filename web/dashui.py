import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import psycopg2
import dash_table

#import dash_table_experiments as dt


#dataframe = pd.DataFrame()

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
    select distinct b.listing_id,b.name as bnbname,b.price as bnbprice
    from uniquecategories uc, yelpcategory yc, yelpbusiness yb, distance d, bnbsum b
    where b.listing_id = d.listing_id and yb.business_id = d.business_id and 
    yb.business_id = yc.yelp_id and yc.category_id = uc.id
    and b.city = '{}' and uc.name='{}' limit 5
        """.format(city,interest)
    return fetch(query)

def get_businesses(city,interest):
    query = """
    select distinct yb.name, yb.stars,yb.review_count ,yb.categories,yb.address
    from uniquecategories uc, yelpcategory yc, yelpbusiness yb, distance d, bnbsum b
    where b.listing_id = d.listing_id and yb.business_id = d.business_id and 
    yb.business_id = yc.yelp_id and yc.category_id = uc.id
    and b.city = '{}'and uc.name='{}'
    order by yb.stars DESC limit 5
        """.format(city,interest)
    return fetch(query)

def fetch(query):
    connection = psycopg2.connect(database='tripperdump', user='tripper',password='tripper', host='localhost', port = 5431)
    df = pd.read_sql(query,connection)
    connection.close()
    return df

colors = {
    'background': '#111111',
    'text': '#7FDBFF',
    "blue": "#4285f4",
    "white": "#fff2ff"
}

dfcity = get_unique_cities()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(style={'backgroundColor': colors['background']},
                children=[
                        
                    html.H3('Welcome to TRIPPER!!!', style={"backgroundColor": colors["blue"],'textAlign': 'center','color': colors['text']}),
                    html.H6('Your one stop solution to plan the perfect vacation!', style={'textAlign': 'center','color': colors['text']}),
                    html.H6('Select the City where you want to have a vacation - ', style={'textAlign': 'center','color': colors['text']}),
                     
                    html.Div(
                    [
                    #html.Label('Select City'),
                    dcc.Dropdown(
                            id = 'city_dropdown',
                            options = [{"label": l, "value": l} for l in dfcity.city],
                            placeholder = "Select a city,style{'textAlign': 'center'}")

                    ])
                    ,


                    html.H6('Now Select what interests you - ', style={'textAlign': 'center','color': colors['text']}),
                    
                    html.Div(
                    [
                    #html.Label('Select Interest'),
                    dcc.Dropdown(
                            id = 'interest_dropdown',
                            placeholder = "Select an Interest,style{'textAlign': 'center'}"
                            )
                    ]), 
                    
                    html.H6('The best places you can live at are', style={'textAlign': 'center','color': colors['text']}),
                    
                    html.Div(
                            id = 'tableDiv',
                            
                            children = 
                            dash_table.DataTable(
                            id = 'table',
                                    )
                            ),
                            
                    html.H4('The places you can visit are', style={'textAlign': 'center','color': colors['text']}),
                    
                    html.Div(
                            id = 'tableDiv1',
                            
                            children = 
                            dash_table.DataTable(
                            
                            id = 'table1',
                                       )
                            )
                            ])
@app.callback(
    Output("interest_dropdown","options"),
    [Input("city_dropdown",'value')])
def interest_dropdown(city):
    dfinterests = get_interests_from_city(city)
    return[{"label": l, "value": l} for l in dfinterests.name]


@app.callback(
     Output("tableDiv","children"),
     [Input("city_dropdown","value"),Input("interest_dropdown","value")])
def generate_table(city,interest):
    df = get_results(city,interest)
    mycolumns = [{'name': i, 'id': i} for i in df.columns]
    return html.Div([
                dash_table.DataTable(
            id='table',
               style_header={'backgroundColor': 'rgb(30, 30, 30)'},
    style_cell={
        'backgroundColor': 'rgb(50, 50, 50)',
        'color': 'white'},
                          
            columns=mycolumns,
            data=df.to_dict("rows") )
        ])
                
@app.callback(
     Output("tableDiv1","children"),
     [Input("city_dropdown","value"),Input("interest_dropdown","value")])
def generate_table1(city,interest):
    df = get_businesses(city,interest)
    mycolumns = [{'name': i, 'id': i} for i in df.columns]
    return html.Div([
                dash_table.DataTable(
            id='table1',
               style_header={'backgroundColor': 'rgb(30, 30, 30)'},
    style_cell={
        'backgroundColor': 'rgb(50, 50, 50)',
        'color': 'white'},
                          
            columns=mycolumns,
            data=df.to_dict("rows") )
        ])
    #return build_table(dataframe)
if __name__ == '__main__':
    app.run_server(debug=True,host='0.0.0.0')

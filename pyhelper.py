

"""
'''
Fin methods:
load_and_clean, get_cov, get_corr, get_beta, get_sharpe, get_volatility,
allocate, get_cum_returns, drop (code hint), get_rolling

SQL methods to return code hints:
create, select, update, delete, insert, join, agg, sub
connect (returns engine)
'''

"""
import os
from dotenv import load_dotenv
import requests
import numpy as np
import pandas as pd
import panel as pn
from panel.interact import interact
from panel import widgets
import plotly.express as px
pn.extension('plotly')
import hvplot.pandas
from sqlalchemy import create_engine
 #to be used with pandas
 #work in progress
class Finhelper: #fin helper
    def __init__(self):
        pass
    def load_and_clean(self,in_path):
        df = pd.read_csv(in_path, index_col = 0, parse_dates = True, infer_datetime_format = True)
        df.dropna(inplace = True)
        df.drop_duplicates(inplace = True)
        df.sort_index()
        return df

    def get_cov(df, tick, ind):
        #return covariance from dataframe, ticker, and index
        cov= df[tick].cov(df[ind])
        return cov
    
    def get_corr(self,df):
        return df.corr()
        
    def get_beta(self,df,tick,ind):
        #get beta from dataframe, ticker, and index, uses get_cov
        cov = get_cov(df,tick,ind)
        var = df[ind].var()
        beta = cov / var
        return beta

    def get_sharpe(self,df, df_type):
        #df_type should be either "price" or "returns" based on dataframe passed in
        if df_type == "price":
            df = df.pct_change(inplace = True)
        sharpe = (df.mean() * 252) / (df.std() * np.sqrt(252))
        return sharpe
        
    def get_volatility(self,df):
        #annualized standard deviation
        return df.std() * np.sqrt(252)
        
    def allocate(self,weights, df):
        #weighted portfolio, weights must be a list of same count as assets
        return df.dot(weights)
        
    def get_cum_returns(self,df, init_inv = 1):
        #dataframe must be daily_returns.  Optional investment amount as 2nd arg
        return ((1 + df).cumprod()) * init_inv
        
    def drop(self):
        #returns help text...thought I'd try this out
        return f"dataframe.drop(columns=['column1', 'column2', 'column3'], inplace=True)"
        #return "Hello"
    def get_rolling(df, days):
        #returns rolling average
        return df.rolling(window=days).mean()
            
class SQLhelper:
    def __init__(self):
        pass
        
    def create(self):
        print("CREATE TABLE <tablename> ( \n"
               "    <field1> SERIAL PRIMARY KEY,\n"
               "    <field2> INT,\n"
               "    <field3> DOUBLE PRECISION,\n"
               "    <field4> FLOAT(10)\n"
               ");"
        )
    
    def select(self):
        print("SELECT <field1>, <field2>, <field3> (or *)\n"
              "FROM <table>\n"
              "WHERE condition (i.e. <field1> > 100)\n"
              "ORDER BY <field1> ASC;"
        )
    
    def update(self):
        print("UPDATE <table>\n"
              "SET <field> = newvalue\n"
              "WHERE <field> = oldvalue;"
        )
    
    def delete(self):
        print("DELETE FROM table\n"
              "WHERE <field1> = value;"
        )
    
    def insert(self):
        print("INSERT INTO table\n"
               "  (<field1>,<field2>,<field3>,<field4>)\n"
               "VALUES\n"
               "(<field1Val>,<field2Val>,<field3Val>,<field4Val>);"
        )
     
    def join(self):
        print("SELECT tbl1.<field1>, tbl1.<field2>, tbl2.<field1>, tbl2.<field2>\n"
              "FROM tbl1 AS alias\n"
              "INNER/LEFT/RIGHT/FULL OUTER/CROSS JOIN tbl2 as alias2 ON alias.<field1> = alias2.<field2>;"
             
        )
        
    def agg(self):
        print("SELECT COUNT(<field>) FROM table;\n\n"
              "SELECT <field1>, COUNT(<field2>) AS \"Total Field2s\"\n"
              "GROUP BY <field1>;"
        )
        
    def sub(self):
        print("SELECT * \n"
              "FROM table \n"
              "WHERE <field2> IN\n"
              "(\n"
              "  SELECT <field1>\n"
              "  FROM table2\n"
              "  WHERE <field2> = value\n"
              ");"
             
        )
        
    def connect(self,db_name):
        engine = create_engine(f"postgresql://postgres:postgres@localhost:5432/{db_name}")
        return engine

class PVHelper:
    def __init__():
        pass
    
    def hvscatter(self,df,x,y, title = "Scatter Plot"):
        return df.hvplot.scatter(
            x = x,
            y = y,
            title = title
        )

    def pxscatter(self,df,x,y,title = "Scatter Plot"):
        return px.scatter(
            df,
            x = x,
            y = y,
            title = title
        )
    
    def mapbox(self,df,lat,lon,keyname = "mapbox"):
        load_dotenv()
        mb_api = os.getenv("mapbox")
        px.set_mapbox_access_token(mb_api)
        scatter_map = px.scatter_mapbox(
            df,
            lat = lat,
            lon = lon
        )
        return scatter_map

class APIHelper:
    def __init__():
        pass

    def get(self,url, param = ""):
        url += f"{param}?format=json"
        response_data = requests.get(url).json()
        return response_data

    def view(self, response):
        print(json.dumps(data,indent = 4))

    def alpaca_create(self, keyname = "ALPACA_API_KEY", secret = "ALPACA_SECRET_KEY"):
        #!!alpaca_trade_api must be loaded as tradeapi
        aak = os.getenv(keyname)
        ask = os.getenv(secret)

        alpaca = tradeapi.REST(
            aak,
            ask,
            api_version="v2"
        )
        return alpaca

    def get_alpaca(self,api,ticker_list,start,end, timeframe = "1D"):
        s = pd.Timestamp(start,tz = "America/New_York").isoformat()
        e = pd.Timestamp(end,tz = "America/New_York").isoformat()
        
        df = api.get_barset(
            ticker_list,
            timeframe,
            start = s,
            end = e

        ).df
        return df


#pyhelper.py import file.  Acts as a wrapper around common FinTech code
#Currently has 4 classes - Finhelper, SQLhelper, PVhelper, and APIhelper

#Written by toddshev, if anything is broken or you think anything can be added, let me know
#Otherwise, hopefully it's helpful
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
import json
from pathlib import Path
from dotenv import load_dotenv
import requests
import numpy as np
import pandas as pd
import alpaca_trade_api as tradeapi
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
        """
        Will read in csv from path using Path method, index first column, parse dates,
        drop NA's, drop duplicates, and sort by the index. Returns dataframe
        """
        in_path = Path(in_path)
        try:
            df = pd.read_csv(in_path, index_col = 0, parse_dates = True, infer_datetime_format = True)
        except:
            print("Could not read csv file.  Please check the path")
        finally:
            #attempt to clean df
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
        """
        Need to supply dataframe, ticker, and index to compare it to
        Calls get_cov method
        """
        cov = get_cov(df,tick,ind)
        var = df[ind].var()
        beta = cov / var
        return beta

    def get_sharpe(self,df, df_type = "returns"):
        """
        Requires dataframe.  If no df_type or "returns" provided, will assume DF has percent changes
        If "price" is passed, will calculate the pct change prior to returning sharpe ratios
        """
        if df_type == "price":
            df = df.pct_change()
        sharpe = (df.mean() * 252) / (df.std() * np.sqrt(252))
        return sharpe
        
    def get_volatility(self,df):
        #annualized standard deviation
        df = df.std() * np.sqrt(252)
        df.sort_values(inplace = True)
        return df
    def allocate(self,weights, df):
        """
        Must pass in weights that match list of assets, then dataframe
        """
        return df.dot(weights)
        
    def get_cum_returns(self,df, init_inv = 1):
        """
        Dataframe must be daily returns.  Optional investment amount as 2nd argument
        """
        return ((1 + df).cumprod()) * init_inv
        
    def drop(self,df, column_list):
        """
        Requires dataframe and list of coumns you wish to remove
        """
        df.drop(columns = column_list, inplace = True)
        return df
   
    def get_rolling(self,df, days):
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
        try:
            engine = create_engine(f"postgresql://postgres:postgres@localhost:5432/{db_name}")
        except:
            print(f"Issue connecting to {db_name}")
        return engine

class PVhelper:
    def __init__(self):
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
        if not type(mb_api) is str:
            raise TypeError("Could not find mapbox key")
        else:
            try:
                px.set_mapbox_access_token(mb_api)
            except:
                print("Could not set mapbox key")
            finally:  #in case it will still load
                scatter_map = px.scatter_mapbox(
                    df,
                    lat = lat,
                    lon = lon
                )
        return scatter_map

class APIhelper:
    def __init__(self):
        pass
        #self.response_data = {}

    def get(self, url, **kwargs):
        try:
            url += f"?format=json"
            if type(kwargs) == None:
                response_data = requests.get(url).json()
            else:
                response_data = requests.get(url, params = kwargs).json()
        except:
            if type(kwargs) ==None:
                response_data = requests.get(url)
            else:
                response_data = requests.get(url, params = kwargs)
        return response_data

    def view(self, data):
        print(json.dumps(data,indent = 4))

    def alpaca_create(self, keyname = "ALPACA_API_KEY", secret = "ALPACA_SECRET_KEY"):
        """
        Default key names are "ALPACA_API_KEY" and "ALPACA_SECRET_KEY".
        If your .env differs, enter those key names as strings
        """
        aak = os.getenv(keyname)
        ask = os.getenv(secret)
        if type(aak) is not str | type(aak) is not str:
            raise Exception("Could not load API or Secret Key")
        #try to create object regardless    
        alpaca = tradeapi.REST(
            aak,
            ask,
            api_version="v2"
        )
        self.alpaca_api = alpaca
        return alpaca

    def get_alpaca_data(self,ticker_list,start,end, timeframe = "1D"):
        """
        Requires you to run alpaca_create first, dates should be entered as 'yy-mm-dd'
        Default timeframe is '1D', you may change this if desired
        """
        s = pd.Timestamp(start,tz = "America/New_York").isoformat()
        e = pd.Timestamp(end,tz = "America/New_York").isoformat()
        
        df = api.get_barset(
            ticker_list,
            timeframe,
            start = s,
            end = e

        ).df
        return df

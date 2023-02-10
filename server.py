'''
Created on 04-Sep-2019

@author: bkadambi
'''

# -*- coding: UTF-8 -*-
"""
hello_flask: First Python-Flask webapp
"""
import pandas as pd
from flask import jsonify, make_response
from flask import json
import json
import os
import webbrowser
import pyTigerGraph as pyTG
from flask import Flask, request, jsonify, render_template
import pyTigerGraph as tg
import numpy as np
from flask import jsonify, make_response
from flask import json
import json
import os
os.environ['MPLCONFIGDIR'] = '/tmp'
import webbrowser
from datetime import datetime, timedelta
from datetime import datetime
from dateutil import parser
import pyTigerGraph as pyTG
import pyTigerGraph as tg
from flask import Flask, request, render_template_string, jsonify, render_template
from flask_cors import CORS
from flask_cors import CORS, cross_origin
from pipeline_qda_new_query import qda_pipeline
from rule_based_pipeline_new_query import test_pipeline
from Dataframecreation_updated import Data_Base_Value
from louvain_pipeline import louvain_result
from cosine import cosine_result
from flask_cors import CORS, cross_origin
import sys, json
import csv
import urllib
import bz2
import threading
from json import dumps
from geopy.geocoders import Nominatim
from pathlib import Path
from plot import plot_by_category,plot_by_date,plot_by_merchant,plot_confusion_matrix
import reverse_geocoder as rg
from numerize import numerize
from sklearn.metrics import precision_score,recall_score, confusion_matrix, classification_report,accuracy_score, f1_score
from datetime import date,timedelta
import random
from louvain_visualisation import louvain_graph,community_level_graph,customer_level_graph

app = Flask(__name__)
CORS(app)
app.config["DEBUG"] = True
app.config['CORS_HEADERS'] = 'Content-Type'

final_list=[]
transactions = pd.DataFrame()
louv_result = pd.DataFrame()
community_id = ""
# history_ls=[]

def connection(graph):
    #for azure server
    conn = tg.TigerGraphConnection(host="http://169.60.49.174",graphname=graph,username="tigergraph",password="tigergraph") 
    secret = conn.createSecret()
    conn.getToken(secret, "1000000")
    return conn

def all_alerts_df(graph):
    conn = connection(graph)
    alertsList = conn.runInstalledQuery("all_alerts")
    df1=pd.json_normalize(alertsList, record_path =['open_alerts_with_type'], max_level=0)['attributes'].apply(pd.Series)
    return df1

show_conn= connection("Alerts")
conn = connection("TX_CRD")
louv_conn = connection('louvien')
cosine_conn = connection("Coisine_Final")
model_performance_conn = connection("model_performance")
print("connection are done")
"""
from flask import Flask  # From module flask import class Flask
app = Flask(__name__)    # Construct an instance of Flask class for our webapp
"""
@app.route('/')   # URL '/' to be handled by main() route handler
def main():
    """Say hello"""
    return 'Hello, world!'

if __name__ == '__main__':  # Script executed directly?
    print("Hello World! Built with a Docker file.")
    app.run(host="0.0.0.0", port=5000, debug=True,use_reloader=True)  # Launch built-in web server and run this Flask webapp

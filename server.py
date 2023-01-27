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
conn = connection("TX_CRD")
print("connection are done")


@app.route('/')
@app.route("/Main")
@app.route("/Prediction")  
@app.route('/DataModel')
@app.route("/Assess")


@app.route('/')
@cross_origin()
def index():
    return render_template('index.html')


@app.route("/Main_Page", methods=['GET', 'POST'])
@cross_origin()
def Main_Page():
    sent_data = request.get_json()
    print(sent_data)

@app.route("/All_alerts_download", methods=['GET', 'POST'])
@cross_origin()
def All_alerts_download():
    sent_data = request.get_json()
     
@app.route('/Main_service', methods=['GET','POST'])
def Main_service():
    data = json.loads(request.data)
    print(data)

@app.route("/Assessment_Page", methods=['GET','POST'])   
def Assessment_Page():
    sent_data = request.get_json()
    print(sent_data)

@app.route("/Assessment_Service", methods=['GET','POST'])  
def Assessment_Service():

    data = json.loads(request.data)
    print(data)
    Rationale= data['rationale']
    Action=data['action']
    # conn = connection("TX_CRD")
    code ="""INTERPRET QUERY (SET <UINT> State_ID, STRING New_Status, STRING Reason_L2) FOR GRAPH TX_CRD {
    start = {Alert.*};
    result =
    SELECT src
    FROM start:src
    WHERE src.id IN State_ID
    POST-ACCUM src.Alert_Status = New_Status,
    src.Reason_Status_L2 = src.Reason_Status_L2 + "|" + Reason_L2;
    
    }"""
    if ( (Action=="Open") and data['txn'] is not None ):
    
        for x in data['txn']:
            alert_list=x['id']
            res = conn.runInterpretedQuery(code, {"State_ID": alert_list,"New_Status": "Open","Reason_L2": Rationale})
            print("updated in DB")
    elif ((Action == "Close") and data['txn'] is not None) : 
         for x in data['txn']:
            alert_list=x['id']
            res = conn.runInterpretedQuery(code, {"State_ID": alert_list,"New_Status": "Close","Reason_L2": Rationale})
            print("updated in DB")
    elif ((Action == "Fraud") and data['txn'] is not None) : 
         for x in data['txn']:
            alert_list=x['id']
            res = conn.runInterpretedQuery(code, {"State_ID": alert_list,"New_Status": "Fraud","Reason_L2": Rationale})
            print("updated in DB")        
    else:
        return "No alert update happened"         
    return ("data updated in DB") 



def database_update(df_db,msgmodel,graph):
    df_db = df_db.rename(columns={'Topics':'Topic','TX_ID':'id'})
    df_db ["Alert_Status"] = 'Open'  
    df_db ["Alert_Message"] = msgmodel
    df_db[['Lat', 'Lon']]= df_db[['Lat', 'Lon']].astype(str)
    df_db[['Date_Created']]=df_db[['Date_Created']].astype(str)   
    conn = connection(graph)
    df_update=conn.upsertVertexDataFrame(df_db,"Alert","id",attributes={"Client_ID": "Client_ID", "Lon": "Lon", "Lat": "Lat","Alert_Status":"Alert_Status","Alert_Message":"Alert_Message","Date_Created":"Date_Created"})
    return df_update  


def data_processing(df,msg,dflonlat,graph):

    if msg == "Rule Based Approach":
        df =df[['tx_id','cont_id','aureq_tx_dt_ttlamt','predict','hdr_credtt','aureq_env_m_cmonnm','mdm_postal_code_id','dist_frd_mean','aureq_tx_mrchntctgyc','age','aureq_env_c_cardbrnd']]
        df = df.rename(columns={ 'tx_id':'TX_ID','aureq_tx_dt_ttlamt':'Amount' ,'hdr_credtt':'Date_Created','aureq_env_m_cmonnm':'Topic','cont_id':'Client_ID','mdm_postal_code_id':'state_id','dist_frd_mean':'dist_frd_mean','aureq_tx_mrchntctgyc':'merchant_ID','age':'age','aureq_env_c_cardbrnd':'Card brand'})
    if msg == "Quadratic Discriminant Analysis":
        df =df[['tx_id','cont_id','aureq_tx_dt_ttlamt','predict','hdr_credtt','aureq_env_m_cmonnm','mdm_postal_code_id','fraud_probability','aureq_tx_mrchntctgyc','age','aureq_env_c_cardbrnd']]
        df = df.rename(columns={ 'tx_id':'TX_ID','aureq_tx_dt_ttlamt':'Amount' ,'hdr_credtt':'Date_Created','aureq_env_m_cmonnm':'Topic','cont_id':'Client_ID','mdm_postal_code_id':'state_id','fraud_probability':'fraud_probability','aureq_tx_mrchntctgyc':'merchant_ID','age':'age','aureq_env_c_cardbrnd':'Card brand'})
    df = pd.merge(df, dflonlat[['Lat','Lon']],on=df['TX_ID'])
    df=df[(df == 'fraud').any(axis=1)]
    df.drop(['key_0'],axis=1,inplace=True)
    # df_data=database_update(df,msg,graph)
    data_json = df.to_json(orient = 'records')
    parsed_json=json.loads(data_json)
    return parsed_json
    
def data_processing_cosine(df):
    c_result = df[['cust_id','only_id', 'frds', 'sim_frds']]
    l = c_result['only_id']
    mylist = []
    f_l = []
    for i in l:
        x = len(i)
        a=1
        for j in i:
            if a < x:
                j = j+","
            mylist.append(j)
            a=a+1
        f_l.append(mylist)
        mylist = []
    l =f_l

    c_result['only_id'] = l
    l = c_result['sim_frds']
    mylist = []
    f_l = []
    for i in l:
        x = len(i)
        a=1
        for j in i:
            if a < x:
                j = str(j)+","
            else:
                j = str(j)
            mylist.append(j)
            a=a+1
        f_l.append(mylist)
        mylist = []
    l =f_l
    
    c_result['sim_frds'] = l
    
    data_json = c_result.to_json(orient = 'records')
    parsed_json=json.loads(data_json)
    return parsed_json


def data_processing_louvain(louv_result):
    dummy_df=louv_result[(louv_result['isfraud'] == 1)]
    frauds= sum(dummy_df['frauds_in_community'].unique())
    dummy_df.loc[:,'fraud_penetration']= np.round(dummy_df['frauds_in_community']/frauds,4)*100
    dummy_df = dummy_df[['cont_id','louvain_communities','fraud_penetration']]
    dummy_df = dummy_df.sort_values('fraud_penetration',ascending=False)
    data_json = dummy_df.to_json(orient = 'records')
    parsed_json=json.loads(data_json)
    return parsed_json
    
def predict_risk(df):
    my_list = []
    df['hdr_credtt'] = pd.to_datetime(df['hdr_credtt'])
    df_need = pd.DataFrame({'Timestamp': df['hdr_credtt'], 'decimal_probabilty': df['fraud_probability']})
    df_need['time'] = df_need['Timestamp'].apply(lambda x: "%d/%d/%d" % ( x.year,x.month,x.day))
    df_need = df_need.sort_values('time')
    df_need['probabilty'] = df_need['decimal_probabilty'].apply(lambda x : round((x *100),1))
    df_need.loc[df_need['probabilty']>=90.0,'risk_category_highrisk']= 'High_risk'
    df_need.loc[(df_need['probabilty'] < 90.0) & (df_need['probabilty'] > 30.0),'risk_category_moderate']= 'Moderate_risk'
    df_need.loc[(df_need['probabilty']<=30.0),'risk_category_lowrisk']= 'Low_risk'
    unq_dates= df_need['time'].unique()
    unq_list = unq_dates.tolist()
    my_list.append(unq_list)
    grp_df_high= pd.DataFrame(df_need.groupby(['time'])['risk_category_highrisk'].value_counts())
    grp_df_high.columns= ['counts']
    grp_df_high.reset_index(inplace=True)
  
    list_high = grp_df_high['counts'].tolist()
    high_date =grp_df_high['time'].tolist()
    v = list((set(unq_list).difference(high_date)))
    for i in v:
        pos = unq_list.index(i)
        list_high.insert(pos,0)
    
    my_list.append(list_high)
    
    grp_df_moderate = pd.DataFrame(df_need.groupby(['time'])['risk_category_moderate'].value_counts())
    grp_df_moderate.columns= ['counts']
    grp_df_moderate.reset_index(inplace=True)
    
    list_moderate = grp_df_moderate['counts'].tolist()
    moderate_date =grp_df_moderate['time'].tolist()
    v = list((set(unq_list).difference(moderate_date)))
    for i in v:
        pos = unq_list.index(i)
        list_moderate.insert(pos,0)

    my_list.append(list_moderate)
    
    grp_df_low = pd.DataFrame(df_need.groupby(['time'])['risk_category_lowrisk'].value_counts())
    grp_df_low.columns= ['counts']
    grp_df_low.reset_index(inplace=True)
    
    list_low = grp_df_low['counts'].tolist()
    low_date =grp_df_low['time'].tolist()
    v = list((set(unq_list).difference(low_date)))
    for i in v:
        pos = unq_list.index(i)
        list_low.insert(pos,0)
    
    my_list.append(list_low)

    return my_list


def predict_risk_RBA(df):
    my_list = []
    df['hdr_credtt'] = pd.to_datetime(df['hdr_credtt'])
    df_need = pd.DataFrame({'Timestamp': df['hdr_credtt'], 'decimal_probabilty': df['dist_frd_mean']})
    df_need['time'] = df_need['Timestamp'].apply(lambda x: "%d/%d/%d" % ( x.year,x.month,x.day))
    df_need = df_need.sort_values('time')
    df_need['probabilty'] = df_need['decimal_probabilty'].apply(lambda x : round(x,2))
    df_need.loc[(-1 <= df_need['probabilty']) & (df_need['probabilty'] <= 1),'risk_category_highrisk']= 'High_risk'
    df_need.loc[(( -1.75 <= df_need['probabilty'] ) & (df_need['probabilty']  < -1)) | ((1 <= df_need['probabilty']) & (df_need['probabilty'] < 1.75)) ,'risk_category_moderate']= 'Moderate_risk'
    df_need.loc[(df_need['probabilty'] < -1.75) | (df_need['probabilty'] > +1.75),'risk_category_lowrisk']= 'Low_risk'
    unq_dates= df_need['time'].unique()
    unq_list = unq_dates.tolist()
    my_list.append(unq_list)
    grp_df_high= pd.DataFrame(df_need.groupby(['time'])['risk_category_highrisk'].value_counts())
    grp_df_high.columns= ['counts']
    grp_df_high.reset_index(inplace=True)

    list_high = grp_df_high['counts'].tolist()
    high_date =grp_df_high['time'].tolist()
    v = list((set(unq_list).difference(high_date)))
    for i in v:
        pos = unq_list.index(i)
        list_high.insert(pos,0)

    my_list.append(list_high)

    grp_df_moderate = pd.DataFrame(df_need.groupby(['time'])['risk_category_moderate'].value_counts())
    grp_df_moderate.columns= ['counts']
    grp_df_moderate.reset_index(inplace=True)

    list_moderate = grp_df_moderate['counts'].tolist()
    moderate_date =grp_df_moderate['time'].tolist()
    v = list((set(unq_list).difference(moderate_date)))
    for i in v:
        pos = unq_list.index(i)
        list_moderate.insert(pos,0)

    my_list.append(list_moderate)

    grp_df_low = pd.DataFrame(df_need.groupby(['time'])['risk_category_lowrisk'].value_counts())
    grp_df_low.columns= ['counts']
    grp_df_low.reset_index(inplace=True)

    list_low = grp_df_low['counts'].tolist()
    low_date =grp_df_low['time'].tolist()
    v = list((set(unq_list).difference(low_date)))
    for i in v:
        pos = unq_list.index(i)
        list_low.insert(pos,0)

    my_list.append(list_low)

    return my_list    


def convert_list(df):
    dummy_df = pd.DataFrame(np.vstack([df.columns, df]))
    ls = dummy_df.values.tolist()
    return ls    


@app.route("/model_Rulebased", methods=['GET','POST'])
@cross_origin(supports_credentials=True)

def model_Rulebased():
    sent_data = request.get_json()
    print(sent_data)
    Action1= sent_data['action1']
    Action2=sent_data['action2']
    global final_res
    final_res=[]
    global history_ls
    history_ls=[]
    suffix='csv'
    global start_date
    start_date=""
    global end_date
    end_date=""
  
    
    if((sent_data['action1'] =='TX_CRD') and (sent_data['action2'] =='Rule Based Approach')):
        # conn = connection('TX_CRD')
        flag_predict ="1"
        start_date=sent_data['startdate']
        end_date=sent_data['enddate']
        n_cl = float( sent_data['action3'])
        global input_parameter_rba
        input_parameter_rba = n_cl
        startTime_db = datetime.now()
        Result_DB = conn.runInstalledQuery("tr_rule_new", params = {"start_date":start_date,"end_date":end_date})
        client_info,tranx_df,latlon_df =Data_Base_Value(Result_DB)
        endTime_db = datetime.now() - startTime_db
        print("RBA QUERY",endTime_db)
        startTime_pl = datetime.now()
        fraud,all_tranx,duration,m_time= test_pipeline(client_info,tranx_df,n_cl)
        endTime_pl = datetime.now() - startTime_pl
        print("RBA pipeline",endTime_pl)
        startTime_dp = datetime.now()
        my_list = predict_risk_RBA(fraud)
        duration = duration.total_seconds()
        duration = str(round(duration,2))
        all_tranx_length=str(len(all_tranx))
        data_json = all_tranx.to_json(orient = 'records')
        trans_json=json.loads(data_json)
        final_res.append(trans_json)
        final_res.append(m_time)
        global total_transactions
        total_transactions = len(all_tranx)
        filename = 'Rule_Base_Approach'
        all_tranx_list = convert_list(all_tranx)              
        parsed_json=data_processing(all_tranx, 'Rule Based Approach',latlon_df,'TX_CRD')
        history_ls.append(parsed_json)
        history_ls.append(Action2)
        endTime_dp = datetime.now() - startTime_dp
        print("RBA dataprocessing",endTime_dp)
        # history_ls[0]= parsed_json
        # history_ls[1]= Action2
        return jsonify({'df':parsed_json,'tranx_length':all_tranx_length,'duration':duration,"risk_list":my_list,"all_list":all_tranx_list})
   
    if((sent_data['action1'] =='TX_CRD') and (sent_data['action2'] =='Quadratic Discriminant Analysis')):
        # conn = connection('TX_CRD')
        start_date=sent_data['startdate']
        end_date=sent_data['enddate']
        Result_DB = conn.runInstalledQuery("tr_rule_new", params = {"start_date":start_date,"end_date":end_date})
        client_info,tranx_df,latlon_df =Data_Base_Value(Result_DB)
        filename = 'Quadratic_Approach'
        probality=  int( sent_data['action3'])
        global input_parameter_qda
        input_parameter_qda = probality 
        probality=probality/100
        fraud_df, All_data, duration,m_time= qda_pipeline(client_info,tranx_df,probality)
        duration = duration.total_seconds()
        duration = str(round(duration, 2))
        all_tranx_length = str(len(All_data))
        total_transactions = len(All_data)
        data_json = All_data.to_json(orient = 'records')
        trans_json=json.loads(data_json)
        final_res.append(trans_json)
        final_res.append(m_time)
        All_data_list = convert_list(All_data) 
        my_list = predict_risk(fraud_df)
        parsed_json=data_processing(All_data,'Quadratic Discriminant Analysis',latlon_df,'TX_CRD')
        history_ls.append(parsed_json)
        history_ls.append(Action2)
        return jsonify({'df':parsed_json,'tranx_length':all_tranx_length,'duration':duration,"risk_list":my_list,"all_list":All_data_list})
    

@app.route("/dashboard", methods=['GET','POST'])
@cross_origin(supports_credentials=True)  

def dashboard():
    sent_data = request.get_json()
    print(sent_data)
    # conn = connection("TX_CRD")
    start_date_dashboard = sent_data['startdate']
    end_date_dashboard = sent_data['enddate']
    global value_final
    value_final=[]
    
    total_trns=conn.getVertexCount('Transactions',where = 'TX_Date >= "'+start_date_dashboard+'",TX_Date < "'+end_date_dashboard+'"')
    actual_df=conn.getVertexDataFrame(vertexType = 'Transactions',sort = "TX_Date",select = 'TX_Amount,MDM_POSTAL_CODE_ID,Fraud_Id,AUREQ_ENV_M_ID_ID,AUREQ_TX_MRCHNTCTGYC,TX_Date,AUREQ_ENV_M_CMONNM,Lat,Lon',where = 'Fraud_Id = "Y",TX_Date >= "'+start_date_dashboard+'",TX_Date < "'+end_date_dashboard+'"' )
    print(total_trns)
    min_amt = float(sent_data['Ammount'].replace('k',''))
    value_final.append([str(total_trns)])                                                           #output
    actual_df = actual_df.rename(columns={'AUREQ_ENV_M_CMONNM':'Topic'})
    
    frd_dum_df= actual_df[actual_df['Fraud_Id']=='Y']
    frd_grp_cnts= pd.DataFrame(frd_dum_df.groupby(['Topic'])['Fraud_Id'].count())
    num_frd_trns= frd_dum_df.shape[0]
    value_final.append([str(num_frd_trns)])
    
    total_frd_amount= frd_dum_df[frd_dum_df['TX_Amount']>min_amt]['TX_Amount'].sum()               #output
    value_final.append([str(round(total_frd_amount,2))])
    
   
    Nfrd_dum_df= actual_df[actual_df['Fraud_Id']=='N']
    Nfrd_grp_cnts= pd.DataFrame(Nfrd_dum_df.groupby(['Topic'])['Fraud_Id'].count())
    num_Nfrd_trns= Nfrd_dum_df.shape[0]                                                            #output

    frd_perc= np.round((num_frd_trns/total_trns)*100,2)                                            #output
    value_final.append([str(frd_perc)])

    frd_grp_cnts.reset_index(inplace=True)
    frd_grp_cnts= frd_grp_cnts.rename(columns={'Fraud_Id':'counts'})

    Nfrd_grp_cnts.reset_index(inplace=True)
    Nfrd_grp_cnts= Nfrd_grp_cnts.rename(columns={'Fraud_Id':'counts'})

    full_data_cnts= pd.DataFrame(actual_df.groupby(['Topic'])['Fraud_Id'].count())
    full_data_cnts.reset_index(inplace=True)
    full_data_cnts= full_data_cnts.rename(columns={'Fraud_Id':'counts'})

    total_sum= full_data_cnts['counts'].sum()                                                      #output
   
    cat_total_dict= dict(zip(full_data_cnts['Topic'],full_data_cnts['counts']))
    cat_frd_dict= dict(zip(frd_grp_cnts['Topic'],frd_grp_cnts['counts']))
    cat_non_frd_dict= dict(zip(Nfrd_grp_cnts['Topic'],Nfrd_grp_cnts['counts']))

    marklist= sorted(cat_total_dict.items(), key=lambda x:x[1],reverse=True)
    sortdict = dict(marklist)
    top_list= list(sortdict.keys())[:20]                       
    
    frd_perc_cat= percentagevalue(cat_frd_dict)                                                   #output
    global plot_category_fig 
    plot_category_fig = plot_by_category(frd_perc_cat.values(),frd_perc_cat.keys())

    fraud_value = Averagefraudpercentage(actual_df,start_date_dashboard,end_date_dashboard)     #output
    global plot_date_fig 
    plot_date_fig = plot_by_date(list(fraud_value.keys()),list(fraud_value.values()))

    merch_per= pd.DataFrame(np.round((frd_dum_df['AUREQ_TX_MRCHNTCTGYC'].value_counts()/frd_dum_df.shape[0])*100,2))
    merch_per.reset_index(inplace=True)
    merch_per.rename(columns= {'index':'merchant_id','AUREQ_TX_MRCHNTCTGYC':'percentage'}, inplace=True)
    merch_per = merch_per.replace([5733,5732,5192,5651],['AMZ Music','CRM Electronics','ANM Apparel','HMk Books'])
    merch_list2 =  merch_per["merchant_id"].astype('str').to_list()
    merch_list1 =  merch_per["percentage"].to_list()
    global plot_merchant_fig
    plot_merchant_fig = plot_by_merchant(merch_list2,merch_list1)

    global loc_df
    loc_df = locat(actual_df)

    if frd_perc < 30.0:
        value_final.append(["LOW RISK"])
    elif frd_perc > 90.0:
        value_final.append(["HIGH RISK"])
    else:
        value_final.append(["MEDIUM RISK"])
    print(value_final)    

    return jsonify({"bar_by_category":plot_category_fig,"bar_by_date":plot_date_fig,"location_df":loc_df,"bar_by_merchant":plot_merchant_fig,"value_final":value_final})        

@app.route("/dashboard_history", methods=['GET','POST'])
@cross_origin(supports_credentials=True)

def dashboard_history():
    try:
        dashboard_value = value_final
        category_fig = plot_category_fig
        date_fig = plot_date_fig
        location_df = loc_df
        merchant_fig = plot_merchant_fig
        print("dashboard history printed") 
        return jsonify({"bar_by_category":category_fig,"bar_by_date":date_fig,"location_df":location_df,"bar_by_merchant":merchant_fig,'dashboard_value':dashboard_value}) 
    except:
        print("")
        return "history printed"    
    

# get the data for the requested query
if __name__ == '__main__':
    #app.run(host="0.0.0.0", port=3000, threaded=True) 
    #app.run(host=os.getenv('LISTEN', '0.0.0.0'), port=int(os.getenv('PORT', '443')))
    app.run(host=os.getenv('LISTEN', '0.0.0.0'), port=int(os.getenv('PORT', '8888')))
    # app.run(host=os.getenv('LISTEN', '0.0.0.0'), port=int(os.getenv('PORT', '14240')))



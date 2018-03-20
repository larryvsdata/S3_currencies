import boto3
import os
import sys
from io import StringIO
import pandas as pd
import gzip
import datetime
import psycopg2

from flask import Flask
from flask import render_template
from datetime import time


aws_access_key_id = ''
#aws_secret_access_key = <acces_key_secret>

bucket='ai.pit.exercise'

s3 = boto3.client('s3',region_name='eu-central-1',aws_access_key_id=''
                  ,aws_secret_access_key = '')

content_list = s3.list_objects(Bucket='ai.pit.exercise')['Contents']








        




fileList=[]
for dictt in content_list:
    s3_object=dictt['Key']
    resource = boto3.resource('s3',region_name='eu-central-1',aws_access_key_id=''
                  ,aws_secret_access_key = '')
    my_bucket = resource.Bucket(bucket)
    my_bucket.download_file(s3_object, s3_object)
    fileList.append(dictt['Key'])
    
columns = ['TimeStamp','Time', 'Open','Highs','Lows','Close']


lengthsDict={}
for nyFile in fileList: 
    df_ = pd.DataFrame( columns=columns)
    with gzip.open(nyFile,'r') as fin:
        ii=0
        for line in fin:
            if 'open' not in str(line):
                ii=ii+1
        lengthsDict[nyFile]=ii







readDataframes=[]    
for nyFile in fileList: 
    df_ = pd.DataFrame(index=range(lengthsDict[nyFile]), columns=columns)
    with gzip.open(nyFile,'r') as fin:
        ii=0
        for line in fin:
            if 'open' not in str(line):
               [time,Open,Highs,Lows,Close]=processLine(line)
               df_['Time'][ii]=time
               df_['Open'][ii]=Open
               df_['Highs'][ii]=Highs
               df_['Lows'][ii]=Lows  
               df_['Close'][ii]=Close
               df_['TimeStamp'][ii]= datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
               ii=ii+1
        readDataframes.append(df_)

def reportError(files,frames):
    
    for ii in range(len(files)):
        fileName=files[ii]
        df=frames[ii]
        print('Missing value ratio of '+ fileName+ " is "+ str(countMissingValueRatio(df)))
 
#reportError(fileList,readDataframes)


def insertIntoDatabase(files,frames):
    
    con=psycopg2.connect(database="postgres",user="postgres",password="")
    cur=con.cursor()
    

    
    for ii in range(len(files)):
        fileName=files[ii]
        df=frames[ii]
        colNames=list(df)
        colNames.remove('TimeStamp')
        
        query_TABLE="CREATE TABLE "+fileName.split(".")[0]+"( TIME INTEGER PRIMARY KEY ,OPEN REAL, HIGH REAL, LOWS REAL,CLOSE REAL)"
        print(query_TABLE)
        try:
    
            cur.execute(query_TABLE)
            print(" TABLE ", fileName.split(".")[0]," got created")
        
        except:
            print(" TABLE ",fileName.split(".")[0]," could not get created")
        
        
        for ind in range(len(df)):
            time=str(df[colNames[0]][ind])
            openVal=str(df[colNames[1]][ind])
            highVal=str(df[colNames[2]][ind])
            lowsVal=str(df[colNames[3]][ind])
            closeVal=str(df[colNames[4]][ind])
            
            query_INSERT="INSERT INTO "+fileName.split(".")[0]+" VALUES("+time+" ,"+openVal+" , "+highVal+" , "+lowsVal+" , "+closeVal+" "+")"
            
            try:   
                cur.execute(query_INSERT)
#                print("Got executed")
            except:
                print("Could not get executed")
                print(query_INSERT)
                break
            
#insertIntoDatabase(fileList,readDataframes)        

       

def countMissingValueRatio(frame):
    margin=5
    errorCount=0
    for index in range(len(frame)-1):     
        if frame['Time'][index+1]-frame['Time'][index] !=margin:
            errorCount+=1
            
    return float(errorCount)/len(frame)
            
        
        



def processLine(line):
    line=str(line)
    dicedLine=line.split(',')
    time=int(dicedLine[0][2:len(dicedLine[0])])
    Open=float(dicedLine[1])
    Highs=float(dicedLine[2])
    Lows=float(dicedLine[3])
    Close=float(dicedLine[4][0:-3])
    return[time,Open,Highs,Lows,Close]


app = Flask(__name__)
@app.route("/line_chart")
def line_chart():
    legend = 'Temperatures'
    temperatures = [73.7, 73.4, 73.8, 72.8, 68.7, 65.2,
                    61.8, 58.7, 58.2, 58.3, 60.5, 65.7,
                    70.2, 71.4, 71.2, 70.9, 71.3, 71.1]
    times = ['12:00PM', '12:10PM', '12:20PM', '12:30PM', '12:40PM', '12:50PM',
             '1:00PM', '1:10PM', '1:20PM', '1:30PM', '1:40PM', '1:50PM',
             '2:00PM', '2:10PM', '2:20PM', '2:30PM', '2:40PM', '2:50PM']
    return render_template('line_chart.html', values=temperatures, labels=times, legend=legend)

        
if __name__ == "__main__":
    app.run(debug=True)
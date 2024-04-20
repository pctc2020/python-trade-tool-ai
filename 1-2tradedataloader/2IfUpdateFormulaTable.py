import time
import NSEDownload.stocks as stocks
import unittest
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# mysql connection 
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Root",
  database="stockmarket"
)

toReset=False
default_start_value=1.0
formulaRangeDatas = [ # script, startValue1, endValue, increment
    {"formulaName": "AO",   "startValue1": 2.0,  "endValue1": 100.0, "increment1": default_start_value, "startValue2": 0.0, "endValue2": 50.0, "increment2": default_start_value, 
    "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "fast (0-50), slow (0-50), " },
    {"formulaName": "AROON",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 50.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment": "fast (0-50), slow (0-50)" },
    {"formulaName": "ADX",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": default_start_value, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  " risk (0-50)" },
    {"formulaName": "BOLLINGERBAND",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 50.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment": "length (0-50) and std (0-50)"},
    {"formulaName": "COPPOCK",   "startValue1": default_start_value,  "endValue1": 40.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 45.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": 50.0, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": 10.0, "increment4": default_start_value, "comment":  "length (0-40), fast (0-45), slow (0-50), offset (0-10)" }, 
    {"formulaName": "CCI",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 0.5, "increment2": 0.01, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  " length (0-50) and c (0-0.025)" },
    {"formulaName": "EMA",   "startValue1": default_start_value,  "endValue1": 75.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 75.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "min (0-50), max (0-50)" },
    {"formulaName": "KC",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 50.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "period (0-50), multiple (0-50)" },
      # roc1 =10, roc2 = 15, roc3 = 20, roc4 = 30
      # sma1 = 10, sma2 = 10, sma3 = 10, sma4 = 15
    {"formulaName": "KST",   "startValue1": 10015020030.0,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": 10010010015.0,  
     "endValue2": 50.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "sme1to3 (1001001001-100100100100), roc1to3 (1001001001-100100100100), signal=(0-50)" },
    {"formulaName": "MACD",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 50.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": 50.0, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": 0.0, "increment4": default_start_value, "comment": "short (0-50), long (0-50), singal (0-50)"},
    {"formulaName": "RSI",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": default_start_value, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value,  "comment": "length (0-50)"},
    {"formulaName": "RVI",   "startValue1": 14.0,  "endValue1": 75.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 100.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "period (0-50), signal (0-50)" },
    {"formulaName": "SMA",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 50.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment": "signal1 (0-50), and signal2 (0-50)" },
    {"formulaName": "STOCH",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 50.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": 50.0, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "fastk (0-50), slowk (0-50), slowd (0-50)" },
    {"formulaName": "ST",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 20.0, "increment2": 0.1, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "length (0-50), multiplier (0.1-20.0)" },
    {"formulaName": "TSI",   "startValue1": default_start_value,  "endValue1": 50.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": 50.0, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "fast (0-50), slow (0-50), signal (0-50)," },      
    {"formulaName": "WILLR",   "startValue1": 14.0,  "endValue1": 100.0, "increment1": default_start_value, "startValue2": default_start_value,  
     "endValue2": default_start_value, "increment2": default_start_value, "startValue3": default_start_value,  "endValue3": default_start_value, "increment3": default_start_value,
      "startValue4": default_start_value,  "endValue4": default_start_value, "increment4": default_start_value, "comment":  "period (0-100)" }      
  ]

def get_latest_formula_row(formulaName): 
    mycursor = mydb.cursor()
    mycursor.execute("select * from formula_range_to_apply WHERE id=(SELECT MAX(id) FROM `formula_range_to_apply`) AND formula_name='"+formulaName+"'")
    firstRow = mycursor.fetchone()
    if(firstRow==None): 
        firstRow = [0, formulaName, default_start_value, default_start_value, default_start_value, default_start_value, default_start_value]
    return firstRow

def update_formula(): 
    mycursor = mydb.cursor()
    maxchunksize=25000
    counter=0
    for formulaRangeData in formulaRangeDatas: 
        latestdata = get_latest_formula_row(formulaRangeData['formulaName'])
        print(formulaRangeData['formulaName'], formulaRangeData['startValue1'], formulaRangeData['startValue2'], 
              formulaRangeData['startValue3'], formulaRangeData['startValue4'], formulaRangeData['increment1'], 
              formulaRangeData['increment2'], formulaRangeData['increment3'], formulaRangeData['increment4'],
              formulaRangeData['endValue1'], formulaRangeData['endValue2'], formulaRangeData['endValue3'], 
              formulaRangeData['endValue4'])
        v1val = float(latestdata[2])
        if(v1val<float(formulaRangeData['startValue1'])): 
            v1val = float(formulaRangeData['startValue1'])

        v2val = float(latestdata[3])
        if(v2val<float(formulaRangeData['startValue2'])): 
            v2val = float(formulaRangeData['startValue2'])
            
        v3val = float(latestdata[4])
        if(v3val<float(formulaRangeData['startValue3'])): 
            v3val = float(formulaRangeData['startValue3'])
            
        v4val = float(latestdata[5])
        if(v4val<float(formulaRangeData['startValue4'])): 
            v4val = float(formulaRangeData['startValue4'])
            
        v5val = float(latestdata[6])

        sqlQry="..."
        v1 = v1val
        while((v1<=formulaRangeData['endValue1']) & (maxchunksize>counter)): 
            v2 = v2val
            while((v2<=formulaRangeData['endValue2'])  & (maxchunksize>counter)): 
                v3 = v3val
                while((v3<=formulaRangeData['endValue3'])  & (maxchunksize>counter)): 
                    v4 = v4val
                    while((v4<=formulaRangeData['endValue4']) & (maxchunksize>counter)): 
                        counter = counter+1
                        v1val = formulaRangeData['startValue1']
                        v2val = formulaRangeData['startValue2']
                        v3val = formulaRangeData['startValue3']
                        v4val = formulaRangeData['startValue4']
                        
                        sqlQry  = "insert into formula_range_to_apply values( NULL, '"+formulaRangeData['formulaName']+"', "
                        sqlQry+=str(v1)+", "+str(v2)+", "+str(v3)+", "+str(v4)+" , -1.0, 'NOT_STARTED' )"
                        try: 
                            if( (v1==float(latestdata[2]) ) & (v2==(latestdata[3])) 
                               & (v3==float(latestdata[4])) & (v4==float(latestdata[5])) ): 
                              print("Ignoring ")
                            else:
                              mycursor.execute(sqlQry)
                        except Exception as error: 
                            print("Err in ", error)
                        v4 = v4 + formulaRangeData['increment1']
                    v3 = v3 + formulaRangeData['increment1']
                    print(sqlQry)
                    mydb.commit()
                v2 = v2 + formulaRangeData['increment1']
            v1 = v1 + formulaRangeData['increment1']


update_formula()



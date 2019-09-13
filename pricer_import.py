#! /usr/bin/env python
import pandas as pd
import numpy as np
import datetime as dt
import os
import re
import pdb
import psycopg2
#from openpyxl import worksheet
#from openpyxl import load_workbook
#from openpyxl.utils.dataframe import dataframe_to_rows

class Budget:
    def __init__(self, budget_file):
        self.budget_file=budget_file
        self.pricer=None
        self.task_info=None
        self.sub_info=None
        self.labor_info=None
        self.travel_info=None
        self.expenses_info=None


    def parse_task_info(self):
        task_text = self.pricer.parse(sheet_name="Task Order Setup", header=None, usecols="C",
                nrows=3)
        task_info = {'agree_no':re.findall('\d{5}[A-Z]', task_text.iloc[1, 0]),
            'proj_no':re.findall('\d{3}$', task_text.iloc[1, 0]),
            'task_no':re.findall('\d+', task_text.iloc[0, 0])[0],
            'task_name':task_text.iloc[0, 0].split(":")[1].strip(),
            'mod_no':re.findall('\d+', task_text.iloc[0, 0])[1],
            'start_date':dt.datetime.strptime(re.findall('\d{2}/\d{2}/\d{2}', task_text.iloc[2, 0])[0],
                '%m/%d/%y'),
            'end_date':dt.datetime.strptime(re.findall('\d{2}/\d{2}/\d{2}', task_text.iloc[2, 0])[1],
                '%m/%d/%y')}
        return task_info

    def parse_sub_info(self):
        sub_info = self.pricer.parse(sheet_name="Task Order Setup", header=None, skiprows=6,
                usecols="B,C", names=["sub_no", "sub_name"])
        sub_info.dropna(how='any', inplace=True)
        sub_info = sub_info.loc[[not x=='.' for x in sub_info['sub_name']], :]
        return(sub_info)

    def parse_labor(self):
        labor = self.pricer.parse(sheet_name="Labor Hours and Deliverables", header=None, skiprows=7,
                usecols="A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z")

        personnel_end_index = labor.iloc[0, :].tolist().index('-')
        personnel_list = labor.iloc[0, 5:personnel_end_index].tolist()

        labor = labor.iloc[0:labor.iloc[:, 2].tolist().index('.')-3, 0:personnel_end_index]
        labor = labor.loc[[not x==0 for x in labor.loc[:, 4]], :]
        labor = labor.loc[[not np.isnan(x) for x in labor.iloc[:, 1]]]
        labor.iloc[:, 0] = [int(float(x)) for x in labor.iloc[:, 1]]

        labor_info = pd.DataFrame({'sub_no':labor.iloc[:, 0], 'work_no':labor.iloc[:, 1],
            'work_name':labor.iloc[:, 2], 'deliv_name':labor.iloc[:, 3],
            'staff_hours':['NULL' for x in labor.iloc[:, 0]]})
        labor_info = labor_info.reset_index().drop('index', axis=1)
        for j in range(0, labor.shape[0]):
            hours = labor.iloc[j, 5:personnel_end_index]
            labor_info.at[j, 'staff_hours'] = {x:y for x,y in zip(personnel_list, hours) if not np.isnan(y)}

        return(labor_info)

    def parse_travel(self):
        travel = self.pricer.parse(sheet_name="Travel", header=None, skiprows=22, usecols="A,B,D,U")
        travel.dropna(how='any', inplace=True)
        travel.columns = ['travel_no', 'company', 'desc', 'cost']
        travel['sub_no'] = [int(float(x)) for x in travel['travel_no']]

    def parse_expenses(self):
        expenses = self.pricer.parse(sheet_name="Expenses", header=None, skiprows=18, usecols="A,B,D,G")
        expenses.dropna(how='any', inplace=True)
        expenses.columns = ['expense_no', 'company', 'desc', 'cost']
        expenses['sub_no'] = [int(float(x)) for x in expenses['expense_no']]

    def pull_pricer(self):

        self.pricer = pd.ExcelFile(os.getcwd() + "/" + self.budget_file)

        self.task_info = self.parse_task_info()

        self.sub_info = self.parse_sub_info()

        self.labor_info = self.parse_labor()

        self.travel_info = self.parse_travel()

        self.expenses_info = self.parse_expenses()

class Rates:
    def __init__(self, rate_file):
        self.rate_file=rate_file
        self.rates=None
        self.rate_info=None
        self.staff_levels=None
        self.level_rates=None

        self.conn = psycopg2.connect(user='airsci', host='localhost', port='5432', database='cost_control')
        self.cursor = self.conn.cursor()

    def parse_rate_info(self):
        rate_text = self.rates.parse(sheet_name="Rates", header=None, skiprows=2, usecols="B", nrows=2)
        rate_info = {'company':rate_text.iloc[0, 0].split(":")[1].strip(),
            'effect_date':dt.datetime.strptime(re.findall('\d{2}/\d{2}/\d{2}', rate_text.iloc[1, 0])[0], '%m/%d/%y')}
        comp_abrv = "".join([word[0] for word in rate_info['company'].split()])
        rate_info['sheet_code'] = comp_abrv + "-" + rate_info['effect_date'].strftime("%y%m%d")
        return rate_info

    def parse_staff_levels(self):
        staff_levels = self.rates.parse(sheet_name="Rates", header=None, skiprows=7, usecols="A,B")
        staff_levels.dropna(how='any', inplace=True)
        staff_levels.columns = ['name', 'level']
        return(staff_levels)

    def parse_level_rates(self):
        level_rates = self.rates.parse(sheet_name="Rates", header=None, skiprows=7, usecols="D,E")
        level_rates.dropna(how='any', inplace=True)
        level_rates.columns = ['level', 'rate']
        return(level_rates)

    def pull_rates(self):

        self.rates = pd.ExcelFile(os.getcwd() + "/" + self.rate_file)

        self.rate_info = self.parse_rate_info()
        self.staff_levels = self.parse_staff_levels()
        self.level_rates = self.parse_level_rates()


df1 = pd.read_csv("~/Desktop/dump/comps.csv", header=None)

def insert_row(table, values, columns=None):
    if columns==None:
        sql = "INSERT INTO %s VALUES(%s);" % (table, str(values.tolist())[1:-1])
    else:
        sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, str(columns)[1:-1].replace("\'", ""), str(values)[1:-1])

    try:
        self.cursor.execute(sql)
    except IntegrityError as error:
        print(error)
    
    self.conn.commit()





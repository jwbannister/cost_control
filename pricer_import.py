#! /usr/bin/env python
#import time
import pandas as pd
import numpy as np
import datetime as dt
import os
import re
#from openpyxl import worksheet
#from openpyxl import load_workbook
#from openpyxl.utils.dataframe import dataframe_to_rows
import pdb

class Budgetize:
    def __init__(self, budget_file):
        self.budget_file=budget_file

    def parse_task_info(self):
        task_text = self.pricer.parse(sheet_name="Task Order Setup", header=None, usecols="C",
                nrows=3)
        task_info = pd.DataFrame({
            'agree_no':re.findall('\d{5}[A-Z]', txt.iloc[1, 0]),
            'proj_no':re.findall('\d{3}$', txt.iloc[1, 0]),
            'task_no':re.findall('\d+', txt.iloc[0, 0])[0],
            'task_name':txt.iloc[0, 0].split(":")[1].strip(),
            'mod_no':re.findall('\d+', txt.iloc[0, 0])[1],
            'start_date':dt.datetime.strptime(re.findall('\d{2}/\d{2}/\d{2}', txt.iloc[2, 0])[0],
                '%m/%d/%y'),
            'end_date':dt.datetime.strptime(re.findall('\d{2}/\d{2}/\d{2}', txt.iloc[2, 0])[1],
                '%m/%d/%y')})
        return task_info

    def parse_sub_info(self):
        sub_info = self.pricer.parse(sheet_name="Task Order Setup", header=None, skiprows=6,
                usecols="B,C", names=["sub_no", "sub_name"])
        sub_info.dropna(how='any', inplace=True)
        sub_info = self.sub_info.loc[[not x=='.' for x in self.sub_info['sub_name']], :]
        return(sub_info)

    def parse_labor(self):
        labor = self.pricer.parse(sheet_name="Labor Hours and Deliverables", header=None, skiprows=7,
                usecols="A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z")

        personnel_end_index = labor.iloc[0, :].tolist().index('-')
        personnel_list = labor.iloc[0, 5:personnel_end_index].tolist()

        labor = labor.iloc[0:labor.iloc[:, 2].tolist().index('.')-3, 0:personnel_end_index]
        labor = labor.loc[[not x==0 for x in labor.loc[:, 4]], :]
        labor = labor.loc[[not np.isnan(x) for x in labor.iloc[:, 1]]]
        labor.iloc[:, 0] = [int(x) for x in labor.iloc[:, 1]]

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
        travel['sub_no'] = [int(x) for x in travel['travel_no']]

    def parse_expenses(self):
        expenses = self.pricer.parse(sheet_name="Expenses", header=None, skiprows=18, usecols="A,B,D,G")
        expenses.dropna(how='any', inplace=True)
        expenses.columns = ['expense_no', 'company', 'desc', 'cost']
        expenses['sub_no'] = [int(x) for x in expenses['expense_no']]

    def pull_pricer(self):

        self.pricer = pd.ExcelFile(os.getcwd() + "/" + self.budget_file)

        self.task_info = self.parse_task_info()

        self.sub_info = self.parse_sub_info()

        self.labor_info = self.parse_labor()

        self.travel_info = self.parse_travel()

        self.expenses_info = self.parse_expenses()

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

def insert_row(conn, table, values, columns=None, id_col=None):
    if columns==None:
        sql = "INSERT INTO %s VALUES(%s);" % (table, str(values.tolist())[1:-1])
    elif id_col is None:
        sql = "INSERT INTO %s(%s) VALUES(%s);" % (table,
                str(columns)[1:-1].replace("\'", ""), str(values)[1:-1])
    else:
        sql = "INSERT INTO %s(%s) VALUES(%s) RETURNING %s;" % (table,
                str(columns)[1:-1].replace("\'", ""), str(values)[1:-1], id_col)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        idx = cur.fetchone()[0]
    except Exception as error:
        idx = error
    conn.commit()
    return(idx)

def select_from_db(conn, sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        result = cur.fetchone()[0]
    except Exception as error:
        print(error)
        result = error
    return(result)

def stash_file(conn, file_type_id, file, storage_name):
    storage_path = "/Users/john/code/cost_control/storage/"
    file_name = storage_name.split(".")[0]
    idx = insert_row(conn, 'info.files', [file_type_id, file_name, storage_path + storage_name],
            columns=['file_type_id', 'file_name', 'path'], id_col='file_id')
    os.system("cp %s %s" % (file, storage_path + storage_name))
    return(idx)

class Modification:
    def __init__(self, mod_file):
        self.mod_file=mod_file
        self.mod=None
        self.conn = psycopg2.connect(user='airsci', host='localhost', port='5432',
                database='cost_control')

    def parse_mod_info(self):
        mod_text = self.mod.parse(sheet_name="Task Order Setup", header=None, usecols="C",
                nrows=3)
        start_date = dt.datetime.strptime(re.findall('\d{2}/\d{2}/\d{2}', mod_text.iloc[2, 0])[0],
                '%m/%d/%y')
        end_date = dt.datetime.strptime(re.findall('\d{2}/\d{2}/\d{2}', mod_text.iloc[2, 0])[1],
                '%m/%d/%y')
        mod_info = {'task_no':re.findall('\d+', mod_text.iloc[0, 0])[0],
            'mod_no':re.findall('\d+', mod_text.iloc[0, 0])[1],
            'proj_no':re.findall('\d{3}$', mod_text.iloc[1, 0])[0],
            'name':mod_text.iloc[0, 0].split(":")[1].strip(),
            'start_date':start_date.strftime("%Y-%m-%d"),
            'end_date':end_date.strftime("%Y-%m-%d"),
            'active':True}
        mod_info['stored_file_name'] = "MOD" + mod_info['mod_no'] + "_" + mod_info['proj_no']\
                + "-" + mod_info['task_no']
        return mod_info

    def parse_sub_info(self):
        sub_info = self.mod.parse(sheet_name="Task Order Setup", header=None, skiprows=6,
                usecols="B,C", names=["sub_no", "name"])
        sub_info.dropna(how='any', inplace=True)
        sub_info = sub_info.loc[[not x=='.' for x in sub_info['name']], :]
        sub_info['active'] = True
        return(sub_info)

    def parse_labor(self):
        labor = self.mod.parse(sheet_name="Labor Hours and Deliverables", header=None, skiprows=7,
                usecols="A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z")

        personnel_end_index = labor.iloc[0, :].tolist().index('-')
        personnel_list = labor.iloc[0, 5:personnel_end_index].tolist()

        labor = labor.iloc[0:labor.iloc[:, 2].tolist().index('.')-3, 0:personnel_end_index]
        labor = labor.loc[[not x==0 for x in labor.loc[:, 4]], :]
        labor = labor.loc[[not np.isnan(x) for x in labor.iloc[:, 1]]]
        labor.iloc[:, 0] = [int(float(x)) for x in labor.iloc[:, 1]]

        labor_info = pd.DataFrame({'sub_no':labor.iloc[:, 0], 'work_no':labor.iloc[:, 1],
            'work_name':labor.iloc[:, 2], 'deliv_name':labor.iloc[:, 3],
            'staff_hours':['NULL' for x in labor.iloc[:, 0]],
            'active':[True for x in labor.iloc[:, 0]]})
        labor_info = labor_info.reset_index().drop('index', axis=1)
        for j in range(0, labor.shape[0]):
            hours = labor.iloc[j, 5:personnel_end_index]
            labor_info.at[j, 'staff_hours'] = {x:y for x,y in zip(personnel_list, hours) if not np.isnan(y)}
        return(labor_info)

    def parse_travel(self):
        travel = self.mod.parse(sheet_name="Travel", header=None, skiprows=22, usecols="A,B,D,U")
        travel.dropna(how='any', inplace=True)
        travel.columns = ['travel_no', 'company', 'name', 'cost']
        travel['travel_no'] = [str(x) for x in travel['travel_no']]
        travel['sub_no'] = [int(float(x)) for x in travel['travel_no']]
        return(travel)

    def parse_expenses(self):
        expenses = self.mod.parse(sheet_name="Expenses", header=None, skiprows=18, usecols="A,B,D,G")
        expenses.dropna(how='any', inplace=True)
        expenses.columns = ['expense_no', 'company', 'name', 'cost']
        expenses['sub_no'] = [int(float(x)) for x in expenses['expense_no']]
        return(expenses)

    def ingest_mod(self):
        self.mod = pd.ExcelFile(os.getcwd() + "/" + self.mod_file)

        mod_info = self.parse_mod_info()
        file_id = stash_file(self.conn, file_type_id=2, file=self.mod_file,
                storage_name=mod_info['stored_file_name'])
        mod_values = [mod_info[x] for x in ['proj_no', 'task_no', 'start_date', 'end_date',
            'mod_no']] + [39]
        mod_idx = insert_row(self.conn, 'project.modifications', mod_values,
                columns=['proj_no', 'task_no', 'start_date', 'end_date', 'mod_no', 'file_id'],
                id_col='mod_id')

        task_info = [mod_info[x] for x in ['task_no', 'proj_no', 'name', 'active']]
        task_idx = insert_row(self.conn, 'project.tasks', task_info,
                columns=['task_no', 'project_no', 'name', 'active'],
                id_col='task_id')

        sub_info = self.parse_sub_info()
        sql = "SELECT task_id FROM project.tasks WHERE project_no = '%s' and task_no = '%s'"\
                % (task_info[1], task_info[0])
        task_idx = select_from_db(self.conn, sql)
        sub_info['task_id'] = task_idx
        for row in sub_info.iterrows():
            sub_idx = insert_row(self.conn, 'project.subtasks', row[1].tolist(),
                    columns=['sub_no', 'name', 'active', 'task_id'], id_col='sub_id')

        labor_info = self.parse_labor()
        for row in labor_info.iterrows():
            sql = "SELECT sub_id FROM project.subtasks WHERE sub_no = '%s' and task_id = %s"\
                    % (row[1]['sub_no'], task_idx)
            sub_idx = select_from_db(self.conn, sql)
            work_values = [row[1][x] for x in ['work_no', 'work_name', 'active']] + [sub_idx]
            work_idx = insert_row(self.conn, 'project.work', work_values,
                    columns=['work_no', 'name', 'active', 'sub_id'], id_col='work_id')
            if not pd.isnull(row[1]['deliv_name']):
                deliv_values = [row[1][x] for x in ['deliv_name', 'active']] +\
                        [work_idx, False, 'work']
                deliv_idx = insert_row(self.conn, 'project.deliverables', deliv_values,
                        columns=['name', 'active', 'id', 'complete', 'type'], id_col='deliv_id')

                hours_dict = [row[1][x] for x in ['staff_hours']]

        travel_info = self.parse_travel()
        for row in travel_info.iterrows():
            sql = "SELECT sub_id FROM project.subtasks WHERE sub_no = '%s' and task_id = %s"\
                    % (row[1]['sub_no'], task_idx)
            sub_idx = select_from_db(self.conn, sql)
            travel_values = [row[1][x] for x in ['travel_no', 'name']] + [sub_idx, True]
            travel_idx = insert_row(self.conn, 'project.travel', travel_values,
                    columns=['travel_no', 'name', 'sub_id', 'active'], id_col='travel_id')
            deliv_values = [row[1][x] for x in ['name']] + [travel_idx, False, 'travel', True]
            deliv_idx = insert_row(self.conn, 'project.deliverables', deliv_values,
                    columns=['name', 'id', 'complete', 'type', 'active'], id_col='deliv_id')

        expenses_info = self.parse_expenses()
        for row in expenses_info.iterrows():
            sql = "SELECT sub_id FROM project.subtasks WHERE sub_no = '%s' and task_id = %s"\
                    % (row[1]['sub_no'], task_idx)
            sub_idx = select_from_db(self.conn, sql)
            expenses_values = [row[1][x] for x in ['expense_no', 'name']] + [sub_idx, True]
            expense_idx = insert_row(self.conn, 'project.expenses', expenses_values,
                    columns=['expense_no', 'name', 'sub_id', 'active'], id_col='expense_id')
            deliv_values = [row[1][x] for x in ['name']] + [expense_idx, False, 'expense', True]
            deliv_idx = insert_row(self.conn, 'project.deliverables', deliv_values,
                    columns=['name', 'id', 'complete', 'type', 'active'], id_col='deliv_id')

class Rates:
    def __init__(self, rate_file):
        self.rate_file=rate_file
        self.rates=None
        self.conn = psycopg2.connect(user='airsci', host='localhost', port='5432',
                database='cost_control')
        self.company_info = pd.read_sql_query("SELECT * FROM info.companies;", self.conn)

    def parse_rate_info(self):
        rate_text = self.rates.parse(sheet_name="Rates", header=None, skiprows=3, usecols="B", nrows=2)
        effect_date = dt.datetime.strptime(re.findall('\d{2}/\d{2}/\d{2}',
            rate_text.iloc[1, 0])[0], '%m/%d/%y')
        rate_info = {'company':rate_text.iloc[0, 0].split(":")[1].strip()}
        if rate_info['company'] not in self.company_info['name'].tolist():
            return("ERROR: Company name on rate sheet not found in company master list")
        else:
            comp_abrv = self.company_info.loc[self.company_info['name']==rate_info['company'],'abrv'].item()
            rate_info['effect_date'] = effect_date.strftime("%Y-%m-%d")
            rate_info['stored_file_name'] = comp_abrv + "-" + effect_date.strftime("%y%m%d") + ".xlsx"
            return rate_info

    def parse_staff_rates(self):
        staff_rates = self.rates.parse(sheet_name="Rates", header=None, skiprows=9, usecols="A,B,C")
        staff_rates.dropna(how='any', inplace=True)
        staff_rates.columns = ['name', 'level', 'rate']
        return(staff_rates)

    def parse_level_rates(self):
        level_rates = self.rates.parse(sheet_name="Rates", header=None, skiprows=7, usecols="D,E")
        level_rates.dropna(how='any', inplace=True)
        level_rates.columns = ['level', 'rate']
        return(level_rates)

    def process_rates(self):
        self.rates = pd.ExcelFile(os.getcwd() + "/" + self.rate_file)

        rate_info = self.parse_rate_info()
        company_id = self.company_info.loc[self.company_info['name']==\
                rate_info['company'],'company_id'].item()
        file_id = stash_file(self.conn, file_type_id=1, file=self.rate_file,
                storage_name=rate_info['stored_file_name'])

        staff_rates = self.parse_staff_rates()
        staff_rates['company_id'] = company_id
        staff_rates['effect_date'] = rate_info['effect_date']
        staff_rates['file_id'] = int(file_id)
        for i in staff_rates.iterrows():
            values = i[1][0:6].tolist()
            idx = insert_row(self.conn, 'info.staff', values,
                    columns=['name', 'level', 'rate', 'company_id', 'effective_date', 'file_id'],
                    id_col='staff_id')

    def get_current_rate_sheet(self):
        sql = """WITH latest_date AS
                     (SELECT name, MAX(effective_date) AS effective_date
                     FROM info.staff GROUP BY name)
                 SELECT s1.name, s2.level, c.name, s2.rate, cc.abrv, f.file_name
                 FROM latest_date s1
                 JOIN info.staff s2 ON s1.name=s2.name AND s1.effective_date=s2.effective_date
                 JOIN info.companies c ON s2.company_id=c.company_id
                 JOIN info.company_classes cc ON c.class_id=cc.class_id
                 JOIN info.files f on s2.file_id=f.file_id;"""
        rate_sheet = pd.read_sql_query(sql, self.conn)
        rate_sheet.columns = ['Staff Name', 'Professional Level', 'Company', 'Rate',
                'Company Class', 'Rate Sheet']
        storage_path = "/Users/john/code/cost_control/storage/current_rate_sheet.csv"
        rate_sheet.to_csv(storage_path, index=False)
        return(storage_path)





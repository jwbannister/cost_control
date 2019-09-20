import psycopg2
conn = psycopg2.connect(user='airsci', host='localhost', port='5432', database='cost_control')
cur = conn.cursor()

tables = ['budget.labor', 'budget.trips', 'budget.purchases', 'project.deliverables',
        'project.work', 'project.travel', 'project.expenses',
        'project.subtasks', 'project.tasks', 'budget.events', 'info.files', 'info.staff']

for tb in tables:
    sql = "DELETE FROM %s WHERE 1=1;" % tb
    cur.execute(sql)
    conn.commit()

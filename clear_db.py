import psycopg2
conn = psycopg2.connect(user='airsci', host='localhost', port='5432', database='cost_control')
cur = conn.cursor()

tables = ['project.deliverables', 'project.work', 'project.travel', 'project.expenses', 'project.modifications', 'project.projects',
        'project.subtasks', 'project.tasks']

for tb in tables:
    sql = "DELETE FROM %s WHERE 1=1;" % tb
    cur.execute(sql)
    conn.commit()
    

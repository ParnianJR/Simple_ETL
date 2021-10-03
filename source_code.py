import psycopg2
from psycopg2 import Error, connect
import pandas as pd
import networkx as nx


def get_column_names(table_name, conn):
    col_cursor = conn.cursor()
    query = """
    SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{}'
    ORDER BY ordinal_position
    """.format(table_name)
    col_cursor.execute(query)
    raw_columns = col_cursor.fetchall()
    raw_columns = list(raw_columns)
    columns = []
    for i in raw_columns:
        n = str(i)
        n = n.replace("(","")
        n = n.replace(")","")
        n = n.replace("," , "")
        n = n.replace("'","")
        columns.append(n)
    return columns

#python postgres connection:


try:
    user1 = input("please enter origin database user:")
    password1 = input("please enter origin database password:")
    host1 = input("please enter origin database host:")
    port1 = input("please enter origin database port")
    database1 = input("please enter origin database database:")

    user2 = input("please enter destination database user:")
    password2 = input("please enter destination database password:")
    host2 = input("please enter destination database host:")
    port2 = input("please enter destination database port")
    database2 = input("please enter destination database database:")

    # Connect to origin database
    connection = psycopg2.connect(user=user1,
                                  password=password1,
                                  host=host1,
                                  port=port1,
                                  database=database1)

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    #get table names from database:
    name_query = """SELECT table_name FROM information_schema.tables 
        WHERE table_schema='public' ;"""
    cursor.execute(name_query)
    names1 = cursor.fetchall()
    table_names = []
    for i in names1:
        n = str(i)
        n = n.replace("(","")
        n = n.replace(")","")
        n = n.replace(',','')
        n = n.replace("'","")
        table_names.append(n)
    
    #find table primary keys:
    pk_dict = dict()
    for l_name in table_names:
        table_name = l_name
        PK_Query = """SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
        AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{}'::regclass
        AND    i.indisprimary;""".format(table_name)
        cursor.execute(PK_Query)
        pk = cursor.fetchall()
        n = str(pk)
        n = n.replace("(","")
        n = n.replace(")","")
        n = n.replace(',','')
        n = n.replace("'","")
        n = n.replace("[","")
        n = n.replace("]","")
        pk_dict[table_name] = n
    
    #find foreign keys and foreign tables for each table:
    table_list = []
    foreign_key = []
    foreign_table = []
    for table_name in table_names:
        FK_Query = """
       SELECT
        tc.table_name, 
        kcu.column_name, 
        ccu.table_name AS foreign_table_name

    FROM 

        information_schema.table_constraints AS tc 

        JOIN information_schema.key_column_usage AS kcu

          ON tc.constraint_name = kcu.constraint_name

          AND tc.table_schema = kcu.table_schema

        JOIN information_schema.constraint_column_usage AS ccu

          ON ccu.constraint_name = tc.constraint_name

          AND ccu.table_schema = tc.table_schema

    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{}';
        """.format(table_name)

        cursor.execute(FK_Query)
        fk = cursor.fetchall()
        for i in fk:
            n = str(i)
            n = n.replace("(","")
            n = n.replace(")","")
            n = n.replace(',','')
            n = n.replace("'","")
            n = n.replace("[","")
            n = n.replace("]","")
            lis2 = n.split(' ')
            table_list.append(lis2[0])
            foreign_key.append(lis2[1])
            foreign_table.append(lis2[2])


    set_table_names = set(table_names)
    
    set_table_list = set(table_list)
    set_foreign_table = set(foreign_table)
    set_independent_tables =  set_table_names - (set_table_list | set_foreign_table)
    
    list_independent_tables = list(set_independent_tables)
    
    #create a DAG that vertices represent tables and edges represent dependency

    DAG = nx.DiGraph()
    for i,it in enumerate(table_list):
        DAG.add_edges_from([(table_list[i] , foreign_table[i])])
        
    topologic_list =  list(reversed(list(nx.topological_sort(DAG))))

    #connect to destination database

    connection2 = psycopg2.connect(user=user2,
                                  password=password2,
                                  host=host2,
                                  port=port2,
                                  database=database2)

    cursor2 = connection2.cursor()

    
    
    
    
    #Doing updates on independent tables:

    for i in list_independent_tables :
        #load table i from origin database:

        select_query = """
        SELECT * FROM {} ;
        """.format(i)

        cursor.execute(select_query)
        table1 = cursor.fetchall()
        
        t1_columns = get_column_names(i , connection)
        df1 = pd.DataFrame(table1 , columns= t1_columns)
        #load table i from destination database

        select_query = """
        SELECT * FROM {} ;
        """.format(i)

        cursor2.execute(select_query)
        table2 = cursor2.fetchall()
        t2_columns = get_column_names(i,connection2)
        df2 = pd.DataFrame(table2 , columns=t2_columns)

        col1 = df1[pk_dict[i]]
        col2 = df2[pk_dict[i]]


        col1 = col1.to_frame()
        col2 = col2.to_frame()

        delete_diff = col2.merge(col1, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
        insert_diff = col1.merge(col2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
        
        del delete_diff["_merge"]
        del insert_diff["_merge"]
        
        if delete_diff.empty and insert_diff.empty: #update
            diffrence2 = df2.merge(df1, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
            del diffrence2["_merge"]
            if not diffrence2.empty:#delete
                pk_diff2 = diffrence2[pk_dict[i]]
                pk_diff2 = pk_diff2.to_frame()
                for index , row in pk_diff2.iterrows():
                    l = row.values.tolist()
                    new_l = l[0]
                    delete_query = """
                    delete from {} where {} = '{}';
                    """.format(i , pk_dict[i] , new_l[0])
                    cursor2.execute(delete_query)
                    connection2.commit()

            difference1 = df1.merge(df2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
            if not difference1.empty:#insert
                for index,row in difference1.iterrows():
                    p = list(row)
                    insert_row = df1[df1['{}'.format(pk_dict[i])] == p[0]]
                    l = insert_row.values.tolist()
                    temp = lambda n:'('+','.join('%s' for i in range(n))+')'
                    table_names = i
                    t = tuple(l[0])
                    query = """
                    insert into {} values {} ;
                    """.format(i , temp(len(t)))
                    cursor2.execute(query,t)
                    connection2.commit()


        else: #delete or insert
            if not delete_diff.empty : #delete
                for index , row in delete_diff.iterrows():
                    new_l = []
                    l = row.values.tolist()
                    new_l = l[0]
                    delete_query = """
                    delete from {} where {} = '{}' ;
                    """.format(i , pk_dict[i] , new_l[0])
                    cursor2.execute(delete_query)
                    connection2.commit()
            
            else: #insert
                print(insert_diff)
                
                for index, row in insert_diff.iterrows():
                    p = list(row)
                    insert_row = df1[df1['{}'.format(pk_dict[i])] == p[0]]
                    l = insert_row.values.tolist()
                    temp = lambda n:'('+','.join('%s' for i in range(n))+')'
                    table_names = i
                    t = tuple(l[0])
                    query = """
                    insert into {} values {} ;
                    """.format(i , temp(len(t)))
                    cursor2.execute(query,t)
                    connection2.commit()

    #topologic_list (inset):

    for i in topologic_list:
        select_query = """
        SELECT * FROM {} ;
        """.format(i)

        cursor.execute(select_query)
        table1 = cursor.fetchall()
        
        t1_columns = get_column_names(i , connection)

        df1 = pd.DataFrame(table1 , columns= t1_columns)

        select_query = """
        SELECT * FROM {} ;
        """.format(i)

        cursor2.execute(select_query)
        table2 = cursor2.fetchall()
        df2 = pd.DataFrame(table2 , columns=t1_columns)

        col1 = df1[pk_dict[i]]
        col2 = df2[pk_dict[i]]

        col1 = col1.to_frame()
        col2 = col2.to_frame()
        
        insert_diff = col1.merge(col2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
        del insert_diff["_merge"]

        if not insert_diff.empty : #insert
            for index, row in insert_diff.iterrows():
                p = list(row)
                insert_row = df1[df1['{}'.format(pk_dict[i])] == p[0]]
                l = insert_row.values.tolist()
                temp = lambda n:'('+','.join('%s' for i in range(n))+')'
                table_names = i
                t = tuple(l[0])
                query = """
                insert into {} values {} ;
                """.format(i , temp(len(t)))
                cursor2.execute(query,t)
                connection2.commit()


    #reverse_topologic_list (delete , update):


    for i in reversed(topologic_list): #delete , update
        select_query = """
        SELECT * FROM {} ;
        """.format(i)

        cursor.execute(select_query)
        table1 = cursor.fetchall()
        
        t1_columns = get_column_names(i , connection)

        df1 = pd.DataFrame(table1 , columns= t1_columns)

        select_query = """
        SELECT * FROM {} ;
        """.format(i)

        cursor2.execute(select_query)
        table2 = cursor2.fetchall()
        df2 = pd.DataFrame(table2 , columns=t1_columns)

        col1 = df1[pk_dict[i]]
        col2 = df2[pk_dict[i]]

        col1 = col1.to_frame()
        col2 = col2.to_frame()
        
        delete_diff = col2.merge(col1, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
        
        del delete_diff["_merge"]
        
        if not delete_diff.empty : #delete
            for index , row in delete_diff.iterrows():
                new_l = []
                l = row.tolist()
                new_l = l[0]
                delete_query = """
                delete from {} where {} = '{}' ;
                """.format(i , pk_dict[i] , new_l[0])
                cursor2.execute(delete_query)
                connection2.commit()

        else : #update
            diffrence2 = df2.merge(df1, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
            del diffrence2["_merge"]
            
            if not diffrence2.empty:
                pk_diff2 = diffrence2[pk_dict[i]]
                pk_diff2 = pk.diff2.to_frame()
                for index , row in pk_diff2.iterrows():
                    l = row.tolist()
                    new_l = l[0]
                    delete_query = """
                    delete from {} where {} = '{}';
                    """.format(i , pk_dict[i] , new_l[0])
                    cursor2.execute(delete_query)
                    connection2.commit()


            difference1 = df1.merge(df2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
            if not difference1.empty:
                for index, row in insert_diff.iterrows():
                    p = list(row)
                    insert_row = df1[df1['{}'.format(pk_dict[i])] == p[0]]
                    l = insert_row.values.tolist()
                    temp = lambda n:'('+','.join('%s' for i in range(n))+')'
                    table_names = i
                    t = tuple(l[0])
                    query = """
                    insert into {} values {} ;
                    """.format(i , temp(len(t)))
                    cursor2.execute(query,t)
                    connection2.commit()


    if(connection2):
        cursor2.close()
        connection2.close()
        print("PostgreSQL Destination connection is closed")


except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL : ", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL Origin connection is closed")
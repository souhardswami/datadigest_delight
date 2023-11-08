from db.db_config import connection as conn

def get_next_report_id():
    try:
        cursor = conn.cursor()
        sql_query = "SELECT report_id FROM report_detail ORDER BY report_id DESC LIMIT 1;"
        cursor.execute(sql_query)
        last_id = cursor.fetchone()
        if last_id is not None:
            return last_id[0] + 1  

    except Exception as e:
        print(f'Error while fetching get_next_report_id: {e}')
    
    return 1
    
    
def add_new_report(report_id):
    try:
        cursor = conn.cursor()
        sql_query = f"INSERT INTO report_detail (report_id, progress, file_path) values({report_id}, 0, '');"
        cursor.execute(sql_query)
        conn.commit()
        
    except Exception as e:
        print (f'Error while fetching add_new_report :  {e}')
    
    finally:
        cursor.close()

def get_file_path(report_id):
    try:
        cursor = conn.cursor()
        sql_query = f"SELECT file_path, progress FROM report_detail where report_id = '{report_id}';"
        cursor.execute(sql_query)
        file_path, progress = cursor.fetchone()
        return [file_path, progress]

    except Exception as e:
        print(f'Error while fetching get_file_path: {e}')
    
        return [None, None]
    
    
    
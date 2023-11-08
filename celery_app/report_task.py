from db.db_config import connection as conn
from celery_app.report import get_uptime_downtime_facade as get_report
import csv

def get_list_of_all_store_ids():
    try:
        cursor = conn.cursor()
        
        # Assuming business_hours table is source of truth for all the stores registerations.
        sql_query = "SELECT DISTINCT store_id FROM business_hours;"
        cursor.execute(sql_query)

        store_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return store_ids
    
    except Exception as e:
        print (f'Error while fetching store ids :  {e}')
        return []
    
def update_report(report_id, progress):
    try:
        cursor = conn.cursor()
        sql_query = f"UPDATE report_detail set progress = {progress} WHERE report_id = {report_id}"
        cursor.execute(sql_query)
        conn.commit()
    
    except Exception as e:
        print (f'Error while update_report :  {e}')
    
    finally:
        cursor.close
    
def get_random_file_path(report_id):
    return f'file_path{str(report_id)}.csv'

def update_file_path(report_id, file_path):
    try:
        cursor = conn.cursor()
        sql_query = f"UPDATE report_detail set file_path = '{file_path}' WHERE report_id = '{report_id}'"
        cursor.execute(sql_query)
        conn.commit()
    
    except Exception as e:
        print (f'Error while update_report :  {e}')
    
    finally:
        cursor.close
    
    

def report_task(report_id):
    temp_file_path = get_random_file_path(report_id)
    update_file_path(report_id, temp_file_path)
    with open('reports/'+ temp_file_path, 'w', newline='') as csv_file:
        fieldnames = ['Store ID', 'Uptime (minutes)', 'Uptime (hours)', 'Uptime (days)', 'Downtime (minutes)', 'Downtime (hours)', 'Downtime (days)']

        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()  
        store_ids = get_list_of_all_store_ids()
        for index, store_id in enumerate(store_ids):
            times = get_report(store_id) 
            
            uptime_minutes, uptime_hours, uptime_days, downtime_minutes, downtime_hours, downtime_days = times
            writer.writerow({
                    'Store ID': store_id,
                    'Uptime (minutes)': uptime_minutes,
                    'Uptime (hours)': uptime_hours,
                    'Uptime (days)': uptime_days,
                    'Downtime (minutes)': downtime_minutes,
                    'Downtime (hours)': downtime_hours,
                    'Downtime (days)': downtime_days
            })
            
            progress = int(((index + 1) / len(store_ids)) * 100)
            
            if index % 100 == 0:
                update_report(report_id, progress)
      
    # Handle edge case.  
    update_report(report_id, 100)
    print (f"Task is done for {report_id}")

    
    
    
    
    
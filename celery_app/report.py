from datetime import datetime, timedelta
import pytz
from db.db_config import connection as conn

sql_query_for_last_hour = """
    SELECT ss.timestamp_utc, ss.status, tz.timezone_str
    FROM store_status ss
    JOIN timezones tz ON ss.store_id = tz.store_id
    LEFT JOIN business_hours bh ON ss.store_id = bh.store_id
        AND DAYOFWEEK(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) = bh.day
    WHERE ss.store_id = %s
        AND ss.timestamp_utc >= UTC_TIMESTAMP() - INTERVAL 1 HOUR
        AND ss.timestamp_utc <= UTC_TIMESTAMP()
        AND (
            bh.start_time_local IS NULL  
            OR (
            TIME(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) >= bh.start_time_local
            AND TIME(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) <= bh.end_time_local
            )
    )
    ORDER BY ss.timestamp_utc ASC;
"""

sql_query_for_last_day = """
    SELECT ss.timestamp_utc, ss.status, tz.timezone_str
    FROM store_status ss
    JOIN timezones tz ON ss.store_id = tz.store_id
    LEFT JOIN business_hours bh ON ss.store_id = bh.store_id
        AND DAYOFWEEK(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) = bh.day
    WHERE ss.store_id = %s
      AND ss.timestamp_utc >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
      AND ss.timestamp_utc <= UTC_TIMESTAMP()
      AND (
        bh.start_time_local IS NULL 
        OR (
          TIME(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) >= bh.start_time_local
          AND TIME(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) <= bh.end_time_local
        )
      )
    ORDER BY ss.timestamp_utc ASC;
"""

sql_query_for_last_week = """
    SELECT ss.timestamp_utc, ss.status, tz.timezone_str
    FROM store_status ss
    JOIN timezones tz ON ss.store_id = tz.store_id
    LEFT JOIN business_hours bh ON ss.store_id = bh.store_id
        AND DAYOFWEEK(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) = bh.day
    WHERE ss.store_id = %s
      AND ss.timestamp_utc >= UTC_TIMESTAMP() - INTERVAL 7 DAY
      AND ss.timestamp_utc <= UTC_TIMESTAMP()
      AND (
        bh.start_time_local IS NULL
        OR (
          TIME(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) >= bh.start_time_local
          AND TIME(CONVERT_TZ(ss.timestamp_utc, 'UTC', tz.timezone_str)) <= bh.end_time_local
        )
      )
    ORDER BY ss.timestamp_utc ASC;
"""

def get_uptime_downtime_in_last_hour(store_id):
    try:
        current_time_utc = datetime.now()
        start_time_last_hour_utc = current_time_utc - timedelta(hours=1)
        cursor = conn.cursor()
        cursor.execute(sql_query_for_last_hour, (store_id,))
        data_points = cursor.fetchall()
        
        uptime_minutes, downtime_minutes = 0, 0
        if len(data_points) > 1:
            for i in range(len(data_points) - 1):
                current_time_utc, current_status, timezone_str = data_points[i]
                next_time_utc, next_status, _ = data_points[i + 1]

                if current_status == 'Active':
                    uptime_minutes += (next_time_utc - current_time_utc).seconds // 60
                else:
                    downtime_minutes += (next_time_utc - current_time_utc).seconds // 60

        cursor.close()
        return [uptime_minutes, downtime_minutes]

    except Exception as e:
        print (f"error while getting get_uptime_downtime_in_last_hour {e}")
        return [-1, -1]
    
def get_uptime_downtime_in_last_day(store_id):
    try:
        current_time_utc = datetime.now()
        start_time_last_hour_utc = current_time_utc - timedelta(hours=24)
        cursor = conn.cursor()
        cursor.execute(sql_query_for_last_day, (store_id,))
        data_points = cursor.fetchall()
        
        uptime_hours, downtime_hours = 0, 0
        
        if len(data_points) > 1:
            for i in range(len(data_points) - 1):
                current_time_utc, current_status, timezone_str = data_points[i]
                next_time_utc, next_status, _ = data_points[i + 1]

                if current_status == 'Active':
                    uptime_hours += (next_time_utc - current_time_utc).seconds / 3600
                else:
                    downtime_hours += (next_time_utc - current_time_utc).seconds / 3600

        cursor.close()
        return [uptime_hours, downtime_hours]

    except Exception as e:
        print (f"error while getting get_uptime_downtime_in_last_day {e}")
        return [-1, -1]
    
def get_uptime_downtime_in_last_week(store_id):
    try:
        current_time_utc = datetime.now()
        start_time_last_hour_utc = current_time_utc - timedelta(hours=24)
        cursor = conn.cursor()
        cursor.execute(sql_query_for_last_day, (store_id,))
        data_points = cursor.fetchall()
        
        uptime_days, downtime_days = 0, 0
        
        if len(data_points) > 1:
            for i in range(len(data_points) - 1):
                current_time_utc, current_status, timezone_str = data_points[i]
                next_time_utc, next_status, _ = data_points[i + 1]

                if current_status == 'Active':
                    uptime_days += (next_time_utc - current_time_utc).seconds / 86400
                else:
                    downtime_days += (next_time_utc - current_time_utc).seconds / 86400

    
        cursor.close()
        return [uptime_days, downtime_days]

    except Exception as e:
        print (f"error while getting get_uptime_downtime_in_last_week {e}")
        return [-1, -1]
    
    
    
def get_list_of_all_store_ids():
    try:
        cursor = conn.cursor()
        
        #  Assuming business_hours table is source of truth for all the stores registerations.
        sql_query = "SELECT DISTINCT store_id FROM business_hours;"
        cursor.execute(sql_query)

        store_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return store_ids
    
    except Exception as e:
        print (f'Error while fetching store ids :  {e}')
        return []

def get_uptime_downtime_facade(store_id):
    try:
        uptime_minutes, downtime_minutes = get_uptime_downtime_in_last_hour(store_id)
        uptime_hours, downtime_hours = get_uptime_downtime_in_last_day(store_id)
        uptime_days, downtime_days = get_uptime_downtime_in_last_week(store_id)
        
        return [
            uptime_minutes,
            uptime_hours,
            uptime_days,
            downtime_minutes,
            downtime_hours,
            downtime_days
        ]
    except Exception as e:
        print (f"Error getting uptime in get_report_facade {e}")
        return [ -1, -1, -1, -1, -1, -1]
#from datetime import datetime, timedelta
from calendar import monthrange
import pendulum
import json

def build_date_range(config):
    
    # work with date string to support manual date override
    # do not forget that range requested is [[ => always add 1 day !!
    date_start_yyyy_mm_dd = ""
    date_end_yyyy_mm_dd = ""
        
    if config['date_kind'] == 'relative':
            
        # use user timezone
        datetime_reference = pendulum.now()
        #user_tz = datetime_reference.timezone.name

        datetime_reference_first_day_month = datetime_reference.start_of('month')
        datetime_reference_last_day_month = datetime_reference.end_of('month')

        datetime_reference_first_day_week = datetime_reference.start_of('week')
        datetime_reference_last_day_week = datetime_reference.end_of('week')

        if config['date_filter_time_cursor'] == 'current':
            if config['date_filter_unit'] == 'month':
                date_start_yyyy_mm_dd = datetime_reference_first_day_month.strftime('%Y-%m-%d')
                date_end_yyyy_mm_dd = datetime_reference_last_day_month.add(days=1).strftime('%Y-%m-%d')

            if config['date_filter_unit'] == 'day':
                date_start_yyyy_mm_dd = datetime_reference.strftime('%Y-%m-%d')
                date_end_yyyy_mm_dd = datetime_reference.add(days=1).strftime('%Y-%m-%d')

        if config['date_filter_time_cursor'] == 'previous':
            if config['date_filter_unit'] == 'month':
                date_start_yyyy_mm_dd = datetime_reference_first_day_month.subtract(months=config['date_filter_num_unit']).strftime('%Y-%m-%d')
                date_end_yyyy_mm_dd = datetime_reference_first_day_month.strftime('%Y-%m-%d')

                if config['date_filter_include_today']:
                    date_end_yyyy_mm_dd = datetime_reference_last_day_month.add(days=1).strftime('%Y-%m-%d')

            if config['date_filter_unit'] == 'day':
                date_start_yyyy_mm_dd = datetime_reference.subtract(days=config['date_filter_num_unit']).strftime('%Y-%m-%d')
                date_end_yyyy_mm_dd = datetime_reference.strftime('%Y-%m-%d')

                if config['date_filter_include_today']:
                    date_end_yyyy_mm_dd = datetime_reference.add(days=1).strftime('%Y-%m-%d')

    else:
        
        date_start_yyyy_mm_dd = config['override_date_start_yyyy_mm_dd']
        date_end_yyyy_mm_dd = config['override_date_end_yyyy_mm_dd']
    
    return {'start': date_start_yyyy_mm_dd, 'end': date_end_yyyy_mm_dd}


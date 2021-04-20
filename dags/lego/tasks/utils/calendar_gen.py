import argparse
from datetime import datetime
from datetime import timedelta
import math
import calendar, os

# CalendarRecord = namedtuple("CalendarRecord", "id date_id date_type d_day d_week d_month d_quarter d_year fiscal_year is_chinese_holiday is_holiday start_time end_time lego_day lego_week lego_week_code lego_week_name lego_month lego_quarter lego_year lego_start_time lego_end_time")

month_week_52 = {
    1:1, 2:1, 3:1,4:1,
    5:2, 6:2, 7:2, 8:2,
    9:3, 10:3, 11:3, 12:3, 13:3,
    14:4, 15:4, 16:4, 17:4,
    18:5, 19:5, 20:5, 21:5,
    22:6, 23:6, 24:6, 25:6, 26:6,
    27:7, 28:7, 29:7, 30:7,
    31:8, 32:8, 33:8, 34:8,
    35:9, 36:9, 37:9, 38:9, 39:9,
    40:10, 41:10, 42:10, 43:10,
    44:11, 45:11, 46:11, 47:11,
    48:12, 49:12, 50:12, 51:12, 52:12
}

month_week_53 = {
    1:1, 2:1, 3:1,4:1,
    5:2, 6:2, 7:2, 8:2,
    9:3, 10:3, 11:3, 12:3, 13:3,
    14:4, 15:4, 16:4, 17:4,
    18:5, 19:5, 20:5, 21:5,
    22:6, 23:6, 24:6, 25:6, 26:6,
    27:7, 28:7, 29:7, 30:7,
    31:8, 32:8, 33:8, 34:8,
    35:9, 36:9, 37:9, 38:9, 39:9,
    40:10, 41:10, 42:10, 43:10,
    44:11, 45:11, 46:11, 47:11, 48:11,
    49:12, 50:12, 51:12, 52:12, 53:12
}

# week 4 4 5
def gen_calendar_day(start_year, mode="day"):
    day_list = []
    year_first_day = datetime.strptime(start_year+"-01-01", "%Y-%m-%d")
    first_weekday = year_first_day.weekday()
    if first_weekday <= 3:
        start_day = year_first_day - timedelta(days = first_weekday )
    else:
        start_day = year_first_day + timedelta(days = 7 - first_weekday )

    year_end_day = datetime.strptime(start_year+"-12-31", "%Y-%m-%d")
    end_weekday = year_end_day.weekday()
    if end_weekday < 3:
        end_day = year_end_day - timedelta(days =  end_weekday + 1 )
    else:
        end_day = year_end_day + timedelta(days = 6 - end_weekday )

    if (end_day - start_day ).days + 1 >= 370:
        year_week_cnt = month_week_53
    else:
        year_week_cnt = month_week_52

    lego_end_day =end_day
    lego_start_day = start_day

    if int(end_day.strftime('%d')) < 31 and int(end_day.strftime('%m')) ==12:
        end_day = end_day + timedelta( days = 31 - int(end_day.strftime('%d')))

    if int(start_day.strftime('%d')) >1 and int(start_day.strftime('%m')) ==1:
        start_day = start_day - timedelta( int(start_day.strftime('%d')) - 1)

    t_day = start_day
    for n in range( (end_day - start_day ).days):
        id = t_day.strftime('%Y%m%d')
        date_id = t_day.strftime('%Y-%m-%d')
        date_type = 'day'
        d_day = t_day.strftime('%d')
        d_week = int(t_day.strftime('%W')) + 1
        d_month = t_day.strftime('%m')
        d_quarter = math.floor((int(d_month) -1 )/3 + 1)
        d_year = t_day.strftime('%Y')
        fiscal_year = ''
        is_chinese_holiday = ''
        is_holiday = ''
        start_time =  t_day.strftime('%Y-%m-%d')
        end_time = t_day.strftime('%Y-%m-%d')
        if t_day <= lego_end_day and t_day >= lego_start_day:
            lego_date = ''
            lego_year = start_year
            lego_week = math.ceil( ((t_day - lego_start_day).days + 1)/7 )
            lego_week_code = str(start_year)+"01"+ str(lego_week).rjust(2,'0')
            lego_week_name =  str(start_year)+"WK"+str(lego_week).rjust(2,'0')
            lego_month = year_week_cnt[lego_week]
            lego_month_disp = start_year +'14' + str(year_week_cnt[lego_week]).rjust(2,"0")
            lego_quarter = math.floor((int(lego_month) -1 )/3 + 1) #if lego_month >1 else 1
            lego_start_time = t_day.strftime('%Y-%m-%d')
            lego_end_time = t_day.strftime('%Y-%m-%d')
        else:
            lego_date = ''
            lego_year = ''
            lego_week = ''
            lego_week_code = ''
            lego_week_name =  ''
            lego_month = ''
            lego_month_disp =''
            lego_quarter = ''
            lego_start_time = ''
            lego_end_time = ''

        lego_day = [id, date_id, date_type, d_day, d_week, d_month, d_quarter,d_year, fiscal_year, is_chinese_holiday,
            is_holiday, start_time, end_time, lego_date, lego_week,lego_week_code, lego_week_name, lego_month,lego_month_disp, lego_quarter, lego_year,lego_start_time, lego_end_time]
        day_list.append(lego_day)

        t_day = t_day + timedelta(days = 1)
    else:
        id = t_day.strftime('%Y%m%d')
        date_id = t_day.strftime('%Y-%m-%d')
        date_type = 'day'
        d_day = t_day.strftime('%d')
        d_week = int(t_day.strftime('%W')) + 1
        d_month = t_day.strftime('%m')
        d_quarter = math.floor((int(d_month) -1 )/3 + 1)
        d_year = t_day.strftime('%Y')
        fiscal_year = ''
        is_chinese_holiday = ''
        is_holiday = ''
        start_time =  t_day.strftime('%Y-%m-%d')
        end_time = t_day.strftime('%Y-%m-%d')
        if t_day <= lego_end_day:
            lego_date = ''
            lego_year = start_year
            lego_week = math.ceil( ((t_day - lego_start_day).days + 1)/7 )
            lego_week_code = str(start_year)+"01"+ str(lego_week).rjust(2,'0')
            lego_week_name =  str(start_year)+"WK"+str(lego_week).rjust(2,'0')
            lego_month = year_week_cnt[lego_week]
            lego_month_disp = start_year +'14' + str(year_week_cnt[lego_week]).rjust(2,"0")
            lego_quarter = math.floor((int(lego_month) -1 )/3 + 1) # if lego_month >1 else 1
            lego_start_time = t_day.strftime('%Y-%m-%d')
            lego_end_time = t_day.strftime('%Y-%m-%d')
        else:
            lego_date = ''
            lego_year = ''
            lego_week = ''
            lego_week_code = ''
            lego_week_name =  ''
            lego_month_disp = ''
            lego_month = ''
            lego_quarter = ''
            lego_start_time = ''
            lego_end_time = ''

        lego_day = [id, date_id, date_type, d_day, d_week, d_month, d_quarter,d_year, fiscal_year, is_chinese_holiday,
            is_holiday, start_time, end_time, lego_date, lego_week,lego_week_code, lego_week_name, lego_month,lego_month_disp, lego_quarter,lego_year, lego_start_time, lego_end_time]
        day_list.append(lego_day)
    return day_list

def gen_calenday_year(day_list:list, year):
    year_list = []
    id = year
    date_id = ''
    date_type ='year'
    d_day = ''
    d_week = ''
    d_month = ''
    d_quarter = ''
    d_year = year
    fiscal_year = ''
    is_chinese_holiday = ''
    is_holiday = ''
    start_time =  year+'-01-01'
    end_time =   year + '-12-31'
    lego_date = ''
    lego_year = year
    lego_week = ''
    lego_week_code =  ''
    lego_week_name =  ''
    lego_month = ''
    lego_month_disp = ''
    lego_quarter = ''
    lego_start_time = None
    lego_end_time = None
    for d in day_list:
        if d[20] == year and lego_start_time is None:
            lego_start_time = d[1]
            continue

        if d[20] == year :
            lego_end_time = d[1]

    year_list.append( [id, date_id, date_type, d_day, d_week, d_month, d_quarter,d_year,
            fiscal_year, is_chinese_holiday,
            is_holiday, start_time, end_time, lego_date, lego_week,lego_week_code, lego_week_name,
            lego_month, lego_month_disp,lego_quarter,lego_year, lego_start_time, lego_end_time])
    return year_list


def gen_calendar_week(day_list:list, year):
    lego_max_week = max( int(v[14]) if v[14] !='' else 0 for v in day_list )
    cal_max_week = max( int(v[4]) if v[4] !='' else 0 for v in day_list )
    week = max(lego_max_week, cal_max_week)

    if lego_max_week == 53:
        year_week_cnt = month_week_53
    else:
        year_week_cnt = month_week_52

    week_list = []
    for i in range( week ):
        i += 1
        n_start_date = None
        n_end_date = None

        for d in day_list:
            if int(d[4]) == i and d[7] == year:
                n_end_date = d[1]
                if n_start_date is None:
                    n_start_date = d[1]
            elif int(d[4]) > i and d[7] == year:
                break

        l_start_date = None
        l_end_date = None
        for d in day_list:
            if d[14] != '' and int(d[14]) == i:
                if l_start_date is None:
                    l_start_date = d[1]
            else:
                if l_start_date is not None:
                    break
            l_end_date = d[1]

        id = year + "13" + str(i).rjust(2,'0')
        date_id = ''
        date_type = 'week'
        if cal_max_week >= i:
            d_day = ''
            d_week = i
            d_month = ''
            d_quarter = ''
            d_year = year
            fiscal_year = ''
            is_chinese_holiday = ''
            is_holiday = ''
            start_time =  n_start_date
            end_time = n_end_date
        else:
            d_day = ''
            d_week = ''
            d_month = ''
            d_quarter = ''
            d_year = year
            fiscal_year = ''
            is_chinese_holiday = ''
            is_holiday = ''
            start_time =  ''
            end_time = ''

        if lego_max_week >= i:
            lego_date = ''
            lego_year = year
            lego_week = i
            lego_week_code =  year + "01" + str(i).rjust(2,'0')
            lego_week_name =  year + "WK" + str(i).rjust(2,'0')
            lego_month = year_week_cnt[i]
            lego_month_disp = year +'14' + str(lego_month).rjust(2,"0")
            lego_quarter =  math.floor((year_week_cnt[i] -1 )/3 + 1) if lego_month >1 else 1
            lego_start_time = l_start_date
            lego_end_time = l_end_date
        else:
            lego_date = ''
            lego_year = ''
            lego_week = ''
            lego_week_code =  ''
            lego_week_name =  ''
            lego_month = ''
            lego_month_disp =''
            lego_quarter = ''
            lego_start_time = ''
            lego_end_time = ''
        lego_week = [id, date_id, date_type, d_day, d_week, d_month, d_quarter,d_year,
            fiscal_year, is_chinese_holiday,
            is_holiday, start_time, end_time, lego_date, lego_week,lego_week_code, lego_week_name,
            lego_month, lego_month_disp,lego_quarter,lego_year, lego_start_time, lego_end_time]
        week_list.append(lego_week)
    return week_list

def gen_calendar_month(week_list:list, year):

    lego_month_list = []
    for i in range(12):
        i += 1
        id = year + '14' + str(i).rjust(2,'0')
        date_id = ''
        date_type ='month'
        d_day = ''
        d_week = ''
        d_month = i
        d_quarter = math.floor((i -1 )/3 + 1) if i >1 else 1
        d_year = year
        fiscal_year = ''
        is_chinese_holiday = ''
        is_holiday = ''
        start_time =  year +"-" +str(i) + "-01"
        end_time =   datetime(int(year), i, calendar.monthrange( int(year), i)[1])

        lego_month_start = None
        lego_month_end = None
        for wk in week_list:
            if wk[17] == '':
                continue
            if i == int(wk[17]):
                if lego_month_start is None:
                    lego_month_start = wk[21]
                lego_month_end = wk[22]
            elif  int(wk[17]) > i:
                break

        lego_date = ''
        lego_year = year
        lego_week = ''
        lego_week_code =  ''
        lego_week_name =  ''
        lego_month = i
        lego_month_disp = year +'14' + str(i).rjust(2,"0")
        lego_quarter =  math.floor((lego_month -1 )/3 + 1) #if lego_month >1 else 1
        lego_start_time = lego_month_start
        lego_end_time = lego_month_end

        lego_month = [id, date_id, date_type, d_day, d_week, d_month, d_quarter,d_year,
            fiscal_year, is_chinese_holiday,
            is_holiday, start_time, end_time, lego_date, lego_week,lego_week_code, lego_week_name,
            lego_month,lego_month_disp, lego_quarter,lego_year, lego_start_time, lego_end_time]
        lego_month_list.append(lego_month)
    return lego_month_list


def gen_calendar_quarter(month_list:list, year):
    quarter_list = []
    for i in range(4):
        i += 1
        id = year + '15' + str(i).rjust(2,'0')
        date_id = ''
        date_type ='quarter'
        d_day = ''
        d_week = ''
        d_month = ''
        d_quarter = i
        d_year = year
        fiscal_year = ''
        is_chinese_holiday = ''
        is_holiday = ''
        if i == 1:
            start_time =  year+'-01-01'
            end_time =   year + '-03-31'
        elif i ==2:
            start_time =  year+'-04-01'
            end_time =   year + '-06-30'
        elif i ==3:
            start_time =  year+'-07-01'
            end_time =   year + '-09-30'
        elif i ==4:
            start_time =  year+'-10-01'
            end_time =   year + '-12-31'

        lego_date = ''
        lego_year = year
        lego_week = ''
        lego_week_code =  ''
        lego_week_name =  ''
        lego_month = ''
        lego_month_disp = ''
        lego_quarter = i
        lego_quarter_start = None
        lego_quarter_end = None
        for m in month_list:
            if m[18] == '':
                continue
            if i == int(m[18]):
                if lego_quarter_start is None:
                    lego_quarter_start = m[20]
                lego_quarter_end = m[21]
            elif  int(m[18]) > i:
                break

        lego_quarter_name = [id, date_id, date_type, d_day, d_week, d_month, d_quarter,d_year,
            fiscal_year, is_chinese_holiday,
            is_holiday, start_time, end_time, lego_date, lego_week,lego_week_code, lego_week_name,
            lego_month,lego_month_disp, lego_quarter,lego_year, lego_quarter_start, lego_quarter_end]

        quarter_list.append(lego_quarter_name)
    return quarter_list

def gen_day_hours(day_list:list):
    day_hour_list = []
    for d in day_list:
        for i in range(24):
            id = d[0] + str(i).rjust(2,'0')
            date_id = d[1]
            date_type = 'hour'
            d_day = d[3]
            d_week = d[4]
            d_month = d[5]
            d_quarter = d[6]
            d_year = d[7]
            fiscal_year = d[8]
            is_chinese_holiday = d[9]
            is_holiday = d[10]
            start_time =  d[11] + " " +str(i).rjust(2,'0') +":00:00"
            end_time = d[12] + " " +str(i).rjust(2,'0') +":59:59"

            lego_date = d[13]
            lego_week = d[14]
            lego_week_code = d[15]
            lego_week_name =  d[16]
            lego_month = d[17]
            lego_month_disp = d[18]
            lego_quarter = d[19]
            lego_year = d[20]
            lego_start_time = d[21] + " " +str(i).rjust(2,'0') +":00:00" if d[21] !='' else ''
            lego_end_time = d[22] + " " +str(i).rjust(2,'0') +":59:59" if d[22] !='' else ''

            lego_day = [id, date_id, date_type, d_day, d_week, d_month, d_quarter,d_year, fiscal_year, is_chinese_holiday,
                is_holiday, start_time, end_time, lego_date, lego_week,lego_week_code, lego_week_name, lego_month, lego_month_disp, lego_quarter, lego_year,lego_start_time, lego_end_time]
            day_hour_list.append(lego_day)
    return day_hour_list

# date_str: yyyymmdd
def get_calendar_day_by_date(date_str):
    date_year = date_str[:4]

    def get_day_list(day_list):
        solar_week = 0
        solar_week_offset = 1
        lego_week = 0
        lego_week_offset = 1
        for day in day_list:
            if day[4] != solar_week:
                solar_week = day[4]
                solar_week_offset =1
            else:
                solar_week_offset +=1

            if day[14] != lego_week:
                lego_week = day[14]
                lego_week_offset =1
            else:
                lego_week_offset +=1

            if day[0] == date_str and day[19] != '':
                # day, solar week, solar week offset, solar month, solar year, lego week, lego week offset, lego month, lego year
                return [day[3], solar_week, solar_week_offset, day[5], day[7], lego_week, lego_week_offset,day[17],day[20] ]

    day_list = gen_calendar_day(start_year = date_year)
    day_disp = get_day_list(day_list)
    if day_disp is not None:
        return day_disp
    if int(date_str[4:6]) == 1:
        day_list = gen_calendar_day(start_year = str(int(date_year) -1))
    elif int(date_str[4:6]) == 12:
        day_list  = gen_calendar_day(start_year = str(int(date_year) +1))
    else:
        day_list = None

    return get_day_list(day_list)

def start(year, path):
    day_list = gen_calendar_day(start_year = year)
    week_list = gen_calendar_week(day_list, year)
    month_list = gen_calendar_month(week_list, year)
    quarte_list = gen_calendar_quarter(week_list, year)
    day_hour_list = gen_day_hours(day_list)
    year_list = gen_calenday_year(day_hour_list, year)
    calender_list = []
    calender_list.extend(day_list)
    calender_list.extend(week_list)
    calender_list.extend(month_list)
    calender_list.extend(quarte_list)
    calender_list.extend(day_hour_list)
    calender_list.extend(year_list)

    str_calendar_list = [",".join( [str(n) if n is not None else '' for n in v])+'\n' for v in calender_list]

    target_path = os.path.join(path, str(year))
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    with open(os.path.join(path, str(year), 'calendar.csv'), 'w', newline='\n') as fd:
        fd.writelines(str_calendar_list)


if __name__ == "__main__":
    pass
    # start('2021', r'C:\workspace\project\LEGO\code\CDP-CR\POC')
    # parser = argparse.ArgumentParser(description="Send out mail")
    # parser.add_argument('-s', "--start")
    # args = parser.parse_args()
    # start_year = args.start
    # if start_year is None:
    #     start_year = '2014'
    # day_list = gen_calendar_day(start_year = start_year)
    # # for d in day_list:
    # #     print(",".join([ str(v) for v in d]))
    # week_list = gen_calendar_week(day_list, start_year)
    # # for d in week_list:
    # #     print(",".join([ str(v) for v in d]))
    # month_list = gen_calendar_month(week_list, start_year)
    # # for d in month_list:
    # #     print(",".join([ str(v) for v in d]))

    # quarte_list = gen_calendar_quarter(week_list, start_year)
    # # for d in quarte_list:
    # #     print(",".join([ str(v) for v in d]))

    # day_hour_list = gen_day_hours(day_list)

    # calender_list = []
    # calender_list.extend(day_list)
    # calender_list.extend(week_list)
    # calender_list.extend(month_list)
    # calender_list.extend(quarte_list)
    # calender_list.extend(day_hour_list)

    # for d in calender_list:
    #     print(",".join([ str(v) for v in d]))

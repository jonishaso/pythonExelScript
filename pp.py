import pandas as pd
from datetime import datetime, timedelta
import calendar
import sys
import json
import smtplib
import time
import schedule
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


SERVER = "smtp-mail.outlook.com"
FROM = "notifications@ruizean.com"
EPWD = "Qn2Vm^tu@mqL9JPX"


json_list_data = json.load(open('Z:/IT/Development/name-list.json'))
syd_name = json_list_data['syd']
mel_name = json_list_data['mel']

income_files = []
time_format_date = '%Y-%m-%d'
file_surfix = "papercut-print-log-"
syd_folder_path = "Z:/IT/Development/printer_report/raw_data/syd/"
mel_folder_path = "Z:/IT/Development/printer_report/raw_data/mel/"


def send_mail(subject, text_body, attachemnt_name='', attachemnt_path='', to=''):
    if to == '':
        return
    msg = MIMEMultipart()
    msg['From'] = FROM
    msg['To'] = to
    msg['Subject'] = subject

    msg.attach(MIMEText(text_body, 'plain'))
    if attachemnt_name != '' and attachemnt_path != '':
        filename = attachemnt_name
        attachment = open(attachemnt_path, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        "attachment; filename= %s" % filename)
        msg.attach(part)

    server = smtplib.SMTP(SERVER, 587)
    server.connect(SERVER, 587)
    server.starttls()
    server.login(FROM, EPWD)
    outer = msg.as_string()
    server.sendmail(FROM, to, outer)
    server.quit()


def income_collection(begin, end):
    day_range = []
    start_date = datetime.strptime(begin, time_format_date)
    end_date = datetime.strptime(end, time_format_date)
    month_start = start_date.month
    year_start = start_date.year
    cc = calendar.Calendar(0)
    month_range = list(range(month_start, end_date.month+1))
    for e in month_range:
        for i in cc.itermonthdays4(year_start, e):
            i_datetime = datetime(i[0], i[1], i[2])
            i_weekday = i[3]
            diff_start = (i_datetime - start_date).days
            diff_end = (i_datetime - end_date).days
            if diff_end <= 0 and diff_start >= 0 and i_weekday in [0, 1, 2, 3, 4]:
                time_str = i_datetime.isoformat().split('T')[0]
                day_range.append("{}{}.csv".format(file_surfix, time_str))
    return day_range


def last_week_days(given_day=''):
    day_range = []
    if given_day != '':
        today = datetime.strptime(given_day, time_format_date)
    else:
        today = datetime.now()
    week_num = (today.isocalendar())[1]
    cal = calendar.Calendar(0)
    for i in cal.itermonthdays4(today.year, today.month):
        if (datetime(i[0], i[1], i[2]).isocalendar())[1] == (week_num-1) and i[3] in [0, 1, 2, 3, 4]:
            i_datetime = datetime(i[0], i[1], i[2])
            time_str = i_datetime.isoformat().split('T')[0]
            day_range.append("{}{}.csv".format(file_surfix, time_str))
        else:
            continue
    return day_range


def calculate(sub_income_df):
    total = 0
    pages = sub_income_df.iloc[:]['Pages'].values.tolist()
    copy = sub_income_df.iloc[:]['Copies'].values.tolist()
    for index, e in enumerate(pages):
        total += e * copy[index]
    return total


def single_day_outcome(income_df, name_list):
    outcome = []
    for i in name_list:
        temp_name = i['name'].split(' ')
        temp_name = "{}{}".format(
            temp_name[0].lower(), temp_name[1][0].lower())
        records = income_df.loc[(income_df['User'] == temp_name)]
        records_sin = records.loc[(records['Duplex'] == 'NOT DUPLEX')]
        records_dup = records.loc[(records['Duplex'] == 'DUPLEX')]
        records_bw = records.loc[(records['Grayscale'] == 'GRAYSCALE')]
        records_color = records.loc[(records['Grayscale'] == 'NOT GRAYSCALE')]
        outcome.append({'name': i['name'],
                        'pages': calculate(records),
                        'double_side': calculate(records_dup),
                        'single_side': calculate(records_sin),
                        'black&white': calculate(records_bw),
                        'color': calculate(records_color)
                        })
    return outcome


def count_pages():
    syd_temp_all_frame = []
    mel_temp_all_frame = []
    file_list = []
    file_str = ''
    """ python ./pp.py  # previous week of current date
        python ./pp.py 2019-02-08 # previous week of 2019-02-08
        python ./pp.py 2019-02-08 2019-02-12 # time from 02-08 to 02-12
    """
    try:
        if len(sys.argv) == 1:
            file_list = last_week_days()
        elif len(sys.argv) == 2:
            datetime.strptime(sys.argv[1], time_format_date)
            file_list = last_week_days(sys.argv[1])
        elif len(sys.argv) == 3:
            temp_a = datetime.strptime(sys.argv[1], time_format_date)
            temp_b = datetime.strptime(sys.argv[2], time_format_date)
            if (temp_b - temp_a).days <= 0:
                raise ValueError('start date is later than end date')
            file_list = income_collection(sys.argv[1], sys.argv[2])
    except ValueError:
        print('err: input date format err')
        return
    for i in file_list:
        try:
            syd_csv_file = pd.read_csv(
                syd_folder_path + i, header=1, usecols=[1, 2, 3, 11, 12], index_col=False, engine='c', error_bad_lines=False)
            mel_csv_file = pd.read_csv(
                mel_folder_path + i, header=1, usecols=[1, 2, 3, 11, 12], index_col=False, engine='c', error_bad_lines=False)
        except FileNotFoundError:
            continue
        else:
            syd_temp_all_frame.append(syd_csv_file)
            mel_temp_all_frame.append(mel_csv_file)
            file_str += i + "; "
    """ if len(temp_all_frame) == 0:

        return {
            "period": file_list[0].split(file_surfix)[1].split('.')[0] + "~" + file_list[-1].split(file_surfix)[1].split('.')[0],
            "valid_files": file_str,
            "outcome_df": pd.DataFrame(data={'': []})
        }
    else: """
    if len(syd_temp_all_frame) == 0:
        syd_new_df = pd.DataFrame(data=[])
        mel_new_df = pd.DataFrame(data=[])
        return {
            "period": "no_raw_data_match",
            "valid_files": file_str,
            "syd_outcome_df": syd_new_df,
            "mel_outcome_df": mel_new_df
        }
    else:
        syd_new_df = pd.DataFrame(data=single_day_outcome(pd.concat(syd_temp_all_frame), syd_name)).sort_values(
            by=['pages'], ascending=False).reset_index().drop(columns=['index'], axis=0)
        mel_new_df = pd.DataFrame(data=single_day_outcome(pd.concat(mel_temp_all_frame), mel_name)).sort_values(
            by=['pages'], ascending=False).reset_index().drop(columns=['index'], axis=0)
        return {
            "period": file_list[0].split(file_surfix)[1].split('.')[0] + "~" + file_list[-1].split(file_surfix)[1].split('.')[0],
            "valid_files": file_str,
            "syd_outcome_df": syd_new_df,
            "mel_outcome_df": mel_new_df
        }


def write_excel_file(final_result):
    outcome_file_name = './' + \
        datetime.now().isoformat().split('.')[0].replace(':', '-') + '.xlsx'
    outcome_file = pd.ExcelWriter(outcome_file_name, engine='xlsxwriter')

    final_result["syd_outcome_df"].to_excel(
        outcome_file, sheet_name='Sydney office', index=False)
    final_result["mel_outcome_df"].to_excel(
        outcome_file, sheet_name='Melbourne office', index=False)

    outcome_workbook = outcome_file.book
    syd_outcome_worksheet = outcome_file.sheets['Sydney office']
    mel_outcome_worksheet = outcome_file.sheets['Melbourne office']

    bold = outcome_workbook.add_format(
        {'bold': True, 'bg_color': 'yellow', 'font_size': 18})
    align = outcome_workbook.add_format({'align': 'center', 'font_size': 16})
    bold_red = outcome_workbook.add_format(
        {'bold': True, 'bg_color': 'yellow'})
    syd_outcome_worksheet.set_column('A:F', 40, align)
    syd_outcome_worksheet.write('A1', 'Name', bold)
    syd_outcome_worksheet.write('B1', 'Total No. of pages', bold)
    syd_outcome_worksheet.write('C1', 'No. of pages  Double-sided', bold)
    syd_outcome_worksheet.write('D1', 'No. of pages  Single side', bold)
    syd_outcome_worksheet.write('E1', 'No. of pages Black & white', bold)
    syd_outcome_worksheet.write('F1', 'No. of pages Color', bold)

    mel_outcome_worksheet.set_column('A:F', 40, align)
    mel_outcome_worksheet.write('A1', 'Name', bold)
    mel_outcome_worksheet.write('B1', 'Total No. of pages', bold)
    mel_outcome_worksheet.write('C1', 'No. of pages  Double-sided', bold)
    mel_outcome_worksheet.write('D1', 'No. of pages  Single side', bold)
    mel_outcome_worksheet.write('E1', 'No. of pages Black & white', bold)
    mel_outcome_worksheet.write('F1', 'No. of pages Color', bold)

    row = 1
    for row_num, value in final_result["syd_outcome_df"].iterrows():
        syd_outcome_worksheet.write(row, 0, value['name'], align)
        syd_outcome_worksheet.write(row, 1, value['pages'], align)
        syd_outcome_worksheet.write(row, 2, value['double_side'], align)
        syd_outcome_worksheet.write(row, 3, value['single_side'], align)
        syd_outcome_worksheet.write(row, 4, value['black&white'], align)
        syd_outcome_worksheet.write(row, 5, value['color'], align)
        row += 1

    syd_outcome_worksheet.write(row+1, 0, "Total", bold)
    syd_outcome_worksheet.write(row+1, 1, '=SUM(B2:B{})'.format(row), bold)
    syd_outcome_worksheet.write(row+1, 3, "period", bold_red)
    syd_outcome_worksheet.write(row+1, 4, final_result["period"], bold_red)

    row = 1
    for row_num, value in final_result["mel_outcome_df"].iterrows():
        mel_outcome_worksheet.write(row, 0, value['name'], align)
        mel_outcome_worksheet.write(row, 1, value['pages'], align)
        mel_outcome_worksheet.write(row, 2, value['double_side'], align)
        mel_outcome_worksheet.write(row, 3, value['single_side'], align)
        mel_outcome_worksheet.write(row, 4, value['black&white'], align)
        mel_outcome_worksheet.write(row, 5, value['color'], align)
        row += 1

    mel_outcome_worksheet.write(row+1, 0, "Total", bold)
    mel_outcome_worksheet.write(row+1, 1, '=SUM(B2:B{})'.format(row), bold)
    mel_outcome_worksheet.write(row+1, 3, "period", bold_red)
    mel_outcome_worksheet.write(row+1, 4, final_result["period"], bold_red)

    outcome_file.save()
    return outcome_file_name


def jobs():
    excel_file_name = write_excel_file(count_pages())
    # send_mail("sched", "Printer Report", to="it.admin@ruizean.com",attachemnt_path=excel_file_name, attachemnt_name="printing_report.xlsx")


# excel_file_name = write_excel_file(count_pages())
jobs()

schedule.every(2).seconds.do(jobs)
while 1:
    schedule.run_pending()
    time.sleep(1)

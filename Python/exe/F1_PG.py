import tkinter as tk
from tkinter import ttk
# import webbrowser
# import requests
import zipfile
import psycopg2
# import time
import urllib.request
import os
import shutil
import pandas as pd


root = tk.Tk()
root.title('F1 Database for Postgres')
root.iconbitmap('D:\\Git\\Darth-Vader\\Python\\exe\\f1_formula1_icon.ico')

host_value = tk.StringVar()
port_value = tk.StringVar()
user_value = tk.StringVar()
password_value = tk.StringVar()
result_value = tk.StringVar(value='Result: ')


def run_db(*args):
    try:
        host = str(host_value.get())
        port = str(port_value.get())
        user = str(user_value.get())
        password = str(password_value.get())
        print(1)
        if not os.path.exists(os.getcwd() + '\\tmp_del'):
            os.mkdir(os.getcwd() + '\\tmp_del')
        conn = psycopg2.connect(database="postgres", user=user, password=password, host=host, port=port)
        url = "https://ergast.com/downloads/f1db_csv.zip"
        urllib.request.urlretrieve(url, os.getcwd() + '\\tmp_del\\f1db_csv.zip')
        print(2)
        with zipfile.ZipFile(os.getcwd() + '\\tmp_del\\f1db_csv.zip', 'r') as zip_ref:
            zip_ref.extractall(os.getcwd()+'\\tmp_del')

        with open(os.getcwd() + '\\tmp_del\\drivers.csv', 'r') as file:
            filedata = file.read()
            filedata = filedata.replace('\'', '\'\'')

        with open(os.getcwd() + '\\tmp_del\\drivers.csv', 'w') as file:
            file.write(filedata)
        
        data = pd.read_csv(os.getcwd() + '\\tmp_del\\drivers.csv')
        df = pd.DataFrame(data)
        cur = conn.cursor()
        cur.execute("DROP SCHEMA IF EXISTS f1 CASCADE;")
        cur.execute("DROP TABLE IF EXISTS f1.drivers;")
        cur.execute("CREATE SCHEMA f1;")
        cur.execute("CREATE TABLE f1.drivers (driverId int, driverRef text, number text, code text, forename text, surname text, dob text, nationality text, url text);")
        print(6)
        for row in df.itertuples():
            cur.execute(f'INSERT INTO f1.drivers VALUES ({row.driverId},\'{row.driverRef}\',\'{row.number}\',\'{row.code}\',\'{row.forename}\',\'{row.surname}\',\'{row.dob}\',\'{row.nationality}\',\'{row.url}\');')
            conn.commit()
        print(7)
        cur.execute("UPDATE f1.drivers SET number = NULL WHERE number = '\\N';")
        cur.execute("UPDATE f1.drivers SET code = NULL WHERE code = '\\N';")
        cur.execute("ALTER TABLE f1.drivers \
                     ALTER COLUMN number TYPE INT \
                     USING number::integer;")

        conn.commit()# <--- makes sure the change is shown in the database
        conn.close()
        cur.close()
        shutil.rmtree(os.getcwd() + '\\tmp_del', ignore_errors=True)
        # files = glob.glob(os.getcwd() + '\\tmp\\*.csv')
        #
        # for f in files:
        #     os.remove(f)
        #     files = glob.glob(os.getcwd() + '\\tmp\\*.zip')
        #
        # for f in files:
        #     os.remove(f)

        result_value.set('Result: successfully')

    except:

        result_value.set('Result: unsuccessfully')


main_frame = ttk.Frame(root, padding=(100, 50))
main_frame.grid()

host_label = ttk.Label(main_frame, text='Host: ')
host_entry = ttk.Entry(main_frame, width=10, textvariable=host_value)
port_label = ttk.Label(main_frame, text='Port: ')
port_entry = ttk.Entry(main_frame, width=10, textvariable=port_value)
user_label = ttk.Label(main_frame, text='User: ')
user_entry = ttk.Entry(main_frame, width=10, textvariable=user_value)
password_label = ttk.Label(main_frame, text='Password: ')
password_entry = ttk.Entry(main_frame, width=10, textvariable=password_value)

f_display_result = ttk.Label(main_frame, textvariable=result_value)

convert_button = ttk.Button(main_frame, text='RUN', command=run_db)

host_label.grid(row=0, column=0, sticky='W')
host_entry.grid(row=0, column=1, sticky='E')
port_label.grid(row=1, column=0, sticky='W')
port_entry.grid(row=1, column=1, sticky='E')
user_label.grid(row=2, column=0, sticky='W')
user_entry.grid(row=2, column=1, sticky='E')
password_label.grid(row=3, column=0, sticky='W')
password_entry.grid(row=3, column=1, sticky='E')
f_display_result.grid(row=4, column=0, sticky='W')
convert_button.grid(row=5, column=0, sticky='W')

host_entry.focus()

root.bind('<Return>', run_db)


root.mainloop()
import tkinter as tk
from tkinter import ttk
# import webbrowser
# import requests
import zipfile
import psycopg2
# import time
import urllib.request
import os
import glob

root = tk.Tk()
root.title('F1 Database for Postgres')
root.iconbitmap(os.getcwd() + '\\f1_formula1_6833.ico')

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

        conn = psycopg2.connect(database="postgres", user=user, password=password, host=host, port=port)

        url = "https://ergast.com/downloads/f1db_csv.zip"
        urllib.request.urlretrieve(url, os.getcwd() + '\\tmp\\f1db_csv.zip')

        with zipfile.ZipFile(os.getcwd() + '\\tmp\\f1db_csv.zip', 'r') as zip_ref:
            zip_ref.extractall(os.getcwd()+'\\tmp')

        cur = conn.cursor()

        cur.execute("DROP SCHEMA IF EXISTS f1 CASCADE;")
        cur.execute("DROP TABLE IF EXISTS f1.drivers;")
        cur.execute("CREATE SCHEMA f1;")
        cur.execute("CREATE TABLE f1.drivers (driverId int, driverRef text, number text, code text, forename text, surname text, dob date, nationality text, url text);")
        cur.execute("COPY f1.drivers FROM 'D:/test/drivers.csv' DELIMITER ',' csv header;")
        cur.execute("UPDATE f1.drivers SET number = NULL WHERE number = '\\N';")
        cur.execute("UPDATE f1.drivers SET code = NULL WHERE code = '\\N';")
        cur.execute("ALTER TABLE f1.drivers \
                     ALTER COLUMN number TYPE INT \
                     USING number::integer;")

        conn.commit()# <--- makes sure the change is shown in the database
        conn.close()
        cur.close()
        files = glob.glob(os.getcwd() + '\\tmp\\*.csv')

        for f in files:
            os.remove(f)
            files = glob.glob(os.getcwd() + '\\tmp\\*.zip')

        for f in files:
            os.remove(f)

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
host_entry.grid(row=0, column=1)
port_label.grid(row=1, column=0, sticky='W')
port_entry.grid(row=1, column=1)
user_label.grid(row=2, column=0, sticky='W')
user_entry.grid(row=2, column=1)
password_label.grid(row=3, column=0, sticky='W')
password_entry.grid(row=3, column=1)
f_display_result.grid(row=4, column=0)
convert_button.grid(row=5, column=0, sticky='W')

host_entry.focus()

root.bind('<Return>', run_db)


root.mainloop()
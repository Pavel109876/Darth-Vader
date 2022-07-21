import tkinter as tk
from tkinter import ttk
from tkinter.ttk import Progressbar
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
root.iconbitmap('C:\\Git\\Darth-Vader\\Python\\exe\\f1_formula1_icon.ico')

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

        if not os.path.exists(os.getcwd() + '\\tmp_del'):
            os.mkdir(os.getcwd() + '\\tmp_del')
        conn = psycopg2.connect(database="postgres", user=user, password=password, host=host, port=port)
        url = "https://ergast.com/downloads/f1db_csv.zip"
        urllib.request.urlretrieve(url, os.getcwd() + '\\tmp_del\\f1db_csv.zip')

        with zipfile.ZipFile(os.getcwd() + '\\tmp_del\\f1db_csv.zip', 'r') as zip_ref:
            zip_ref.extractall(os.getcwd() + '\\tmp_del')

        with open(os.getcwd() + '\\tmp_del\\drivers.csv', 'r', errors='ignore') as file:
            filedata = file.read()
            filedata = filedata.replace('\'', '\'\'')

        with open(os.getcwd() + '\\tmp_del\\drivers.csv', 'w') as file:
            file.write(filedata)

        with open(os.getcwd() + '\\tmp_del\\driver_standings.csv', 'r', errors='ignore') as file:
            filedata = file.read()
            filedata = filedata.replace('D', '0')

        with open(os.getcwd() + '\\tmp_del\\driver_standings.csv', 'w') as file:
            file.write(filedata)

        # with open(os.getcwd() + '\\tmp_del\\races.csv', 'r') as file:
        #     filedata = file.read()
        #     filedata = filedata.replace('\\N', 'NULL')
        #
        # with open(os.getcwd() + '\\tmp_del\\races.csv', 'w') as file:
        #     file.write(filedata)

        data = pd.read_csv(os.getcwd() + '\\tmp_del\\drivers.csv', encoding='latin1')
        df = pd.DataFrame(data)

        data1 = pd.read_csv(os.getcwd() + '\\tmp_del\\driver_standings.csv')
        df1 = pd.DataFrame(data1)

        data2 = pd.read_csv(os.getcwd() + '\\tmp_del\\races.csv')
        df2 = pd.DataFrame(data2)

        data3 = pd.read_csv(os.getcwd() + '\\tmp_del\\lap_times.csv')
        df3 = pd.DataFrame(data3)

        data4 = pd.read_csv(os.getcwd() + '\\tmp_del\\constructors.csv')
        df4 = pd.DataFrame(data4)

        data5 = pd.read_csv(os.getcwd() + '\\tmp_del\\seasons.csv')
        df5 = pd.DataFrame(data5)

        data6 = pd.read_csv(os.getcwd() + '\\tmp_del\\status.csv')
        df6 = pd.DataFrame(data6)

        data7 = pd.read_csv(os.getcwd() + '\\tmp_del\\constructor_results.csv')
        df7 = pd.DataFrame(data7)

        data8 = pd.read_csv(os.getcwd() + '\\tmp_del\\constructor_standings.csv')
        df8 = pd.DataFrame(data8)

        data9 = pd.read_csv(os.getcwd() + '\\tmp_del\\results.csv')
        df9 = pd.DataFrame(data9)

        data10 = pd.read_csv(os.getcwd() + '\\tmp_del\\pit_stops.csv')
        df10 = pd.DataFrame(data10)

        data11 = pd.read_csv(os.getcwd() + '\\tmp_del\\circuits.csv')
        df11 = pd.DataFrame(data11)

        data12 = pd.read_csv(os.getcwd() + '\\tmp_del\\qualifying.csv')
        df12 = pd.DataFrame(data12)

        data13 = pd.read_csv(os.getcwd() + '\\tmp_del\\sprint_results.csv')
        df13 = pd.DataFrame(data13)

        cur = conn.cursor()

        cur.execute("DROP SCHEMA IF EXISTS f1 CASCADE;")
        cur.execute("DROP TABLE IF EXISTS f1.drivers;")
        cur.execute("CREATE SCHEMA f1;")
        cur.execute("CREATE TABLE f1.drivers (driverId int PRIMARY KEY, driverRef text, number text, code text, "
                    "forename text, surname text, dob date, nationality text, url text);")
        cur.execute("CREATE TABLE f1.circuits (circuitId int PRIMARY KEY, circuitRef text, name text, location text, "
                    "country text, lat text, lng text, alt text, url text);")
        cur.execute("CREATE TABLE f1.races (raceId int PRIMARY KEY, year int, round int, circuitId int, name text, "
                    "date text, time text, url text, fp1_date text, fp1_time text, fp2_date text, fp2_time text, "
                    "fp3_date text, fp3_time text, quali_date text, quali_time text, sprint_date text, sprint_time "
                    "text, CONSTRAINT fk_circuits FOREIGN KEY(circuitId) REFERENCES f1.circuits(circuitId));")
        cur.execute("CREATE TABLE f1.driver_standings (driverStandingsId int PRIMARY KEY, raceId int, driverId int, "
                    "points int, position int, positionText int, wins int, CONSTRAINT fk_drivers FOREIGN KEY("
                    "driverId) REFERENCES f1.drivers(driverId), CONSTRAINT fk_races FOREIGN KEY(raceId) REFERENCES "
                    "f1.races(raceId));")
        cur.execute("CREATE TABLE f1.lap_times (raceId int, driverId int, lap int,        position int, time text, "
                    "milliseconds int, CONSTRAINT fk_drivers2 FOREIGN KEY(driverId) REFERENCES f1.drivers(driverId), "
                    "CONSTRAINT fk_races2 FOREIGN KEY(raceId) REFERENCES f1.races(raceId));")
        cur.execute("CREATE TABLE f1.constructors (constructorId int PRIMARY KEY, constructorRef text, name text, "
                    "nationality text, url text);")
        cur.execute("CREATE TABLE f1.seasons (year int PRIMARY KEY, url text);")
        cur.execute("CREATE TABLE f1.status (statusId int PRIMARY KEY, status text);")
        cur.execute("CREATE TABLE f1.constructor_results (constructorResultsId int PRIMARY KEY, raceId int, "
                    "constructorId int, points int, status text, CONSTRAINT fk_races3 FOREIGN KEY(raceId) REFERENCES "
                    "f1.races(raceId), CONSTRAINT fk_constructor FOREIGN KEY(constructorId) REFERENCES "
                    "f1.constructors(constructorId));")
        cur.execute("CREATE TABLE f1.constructor_standings (constructorStandingsId int PRIMARY KEY, raceId int, "
                    "constructorId int, points decimal, position int, positionText text, wins int, CONSTRAINT "
                    "fk_races4 FOREIGN KEY(raceId) REFERENCES f1.races(raceId), CONSTRAINT fk_constructor2 FOREIGN "
                    "KEY(constructorId) REFERENCES f1.constructors(constructorId));")
        cur.execute("CREATE TABLE f1.results (resultId int PRIMARY KEY, raceId int, driverId int, constructorId int, "
                    "number text, grid int, position text, positionText text,  positionOrder int, points int, "
                    "laps int, time text, milliseconds text, fastestLap text, rank text, fastestLapTime text, "
                    "fastestLapSpeed text, statusId int, CONSTRAINT fk_races5 FOREIGN KEY(raceId) REFERENCES "
                    "f1.races(raceId), CONSTRAINT fk_constructor3 FOREIGN KEY(constructorId) REFERENCES "
                    "f1.constructors(constructorId), CONSTRAINT fk_drivers3 FOREIGN KEY(driverId) REFERENCES "
                    "f1.drivers(driverId), CONSTRAINT fk_status FOREIGN KEY(statusId) REFERENCES f1.status("
                    "statusId));")
        cur.execute("CREATE TABLE f1.pit_stops (raceId int, driverId int, stop int, lap int, time time, duration "
                    "text, milliseconds int, CONSTRAINT fk_drivers4 FOREIGN KEY(driverId) REFERENCES f1.drivers("
                    "driverId), CONSTRAINT fk_races6 FOREIGN KEY(raceId) REFERENCES f1.races(raceId));")
        cur.execute("CREATE TABLE f1.qualifying (qualifyId int PRIMARY KEY, raceId int, driverId int, constructorId "
                    "int, number int, position int, q1 text, q2 text, q3 text, CONSTRAINT fk_drivers5 FOREIGN KEY("
                    "driverId) REFERENCES f1.drivers(driverId), CONSTRAINT fk_races7 FOREIGN KEY(raceId) REFERENCES "
                    "f1.races(raceId), CONSTRAINT fk_constructor4 FOREIGN KEY(constructorId) REFERENCES "
                    "f1.constructors(constructorId));")
        cur.execute("CREATE TABLE f1.sprint_results (resultId int PRIMARY KEY, raceId int, driverId int, "
                    "constructorId int, number int, grid int, position text, positionText text,  positionOrder int, "
                    "points int, laps int, time text, milliseconds text, fastestLap text, fastestLapTime text, "
                    "statusId int, CONSTRAINT fk_drivers6 FOREIGN KEY(driverId) REFERENCES f1.drivers(driverId), "
                    "CONSTRAINT fk_races8 FOREIGN KEY(raceId) REFERENCES f1.races(raceId), CONSTRAINT fk_constructor5 "
                    "FOREIGN KEY(constructorId) REFERENCES f1.constructors(constructorId), CONSTRAINT fk_status2 "
                    "FOREIGN KEY(statusId) REFERENCES f1.status(statusId));")

        for row in df.itertuples():
            cur.execute(
                f'INSERT INTO f1.drivers VALUES ({row.driverId},\'{row.driverRef}\',\'{row.number}\',\'{row.code}\',\'{row.forename}\',\'{row.surname}\',\
            \'{row.dob}\',\'{row.nationality}\',\'{row.url}\');')
        conn.commit()

        for row in df11.itertuples():
            cur.execute(
                f'INSERT INTO f1.circuits VALUES ({row.circuitId},\'{row.circuitRef}\',\'{row.name}\',\'{row.location}\',\'{row.country}\',\'{row.lat}\',\'{row.lng}\',\'{row.alt}\', \'{row.url}\');')
            conn.commit()

        for row in df2.itertuples():
            cur.execute(
                f'INSERT INTO f1.races VALUES ({row.raceId}, {row.year}, {row.round}, {row.circuitId}, \'{row.name}\',\
             \'{row.date}\', \'{row.time}\', \'{row.url}\',\'{row.fp1_date}\', \'{row.fp1_time}\',\'{row.fp2_date}\', \'{row.fp2_time}\',\
              \'{row.fp3_date}\',\'{row.fp3_time}\',\'{row.quali_date}\',\'{row.quali_time}\', \'{row.sprint_date}\',\'{row.sprint_time}\');')
        conn.commit()

        for row in df1.itertuples():
            cur.execute(
                f'INSERT INTO f1.driver_standings VALUES ({row.driverStandingsId}, {row.raceId}, {row.driverId}, {row.points}, {row.position}, {row.positionText}, {row.wins});')
        conn.commit()

        for row in df3.itertuples():
            cur.execute(
                f'INSERT INTO f1.lap_times VALUES ({row.raceId}, {row.driverId}, {row.lap}, {row.position}, \'{row.time}\', {row.milliseconds});')
            conn.commit()

        for row in df4.itertuples():
            cur.execute(
                f'INSERT INTO f1.constructors VALUES ({row.constructorId}, \'{row.constructorRef}\', \'{row.name}\', \'{row.nationality}\', \'{row.url}\');')
            conn.commit()

        for row in df5.itertuples():
            cur.execute(f'INSERT INTO f1.seasons VALUES ({row.year}, \'{row.url}\');')
            conn.commit()

        for row in df6.itertuples():
            cur.execute(f'INSERT INTO f1.status VALUES ({row.statusId}, \'{row.status}\');')
            conn.commit()

        for row in df7.itertuples():
            cur.execute(
                f'INSERT INTO f1.constructor_results VALUES ({row.constructorResultsId}, {row.raceId}, {row.constructorId}, {row.points}, \'{row.status}\');')
            conn.commit()

        for row in df8.itertuples():
            cur.execute(
                f'INSERT INTO f1.constructor_standings VALUES ({row.constructorStandingsId}, {row.raceId}, {row.constructorId}, {row.points}, {row.position}, \'{row.positionText}\', {row.wins});')
        conn.commit()

        for row in df9.itertuples():
            cur.execute(
                f'INSERT INTO f1.results VALUES ({row.resultId}, {row.raceId}, {row.driverId}, {row.constructorId}, \'{row.number}\', {row.grid}, \'{row.position}\', \'{row.positionText}\', {row.positionOrder}, {row.points}, {row.laps},'
                f'\'{row.time}\', \'{row.milliseconds}\', \'{row.fastestLap}\', \'{row.rank}\', \'{row.fastestLapTime}\', \'{row.fastestLapSpeed}\', {row.statusId});')
            conn.commit()

        for row in df10.itertuples():
            cur.execute(
                f'INSERT INTO f1.pit_stops VALUES ({row.raceId}, {row.driverId}, {row.stop}, {row.lap}, \'{row.time}\',\'{row.duration}\', {row.milliseconds});')
        conn.commit()

        for row in df12.itertuples():
            cur.execute(
                f'INSERT INTO f1.qualifying VALUES ({row.qualifyId}, {row.raceId}, {row.driverId}, {row.constructorId}, {row.number}, {row.position}, \'{row.q1}\', \'{row.q2}\', \'{row.q3}\');')
        conn.commit()

        for row in df13.itertuples():
            cur.execute(
                f'INSERT INTO f1.sprint_results VALUES ({row.resultId}, {row.raceId}, {row.driverId}, {row.constructorId}, \'{row.number}\', {row.grid}, \'{row.position}\', \'{row.positionText}\', {row.positionOrder}, {row.points}, {row.laps},'
                f'\'{row.time}\', \'{row.milliseconds}\', \'{row.fastestLap}\', \'{row.fastestLapTime}\',  {row.statusId});')
        conn.commit()

        cur.execute("UPDATE f1.drivers SET number = NULL WHERE number = '\\N';")
        cur.execute("UPDATE f1.drivers SET code = NULL WHERE code = '\\N';")
        cur.execute("UPDATE f1.races SET time = NULL WHERE time = '\\N';")
        cur.execute("UPDATE f1.races SET fp1_date = NULL WHERE fp1_date = '\\N';")
        cur.execute("UPDATE f1.races SET fp1_time = NULL WHERE fp1_time = '\\N';")
        cur.execute("UPDATE f1.races SET fp2_date = NULL WHERE fp2_date = '\\N';")
        cur.execute("UPDATE f1.races SET fp2_time = NULL WHERE fp2_time = '\\N';")
        cur.execute("UPDATE f1.races SET fp3_date = NULL WHERE fp3_date = '\\N';")
        cur.execute("UPDATE f1.races SET fp3_time = NULL WHERE fp3_time = '\\N';")
        cur.execute("UPDATE f1.races SET quali_date = NULL WHERE quali_date = '\\N';")
        cur.execute("UPDATE f1.races SET quali_time = NULL WHERE quali_time = '\\N';")
        cur.execute("UPDATE f1.races SET sprint_date = NULL WHERE sprint_date = '\\N';")
        cur.execute("UPDATE f1.races SET sprint_time = NULL WHERE sprint_time = '\\N';")
        cur.execute("UPDATE f1.results SET position = NULL WHERE position = '\\N';")
        cur.execute("UPDATE f1.results SET time = NULL WHERE time = '\\N';")
        cur.execute("UPDATE f1.results SET milliseconds = NULL WHERE milliseconds = '\\N';")
        cur.execute("UPDATE f1.results SET fastestLap = NULL WHERE fastestLap = '\\N';")
        cur.execute("UPDATE f1.results SET rank = NULL WHERE rank = '\\N';")
        cur.execute("UPDATE f1.results SET fastestLapTime = NULL WHERE fastestLapTime = '\\N';")
        cur.execute("UPDATE f1.results SET fastestLapSpeed = NULL WHERE fastestLapSpeed = '\\N';")
        cur.execute("UPDATE f1.results SET number = NULL WHERE number = '\\N';")
        cur.execute("UPDATE f1.qualifying SET q1 = NULL WHERE q1 = '\\N';")
        cur.execute("UPDATE f1.qualifying SET q2 = NULL WHERE q2 = '\\N';")
        cur.execute("UPDATE f1.qualifying SET q3 = NULL WHERE q3 = '\\N';")
        cur.execute("UPDATE f1.qualifying SET q1 = NULL WHERE q1 = 'nan';")
        cur.execute("UPDATE f1.qualifying SET q2 = NULL WHERE q2 = 'nan';")
        cur.execute("UPDATE f1.qualifying SET q3 = NULL WHERE q3 = 'nan';")
        cur.execute("UPDATE f1.sprint_results SET position = NULL WHERE position = '\\N';")
        cur.execute("UPDATE f1.sprint_results SET time = NULL WHERE time = '\\N';")
        cur.execute("UPDATE f1.sprint_results SET milliseconds = NULL WHERE milliseconds = '\\N';")
        cur.execute("UPDATE f1.sprint_results SET fastestLap = NULL WHERE fastestLap = '\\N';")
        cur.execute("UPDATE f1.sprint_results SET fastestLapTime = NULL WHERE fastestLapTime = '\\N';")

        cur.execute("ALTER TABLE f1.drivers \
                     ALTER COLUMN number TYPE INT \
                     USING number::integer;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN time TYPE time \
                     USING time::time;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN fp1_time TYPE time \
                     USING fp1_time::time;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN fp2_time TYPE time \
                     USING fp2_time::time;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN fp3_time TYPE time \
                     USING fp3_time::time;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN quali_time TYPE time \
                     USING quali_time::time;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN sprint_time TYPE time \
                     USING sprint_time::time;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN date TYPE date \
                     USING date::date;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN fp1_date TYPE date \
                     USING fp1_date::date;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN fp2_date TYPE date \
                     USING fp2_date::date;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN quali_date TYPE date \
                     USING quali_date::date;")
        cur.execute("ALTER TABLE f1.races \
                     ALTER COLUMN sprint_date TYPE date \
                     USING sprint_date::date;")
        cur.execute("ALTER TABLE f1.results \
                     ALTER COLUMN position TYPE int \
                     USING position::integer;")
        cur.execute("ALTER TABLE f1.results \
                     ALTER COLUMN milliseconds TYPE int \
                     USING milliseconds::integer;")
        cur.execute("ALTER TABLE f1.results \
                     ALTER COLUMN number TYPE INT \
                     USING number::integer;")
        cur.execute("ALTER TABLE f1.results \
                     ALTER COLUMN fastestLap TYPE INT \
                     USING fastestLap::integer;")
        cur.execute("ALTER TABLE f1.results \
                     ALTER COLUMN rank TYPE INT \
                     USING rank::integer;")
        cur.execute("ALTER TABLE f1.results \
                     ALTER COLUMN fastestLapTime TYPE time \
                     USING fastestLapTime::time;")
        cur.execute("ALTER TABLE f1.results \
                     ALTER COLUMN fastestLapSpeed TYPE decimal \
                     USING fastestLapSpeed::decimal;")
        cur.execute("ALTER TABLE f1.qualifying \
                     ALTER COLUMN q1 TYPE time \
                     USING q1::time;")
        cur.execute("ALTER TABLE f1.qualifying \
                     ALTER COLUMN q2 TYPE time \
                     USING q2::time;")
        cur.execute("ALTER TABLE f1.qualifying \
                     ALTER COLUMN q3 TYPE time \
                     USING q3::time;")
        cur.execute("ALTER TABLE f1.sprint_results \
                     ALTER COLUMN position TYPE int \
                     USING position::integer;")
        cur.execute("ALTER TABLE f1.sprint_results \
                     ALTER COLUMN milliseconds TYPE int \
                     USING milliseconds::integer;")
        cur.execute("ALTER TABLE f1.sprint_results \
                     ALTER COLUMN fastestLap TYPE int \
                     USING fastestLap::integer;")
        cur.execute("ALTER TABLE f1.sprint_results \
                     ALTER COLUMN fastestLapTime TYPE time \
                     USING fastestLapTime::time;")

        conn.commit()
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

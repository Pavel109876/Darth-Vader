import webbrowser
import requests
import zipfile
import psycopg2
# import time
import urllib.request
import os
from gui import host_value

url = "https://ergast.com/downloads/f1db_csv.zip"
urllib.request.urlretrieve(url)


# res = requests.get('https://ergast.com/downloads/f1db_csv.zip')
# res.content

# with open("f1db_csv.zip", "wb") as code:
#     code.write(res.content)
#
# webbrowser.open('https://ergast.com/downloads/f1db_csv.zip')
# fantasy_zip = zipfile.ZipFile('D:\\f1db_csv.zip')
# fantasy_zip.extractall('D:\\test')
# fantasy_zip.close()
# time.sleep(5)

with zipfile.ZipFile("C:\\Users\\Pavel.Isachanka\\Downloads\\f1db_csv.zip", 'r') as zip_ref:
    zip_ref.extractall("D:\\test")

try:
    conn = psycopg2.connect(database="postgres", user="postgres", password="postgres", host=host_value, port="5432")
except:
    print("I can't connect to database!")

cur = conn.cursor()

try:
    cur.execute("DROP TABLE IF EXISTS public.drivers;")
    cur.execute("CREATE TABLE drivers (driverId int, driverRef text, number text, code text, forename text, surname text, dob date, nationality text, url text);")
    cur.execute("COPY public.drivers FROM 'D:/test/drivers.csv' DELIMITER ',' csv header;")
    cur.execute("UPDATE public.drivers SET number = NULL WHERE number = '\\N';")
    cur.execute("UPDATE public.drivers SET code = NULL WHERE code = '\\N';")
    cur.execute("   ALTER TABLE public.drivers \
                    ALTER COLUMN number TYPE INT \
                    USING number::integer;")
except:
    print("I can't drop our test database!")


conn.commit() # <--- makes sure the change is shown in the database
conn.close()
cur.close()

path = "C:\\Users\\Pavel.Isachanka\\Downloads\\f1db_csv.zip"
os.remove(path)

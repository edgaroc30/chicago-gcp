import logging
from logging.handlers import RotatingFileHandler
import pandas_gbq
from dotenv import load_dotenv
import os
load_dotenv()

logfile = 'chicago.log'
handler = RotatingFileHandler(logfile, 
    maxBytes=5*1024*1024,
    mode='a', 
    backupCount=2,
    encoding=None,
    delay=0)

logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s %(lineno)d:%(message)s',
    datefmt='%Y/%m/%d %H:%M:%S ',
    handlers = [handler],
    level=logging.DEBUG)

sql = """
SELECT * FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` LIMIT 1000'
"""


df = pandas_gbq.read_gbq(sql, project_id=os.environ.get("PROJECT_ID"))
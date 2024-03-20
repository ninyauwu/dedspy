import pathlib
import pyodbc
from pathlib import Path
from loguru import logger


class Settings:
    baseDir = Path.cwd()
    rawDir = Path("raw_data")
    processedDir = Path("processed_data")
    logDir = baseDir / "log"

    DB = {"servername": "MSI\\SQLEXPRESS",
          "database": "DEDSDatabase1"}

    export_conn = pyodbc.connect('DRIVER={SQL SERVER};SERVER=' + DB['servername'] +
                                 ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')
    export_cursor = export_conn.cursor()
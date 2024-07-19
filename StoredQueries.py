import math

import pandas as pd
import pyodbc

import VehiclePresencePreprocessing


def db_connection():
    return '''DRIVER={ODBC Driver 17 for SQL Server};
        SERVER=VMDEV1\\SQLEXPRESS;
        DATABASE=CRUSH_1102;
        Trusted_Connection=yes;'''


def part_sales():

    import PartSalesPreprocessing as PSP

    # Define your connection string
    conn_str = db_connection()

    conn = pyodbc.connect(conn_str)

    cursor = conn.cursor()

    cursor.execute("""
    SELECT
        Parts.LongName,
        COUNT(Qty) AS NumberOfPartsSold,
        SUM(PosSalesDtl.Qty * PosSalesDtl.UnitPrice) AS TotalPrice,
        CAST(PosSalesDtl.CreDtTm AS DATE) AS DateOfSale
    FROM [CRUSH_1102].[dbo].[PosSalesDtl]
    JOIN [CRUSH_1102].[dbo].[Parts] ON Parts.PartRno = PosSalesDtl.PartRno
    WHERE PosSalesDtl.PartRno NOT LIKE 285 --GATE FEE ID
    GROUP BY Parts.LongName, CAST(PosSalesDtl.CreDtTm AS DATE)
    ORDER BY DateOfSale
""")

    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    part_sales_df = pd.DataFrame.from_records(rows, columns=columns)

    conn.close()

    # Group all sales by day, ignoring category.
    part_sales_df = PSP.sum_part_sales_by_category_by_day(part_sales_df)

    return part_sales_df


def vehicle_info():

    import VehicleInfoPreprocessing as VIP

    # Define your connection string
    conn_str = db_connection()

    conn = pyodbc.connect(conn_str)

    cursor = conn.cursor()

    # Execute a sample query
    cursor.execute("""SELECT
      [Year]
      ,[Make]
      ,[Model]
      ,[Color]
	  ,CAST(Vehicles.[YardDtTm] AS DATE) AS YardEnterDate
	  ,CAST(Vehicles.[CrushDtTm] AS DATE) AS YardRemovalDate --Crush
   
  FROM [CRUSH_1102].[dbo].[Vehicles]
  ORDER BY YardDtTm""")

    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    vehicle_info_df = pd.DataFrame.from_records(rows, columns=columns)

    conn.close()

    # Fill in the records when either YardEnterDate or YardRemovalDate is missing
    vehicle_info_df = VIP.impute_yard_days(vehicle_info_df)

    # Drop Records where both YardEnterDate AND YardRemovalDate are missing.
    vehicle_info_df = VIP.remove_missing_vehicle_days(vehicle_info_df)

    return vehicle_info_df




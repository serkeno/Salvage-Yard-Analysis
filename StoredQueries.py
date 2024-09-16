import pandas as pd
import pyodbc

def db_connection():
    """
    Returns a connection string to the database.
    :return:
    """
    return '''DRIVER={ODBC Driver 17 for SQL Server};
        SERVER=MSI;
        DATABASE=CRUSH_1102;
        Trusted_Connection=yes;'''


def part_sales():
    """
    Pulls part sale data and transforms it into a pandas dataframe, removing the GATE FEE. This is intended to be used for analyzing basic information about
    part sales, such as
    :return: Pandas Dataframe
    """

    # Define your connection string
    conn_str = db_connection()

    conn = pyodbc.connect(conn_str)

    cursor = conn.cursor()

    cursor.execute("""
SELECT
    
	  COUNT(Qty) AS NumberOfPartsSold
	  ,SUM(PosSalesDtl.UnitPrice) AS TotalPrice
	  ,CAST(PosSalesDtl.CreDtTm AS DATE) AS DateOfSale

  FROM [CRUSH_1102].[dbo].[PosSalesDtl]
  JOIN [CRUSH_1102].[dbo].[Parts] ON Parts.PartRno = PosSalesDtl.PartRno
    WHERE PosSalesDtl.PartRno NOT LIKE 285 --GATE FEE ID
	GROUP BY CAST(PosSalesDtl.CreDtTm AS DATE), CAST(PosSalesDtl.CreDtTm AS DATE), CAST(PosSalesDtl.CreDtTm AS DATE)
	ORDER BY DateOfSale
""")

    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    part_sales_df = pd.DataFrame.from_records(rows, columns=columns)

    conn.close()

    return part_sales_df


def vehicle_info():
    """
    Creates a Pandas Dataframe from the Vehicles table in the CRUSH database. It is intended for use in analysis
    of vehicle presence on the lot, including the total number of Yard-Days for each vehicle type.
    :return: Pandas Dataframe
    """
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

def POS_transactions():
    """
    Creates a Pandas Dataframe from the Parts and PosSalesDtl tables in the CRUSH database. Intended to be used in analysis
    of POS_transactions, such as finding the number of each item sold, average price of each item type, and which types
    of items are most likely to be sold together.
    :return: Pandas Dataframe
    """
    conn_str = db_connection()

    conn = pyodbc.connect(conn_str)

    cursor = conn.cursor()

    cursor.execute("""
   SELECT p.[PartRno]
      ,p.[PartId]
      ,p.[LongName] as "PartName"
      ,s.[PosSaleDtlRno] as "LineItemID"
      ,s.[PosSaleHdrRno] as "TransactionID"
      ,s.[Seq]
      ,s.[PartPriceRno]
      ,s.[Qty]
      ,s.[UnitPrice]
      ,s.[ExtPrice]
      ,s.[CreDtTm]
FROM [CRUSH_1102].[dbo].[Parts] p
JOIN [CRUSH_1102].[dbo].[PosSalesDtl] s
ON p.[PartRno] = s.[PartRno];
    """)

    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    POS_df = pd.DataFrame.from_records(rows, columns=columns)

    conn.close()

    return POS_df

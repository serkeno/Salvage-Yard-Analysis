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


def vehicle_presence(presence_type="binary", vehicle_type="model", feature_ratio=100):
    """

    :param feature_ratio: The feature ratio is either 'None' to indicate that no ratio should be applied to the number
    of feature columns to records in the dataframe. If a positive integer is given it will be sent to the
    VehiclePresencePreprocessing.select_top_models() method in the form of records/feature_ratio.
    :param presence_type: Determines the type of presence used, default is binary, where the dataframe will only
    indicate whether a vehicle type is present on each day. Alternatively, you can select 'continuous' which will cause
    the dataframe returned to keep track of the number of each vehicle type on the yard each day.
    :param vehicle_type: Determines the type of vehicle columns used, either the default as model, which will use only
    the vehicle model as features, or year_model, which will use a combined feature of the year and model for each
    feature.
    :return: Returns a dataframe for use in the ML Model, where a number of features created by the presence_type and
    vehicle_type are used to create feature columns and are then merged with the NumberOfPartsSold column from
    part_sales dataframe from the part_sales() method defined in StoredQueries.py on the Date column.
    """
    import VehicleInfoPreprocessing as VIP
    # vehicle_info_df = vehicle_info()
    # part_sales_df = part_sales()

    vehicle_info_df = pd.read_csv("vehicle_info.csv")
    part_sales_df = pd.read_csv("Part_sales_by_day.csv")

    # Group all sales by day, ignoring category.
    # part_sales_df = PSP.sum_part_sales_by_category_by_day(part_sales_df)

    # Fill in the records when either YardEnterDate or YardRemovalDate is missing
    vehicle_info_df = VIP.impute_yard_days(vehicle_info_df)

    # Drop Records where both YardEnterDate AND YardRemovalDate are missing.
    vehicle_info_df = VIP.remove_missing_vehicle_days(vehicle_info_df)

    if feature_ratio != "None" and isinstance(feature_ratio, int) is False or isinstance(feature_ratio,
                                                                                         int) is True and feature_ratio <= 0:
        raise ValueError("Feature ratio must be either 'None' or a positive integer.")

    valid_presence_type_parameters = ["binary", "continuous"]

    valid_vehicle_type_parameters = ["model", "year_model"]

    if presence_type not in valid_presence_type_parameters:
        raise ValueError(f"presence_type must be one of the following{valid_presence_type_parameters}")

    if vehicle_type not in valid_vehicle_type_parameters:
        raise ValueError(f"vehicle_type must be one of the following{valid_vehicle_type_parameters}")

    # Determine if model or YearModel should be used based on the vehicle_type parameter
    if vehicle_type == "year_model":
        # Create the YearModel attribute
        vehicle_info_df['YearModel'] = vehicle_info_df['Year'].astype(str) + '_' + vehicle_info_df['Model']

        # Overwrite the Model attribute with YearModel
        vehicle_info_df['Model'] = vehicle_info_df['YearModel']

    # Step 1: Create a date range
    start_date = vehicle_info_df['YardEnterDate'].min().normalize()
    end_date = vehicle_info_df['YardEnterDate'].max().normalize()
    date_range = pd.date_range(start=start_date, end=end_date)

    # Step 2: Initialize the dataframe with the date range
    presence_df = pd.DataFrame(date_range, columns=['Date'])

    # Step 3: Prepare columns for each unique model and set initial values to 0
    unique_models = vehicle_info_df['Model'].unique()
    model_columns = pd.DataFrame(0, index=presence_df.index, columns=unique_models)

    # Step 4: Concatenate the date and model columns
    presence_df = pd.concat([presence_df, model_columns], axis=1)

    # Step 5: Update values based on vehicle presence
    if presence_type == "binary":
        for _, row in vehicle_info_df.iterrows():
            model = row['Model']
            enter_date = pd.to_datetime(row['YardEnterDate']).normalize()
            removal_date = pd.to_datetime(row['YardRemovalDate']).normalize()

            # Check presence and update the values
            presence_df.loc[(presence_df['Date'] >= enter_date) & (presence_df['Date'] <= removal_date), model] = 1

        # De-fragment the dataframe
        presence_df = presence_df.copy()

    elif presence_type == "continuous":
        for _, row in vehicle_info_df.iterrows():
            model = row['Model']
            enter_date = pd.to_datetime(row['YardEnterDate']).normalize()
            removal_date = pd.to_datetime(row['YardRemovalDate']).normalize()

            # Check presence and update the values
            presence_df.loc[(presence_df['Date'] >= enter_date) & (presence_df['Date'] <= removal_date), model] += 1

        # De-fragment the dataframe
        presence_df = presence_df.copy()

    # Ensure the 'Date' column in part_sales_df is of datetime type
    part_sales_df['Date'] = pd.to_datetime(part_sales_df['Date']).dt.normalize()

    # Combine presence with part_sales based on Date
    merged_df = pd.merge(presence_df, part_sales_df, on='Date', how='left')

    # Remove dates that have no parts sold or no total price
    cleaned_df = merged_df.dropna(subset=['TotalPartsSold', 'TotalPrice'])

    # Return the dataframe without adjusting record to feature ratios
    if feature_ratio == "None":
        return cleaned_df
    # Filter the dataframe feature columns to fit a specific ratio
    else:
        # Select Top models to improve record to feature ratio
        filtered_df = VehiclePresencePreprocessing.select_top_models(cleaned_df)

        return filtered_df

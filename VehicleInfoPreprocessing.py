import pandas as pd
def impute_yard_days(vehicle_info_df):
    """

    :return: Returns a vehicle_info dataframe with YardEnterDate and YardRemovalDate values filled in based on average
    days on lot. If only YardEnterDate exists, but YardRemovalDate is null, it will calculate the average days complete
    records exist on the lot and impute that as the missing value. Works vice versa for the opposite scenario.
    """
    vehicle_info_df = vehicle_info_df

    # Convert date columns to datetime format
    vehicle_info_df['YardEnterDate'] = pd.to_datetime(vehicle_info_df['YardEnterDate'], errors='coerce')
    vehicle_info_df['YardRemovalDate'] = pd.to_datetime(vehicle_info_df['YardRemovalDate'], errors='coerce')

    # Calculate the duration on yard for records with both dates available
    vehicle_info_df['DaysOnYard'] = (vehicle_info_df['YardRemovalDate'] - vehicle_info_df['YardEnterDate']).dt.days

    # Calculate the average days on yard
    average_days_on_yard = vehicle_info_df['DaysOnYard'].mean()

    # Impute missing YardRemovalDate
    vehicle_info_df.loc[
        vehicle_info_df['YardRemovalDate'].isna() & vehicle_info_df['YardEnterDate'].notna(), 'YardRemovalDate'] = \
        vehicle_info_df['YardEnterDate'] + pd.to_timedelta(average_days_on_yard, unit='d')

    # Impute missing YardEnterDate
    vehicle_info_df.loc[
        vehicle_info_df['YardEnterDate'].isna() & vehicle_info_df['YardRemovalDate'].notna(), 'YardEnterDate'] = \
        vehicle_info_df['YardRemovalDate'] - pd.to_timedelta(average_days_on_yard, unit='d')

    return vehicle_info_df


def remove_missing_vehicle_days(vehicle_info_df):
    """

    :param vehicle_info_df:
    :return: return a vehicle_info_df where records with neither a YardEnterDate nor YardRemovalDate exist.
    """
    vehicle_info_df = vehicle_info_df

    vehicle_info_df = vehicle_info_df.dropna(subset=['YardEnterDate', 'YardRemovalDate'], how='all')

    return vehicle_info_df

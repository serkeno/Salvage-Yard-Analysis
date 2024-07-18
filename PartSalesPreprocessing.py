import pandas as pd
def sum_part_sales_by_category_by_day(part_sales_df):

    part_sales_df = part_sales_df

    # Convert DateOfSale to datetime format
    part_sales_df['DateOfSale'] = pd.to_datetime(part_sales_df['DateOfSale'], errors='coerce')

    # Group by DateOfSale and sum the NumberOfPartsSold
    total_parts_sold_per_day = part_sales_df.groupby(part_sales_df['DateOfSale'].dt.date)[
        'NumberOfPartsSold'].sum().reset_index()

    # Rename the columns for clarity
    total_parts_sold_per_day.columns = ['Date', 'TotalPartsSold']

    # Convert Date column back to datetime format
    total_parts_sold_per_day['Date'] = pd.to_datetime(total_parts_sold_per_day['Date'])

    return total_parts_sold_per_day
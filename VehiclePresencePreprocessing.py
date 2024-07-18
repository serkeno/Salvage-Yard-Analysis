import math


def select_top_models(cleaned_df):
    """

    :param cleaned_df: The cleaned dataframe is a dataframe taken from any of the dataframe creation methods used to
    create preprocessed dataframes for JSON files. It is the second to last step before the filtered dataframe, which
    is returned as the final product before sending it to JSONFileBuilder.build_file().
    :return: The return is a dataframe in which the number of models or YearModels as column is reduced to match 1/10 of
    the number of records. As a general rule of thumb ML models should have 100 records for each feature used in
    the prediction.
    """
    # Step 1: Calculate the popularity of each model by summing the columns
    model_popularity = cleaned_df.drop(columns=['Date', 'TotalPartsSold']).sum().sort_values(ascending=False)

    # Step 2: Determine the number of top models to select (1/100th of the number of records)
    num_records = cleaned_df.shape[0]
    num_top_models = max(1, math.floor(num_records // 100))  # Ensure at least 1 model is selected

    # Step 3: Identify the top models based on the calculated number
    top_models = model_popularity.head(num_top_models).index.tolist()

    # Step 4: Keep only the 'Date', 'TotalPartsSold', and top models
    filtered_df = cleaned_df[['Date', 'TotalPartsSold'] + top_models]

    return filtered_df

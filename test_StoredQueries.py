import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from StoredQueries import part_sales, vehicle_info, POS_transactions
from VehicleInfoPreprocessing import impute_yard_days, remove_missing_vehicle_days
class TestStoredQueries(unittest.TestCase):

    @patch('StoredQueries.pyodbc.connect')
    def test_part_sales(self, mock_connect):
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Mock the connection to return the mock cursor
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock the cursor to return example data
        mock_cursor.fetchall.return_value = [
            (5, 500.0, '2024-09-01'),
            (3, 300.0, '2024-09-02')
        ]
        mock_cursor.description = [('NumberOfPartsSold',), ('TotalPrice',), ('DateOfSale',)]

        # Call the function
        result = part_sales()

        # Check the dataframe structure
        expected_df = pd.DataFrame({
            'NumberOfPartsSold': [5, 3],
            'TotalPrice': [500.0, 300.0],
            'DateOfSale': ['2024-09-01', '2024-09-02']
        })

        pd.testing.assert_frame_equal(result, expected_df)

        # Ensure the connection was closed
        mock_conn.close.assert_called_once()

    @patch('VehicleInfoPreprocessing.remove_missing_vehicle_days')
    @patch('VehicleInfoPreprocessing.impute_yard_days')
    @patch('StoredQueries.pyodbc.connect')
    def test_vehicle_info(self, mock_connect, mock_remove_missing, mock_impute):
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Mock the connection to return the mock cursor
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock the cursor to return example data
        mock_cursor.fetchall.return_value = [
            (2005, 'Toyota', 'Camry', 'Red', '2024-01-01', '2024-01-10'),
            (2010, 'Honda', 'Civic', 'Blue', '2024-02-01', '2024-02-10')
        ]
        mock_cursor.description = [
            ('Year',), ('Make',), ('Model',), ('Color',), ('YardEnterDate',), ('YardRemovalDate',)
        ]

        # Mock the impute_yard_days and remove_missing_vehicle_days functions to return the dataframe without changes
        mock_impute.side_effect = lambda df: df
        mock_remove_missing.side_effect = lambda df: df

        # Call the function
        result = vehicle_info()

        # Check the dataframe structure
        expected_df = pd.DataFrame({
            'Year': [2005, 2010],
            'Make': ['Toyota', 'Honda'],
            'Model': ['Camry', 'Civic'],
            'Color': ['Red', 'Blue'],
            'YardEnterDate': ['2024-01-01', '2024-02-01'],
            'YardRemovalDate': ['2024-01-10', '2024-02-10']
        })

        pd.testing.assert_frame_equal(result, expected_df)

        # Ensure the connection was closed
        mock_conn.close.assert_called_once()

    @patch('StoredQueries.pyodbc.connect')
    def test_POS_transactions(self, mock_connect):
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Mock the connection to return the mock cursor
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock the cursor to return example data
        mock_cursor.fetchall.return_value = [
            (1, 'Part001', 'Part Name 1', 101, 201, 1, 1, 1, 10.0, 10.0, '2024-09-01 00:00:00'),
            (2, 'Part002', 'Part Name 2', 102, 202, 2, 2, 2, 20.0, 40.0, '2024-09-02 00:00:00')
        ]
        mock_cursor.description = [
            ('PartRno',), ('PartId',), ('PartName',), ('LineItemID',), ('TransactionID',),
            ('Seq',), ('PartPriceRno',), ('Qty',), ('UnitPrice',), ('ExtPrice',), ('CreDtTm',)
        ]

        # Call the function
        result = POS_transactions()

        # Check the dataframe structure
        expected_df = pd.DataFrame({
            'PartRno': [1, 2],
            'PartId': ['Part001', 'Part002'],
            'PartName': ['Part Name 1', 'Part Name 2'],
            'LineItemID': [101, 102],
            'TransactionID': [201, 202],
            'Seq': [1, 2],
            'PartPriceRno': [1, 2],
            'Qty': [1, 2],
            'UnitPrice': [10.0, 20.0],
            'ExtPrice': [10.0, 40.0],
            'CreDtTm': ['2024-09-01 00:00:00', '2024-09-02 00:00:00']
        })

        pd.testing.assert_frame_equal(result, expected_df)

        # Ensure the connection was closed
        mock_conn.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()


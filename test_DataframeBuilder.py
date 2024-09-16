import unittest
import pandas as pd
from DataframeBuilder import vehicle_presence


class TestVehiclePresence(unittest.TestCase):

    def setUp(self):
        # This is where you would mock data or set up any initial state for the tests
        # You can use pd.DataFrame or mock library to create test data
        # Since the actual CSV files are not available, I'm using hypothetical dataframes

        self.vehicle_info_mock = pd.DataFrame({
            'Model': ['ModelA', 'ModelB'],
            'Year': [2005, 2010],
            'YardEnterDate': ['2022-01-01', '2022-02-01'],
            'YardRemovalDate': ['2022-01-10', '2022-02-10']
        })

        self.part_sales_mock = pd.DataFrame({
            'Date': ['2022-01-01', '2022-01-02', '2022-02-01'],
            'TotalPartsSold': [5, 3, 8],
            'TotalPrice': [500, 300, 800]
        })

        # Patching the read_csv function to return the mock data
        pd.read_csv = lambda file_path: self.vehicle_info_mock if 'vehicle_info' in file_path else self.part_sales_mock

    def test_invalid_presence_type(self):
        with self.assertRaises(ValueError):
            vehicle_presence(presence_type="invalid", vehicle_type="model")

    def test_invalid_vehicle_type(self):
        with self.assertRaises(ValueError):
            vehicle_presence(presence_type="binary", vehicle_type="invalid")

    def test_invalid_target_type(self):
        with self.assertRaises(ValueError):
            vehicle_presence(target_type="invalid")

    def test_binary_presence(self):
        df = vehicle_presence(presence_type="binary", vehicle_type="model", target_type="TotalPartsSold")
        self.assertIn('ModelA', df.columns)
        self.assertIn('ModelB', df.columns)
        self.assertIn('Date', df.columns)
        self.assertIn('TotalPartsSold', df.columns)

        # Check if the binary presence is correctly calculated
        self.assertEqual(df.loc[df['Date'] == '2022-01-05', 'ModelA'].values[0], 1)
        self.assertEqual(df.loc[df['Date'] == '2022-01-05', 'ModelB'].values[0], 0)

    def test_continuous_presence(self):
        df = vehicle_presence(presence_type="continuous", vehicle_type="model", target_type="TotalPartsSold")
        self.assertIn('ModelA', df.columns)
        self.assertIn('ModelB', df.columns)
        self.assertIn('Date', df.columns)
        self.assertIn('TotalPartsSold', df.columns)

        # Check if the continuous presence is correctly calculated
        self.assertEqual(df.loc[df['Date'] == '2022-01-05', 'ModelA'].values[0], 1)
        self.assertEqual(df.loc[df['Date'] == '2022-01-05', 'ModelB'].values[0], 0)


if __name__ == '__main__':
    unittest.main()


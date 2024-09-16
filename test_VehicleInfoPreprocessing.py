import unittest
import pandas as pd
from VehicleInfoPreprocessing import impute_yard_days, remove_missing_vehicle_days

class TestVehicleInfoPreprocessing(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.vehicle_data = pd.DataFrame({
            'YardEnterDate': ['2024-01-01', '2024-02-01', None, '2024-03-01'],
            'YardRemovalDate': ['2024-01-10', None, '2024-03-10', None]
        })

    def test_impute_yard_days(self):
        # Call the function
        result = impute_yard_days(self.vehicle_data.copy())

        # Expected result after imputation
        expected_data = pd.DataFrame({
            'YardEnterDate': ['2024-01-01', '2024-02-01', '2024-03-05', '2024-03-01'],
            'YardRemovalDate': ['2024-01-10', '2024-02-10', '2024-03-10', '2024-03-10'],
            'DaysOnYard': [9.0, 9.0, 5.0, 9.0]
        })

        # Check if the result matches the expected output
        pd.testing.assert_frame_equal(result, expected_data)

    def test_remove_missing_vehicle_days(self):
        # Call the function
        result = remove_missing_vehicle_days(self.vehicle_data.copy())

        # Expected result after removing rows with both dates missing
        expected_data = pd.DataFrame({
            'YardEnterDate': ['2024-01-01', '2024-02-01', None, '2024-03-01'],
            'YardRemovalDate': ['2024-01-10', None, '2024-03-10', None]
        }).dropna(subset=['YardEnterDate', 'YardRemovalDate'], how='all')

        # Check if the result matches the expected output
        pd.testing.assert_frame_equal(result, expected_data)

if __name__ == '__main__':
    unittest.main()


import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import json
import pandas as pd
from JSONFileBuilder import build_file, build_binary_vehicle_presence, build_continuous_vehicle_presence


class TestJSONFileBuilder(unittest.TestCase):

    @patch('JSONFileBuilder.datetime')
    @patch('JSONFileBuilder.os.makedirs')
    @patch('JSONFileBuilder.open', new_callable=mock_open)
    def test_build_file(self, mock_file, mock_makedirs, mock_datetime):
        # Mock the current date
        mock_datetime.now.return_value.strftime.return_value = '2024-09-02'

        # Create a sample dataframe
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

        # Call the function
        build_file(df, name="TestFile")

        # Check that the correct directory was created
        mock_makedirs.assert_called_once_with(os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls'),
                                              exist_ok=True)

        # Check that the file was opened with the correct path
        file_name = os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls', 'TestFile_2024-09-02.json')
        mock_file.assert_called_once_with(file_name, 'w')

        # Check that the correct JSON content was written
        expected_json = df.to_json()
        mock_file().write.assert_called_once_with(json.dumps(json.loads(expected_json), indent=4))

    @patch('JSONFileBuilder.StoredQueries.binary_vehicle_presence')
    @patch('JSONFileBuilder.datetime')
    @patch('JSONFileBuilder.os.makedirs')
    @patch('JSONFileBuilder.open', new_callable=mock_open)
    def test_build_binary_vehicle_presence(self, mock_file, mock_makedirs, mock_datetime, mock_query):
        # Mock the current date
        mock_datetime.now.return_value.strftime.return_value = '2024-09-02'

        # Mock the stored query
        df = pd.DataFrame({'model': ['ModelA'], 'count': [1]})
        mock_query.return_value = df

        # Call the function
        build_binary_vehicle_presence()

        # Check that the correct directory was created
        mock_makedirs.assert_called_once_with(os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls'),
                                              exist_ok=True)

        # Check that the file was opened with the correct path
        file_name = os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls',
                                 'binary_vehicle_presence2024-09-02.json')
        mock_file.assert_called_once_with(file_name, 'w')

        # Check that the correct JSON content was written
        expected_json = df.to_json()
        mock_file().write.assert_called_once_with(json.dumps(json.loads(expected_json), indent=4))

    @patch('JSONFileBuilder.StoredQueries.continuous_vehicle_presence')
    @patch('JSONFileBuilder.datetime')
    @patch('JSONFileBuilder.os.makedirs')
    @patch('JSONFileBuilder.open', new_callable=mock_open)
    def test_build_continuous_vehicle_presence(self, mock_file, mock_makedirs, mock_datetime, mock_query):
        # Mock the current date
        mock_datetime.now.return_value.strftime.return_value = '2024-09-02'

        # Mock the stored query
        df = pd.DataFrame({'model': ['ModelA'], 'count': [10]})
        mock_query.return_value = df

        # Call the function
        build_continuous_vehicle_presence()

        # Check that the correct directory was created
        mock_makedirs.assert_called_once_with(os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls'),
                                              exist_ok=True)

        # Check that the file was opened with the correct path
        file_name = os.path.join(os.path.expanduser('~'), 'Desktop', 'Database Pulls',
                                 'continuous_vehicle_presence2024-09-02.json')
        mock_file.assert_called_once_with(file_name, 'w')

        # Check that the correct JSON content was written
        expected_json = df.to_json()
        mock_file().write.assert_called_once_with(json.dumps(json.loads(expected_json), indent=4))


if __name__ == '__main__':
    unittest.main()


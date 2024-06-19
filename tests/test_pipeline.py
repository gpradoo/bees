import unittest
from unittest.mock import patch, mock_open, MagicMock
import requests
import os
import json
import pandas as pd
from datetime import datetime
from dags.pipeline import process_layer_bronze, process_layer_silver, process_layer_gold, get_latest_file

class TestPipeline(unittest.TestCase):

    @patch('dags.pipeline.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    @patch('dags.pipeline.os.makedirs')
    def test_process_layer_bronze(self, mock_makedirs, mock_open, mock_requests_get):
        # Mocking API response
        mock_requests_get.return_value.status_code = 200
        mock_requests_get.return_value.json.return_value = {'key': 'value'}

        with patch('builtins.open', mock_open()) as mocked_open:
            output_dir_bronze = 'dags/bronze/'
            current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(output_dir_bronze, f'raw_data_{current_time}.json')

            process_layer_bronze()

            mock_requests_get.assert_called_once_with('https://api.openbrewerydb.org/breweries')
            
            # Verify that open was called with the expected file path and write mode
            mocked_open.assert_called_once_with(output_file, 'w')
            self.assertTrue(os.path.exists('dags/bronze/'))

    @patch('dags.pipeline.get_latest_file')
    @patch('dags.pipeline.pd.read_json')
    @patch('dags.pipeline.pd.DataFrame.to_parquet')
    @patch('dags.pipeline.os.makedirs')
    def test_process_layer_silver(self, mock_makedirs, mock_to_parquet, mock_read_json, mock_get_latest_file):
        mock_get_latest_file.return_value = 'dags/bronze/mock_file.json'
        mock_read_json.return_value = pd.DataFrame({'country': ['us'], 'state': ['ca'], 'city': ['sf'], 'brewery_type': ['micro']})
        
        process_layer_silver()
        
        mock_get_latest_file.assert_called_once_with('dags/bronze/')
        mock_read_json.assert_called_once_with('dags/bronze/mock_file.json')
        mock_to_parquet.assert_called()
        self.assertTrue(os.path.exists('dags/silver/'))

    @patch('dags.pipeline.get_latest_file')
    @patch('dags.pipeline.pd.read_parquet')
    @patch('dags.pipeline.pd.DataFrame.to_parquet')
    @patch('dags.pipeline.os.makedirs')
    def test_process_layer_gold(self, mock_makedirs, mock_to_parquet, mock_read_parquet, mock_get_latest_file):
        mock_get_latest_file.return_value = 'dags/silver/mock_file.parquet'
        mock_read_parquet.return_value = pd.DataFrame({
            'brewery_type': ['micro'], 'country': ['us'], 'state_province': ['ca'], 'city': ['sf']
        })
        
        process_layer_gold()
        
        mock_get_latest_file.assert_called_once_with('dags/silver/', '.parquet')
        mock_read_parquet.assert_called_once_with('dags/silver/mock_file.parquet')
        mock_to_parquet.assert_called()
        self.assertTrue(os.path.exists('dags/gold/'))
        
if __name__ == '__main__':
    unittest.main()

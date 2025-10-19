
import unittest
import pandas as pd
from pyspark.sql import SparkSession
import os
import shutil

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from transformaciones import run_transformations

class TestTransformaciones(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up a Spark session for the tests."""
        cls.spark = SparkSession.builder \
            .appName("TestSegurosTransformations") \
            .master("local[*]") \
            .getOrCreate()

        # Create dummy CSV files for testing
        cls.test_dir = "/tmp/spark_test_data"
        os.makedirs(cls.test_dir, exist_ok=True)
        cls.polizas_path = os.path.join(cls.test_dir, "polizas.csv")
        cls.siniestros_path = os.path.join(cls.test_dir, "siniestros.csv")
        cls.output_path = os.path.join(cls.test_dir, "output")

        polizas_data = {'poliza_id': ['P01', 'P02'], 'producto': ['AUTO', 'VIDA'],
                        'suma_asegurada': [1000, 2000], 'prima_mensual': [100, 200]}
        siniestros_data = {'siniestro_id': ['S01'], 'poliza_id': ['P01'],
                           'monto_reclamado': [500]}

        pd.DataFrame(polizas_data).to_csv(cls.polizas_path, index=False)
        pd.DataFrame(siniestros_data).to_csv(cls.siniestros_path, index=False)

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session and clean up files."""
        cls.spark.stop()
        if os.path.exists(cls.test_dir):
            shutil.rmtree(cls.test_dir)

    def test_run_transformations(self):
        """Test the main transformation logic."""
        run_transformations(self.spark, self.polizas_path, self.siniestros_path, self.output_path)

        # Read the output Parquet file
        result_df = self.spark.read.parquet(self.output_path)

        self.assertEqual(result_df.count(), 2)

        # Check aggregations for 'AUTO' product
        auto_row = result_df.filter(result_df.producto == 'AUTO').collect()[0]
        self.assertEqual(auto_row['numero_polizas'], 1)
        self.assertEqual(auto_row['numero_siniestros'], 1)
        self.assertEqual(auto_row['total_monto_reclamado'], 500)

        # Check aggregations for 'VIDA' product (no claims)
        vida_row = result_df.filter(result_df.producto == 'VIDA').collect()[0]
        self.assertEqual(vida_row['numero_polizas'], 1)
        self.assertEqual(vida_row['numero_siniestros'], 0)
        self.assertEqual(vida_row['total_monto_reclamado'], None) # Sum of nulls is null

if __name__ == '__main__':
    unittest.main()

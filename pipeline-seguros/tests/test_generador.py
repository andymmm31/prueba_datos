
import unittest
import os
import pandas as pd
from datetime import datetime

# Add the scripts directory to the Python path to import the script
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from generar_datos import generar_polizas, generar_siniestros, NUM_POLIZAS, NUM_SINIESTROS

class TestGeneradorDatos(unittest.TestCase):

    def setUp(self):
        """Set up for the tests."""
        self.polizas_df = generar_polizas(NUM_POLIZAS)
        self.siniestros_df = generar_siniestros(NUM_SINIESTROS, self.polizas_df)

    def test_generar_polizas_shape_and_columns(self):
        """Test the shape and columns of the generated policies DataFrame."""
        self.assertEqual(self.polizas_df.shape[0], NUM_POLIZAS)
        expected_cols = ['poliza_id', 'cliente_id', 'producto', 'suma_asegurada',
                           'prima_mensual', 'fecha_inicio', 'estado', 'region']
        self.assertListEqual(list(self.polizas_df.columns), expected_cols)

    def test_generar_siniestros_shape_and_columns(self):
        """Test the shape and columns of the generated claims DataFrame."""
        self.assertEqual(self.siniestros_df.shape[0], NUM_SINIESTROS)
        expected_cols = ['siniestro_id', 'poliza_id', 'fecha_siniestro',
                           'tipo_siniestro', 'monto_reclamado', 'estado']
        self.assertListEqual(list(self.siniestros_df.columns), expected_cols)

    def test_error_introduction(self):
        """Test that errors (NaNs or negative values) are introduced."""
        # Check for NaNs
        polizas_has_nans = self.polizas_df.isnull().sum().sum() > 0
        siniestros_has_nans = self.siniestros_df.isnull().sum().sum() > 0

        # Check for specific invalid values
        polizas_has_negatives = (self.polizas_df['suma_asegurada'] < 0).any() or \
                                (self.polizas_df['prima_mensual'] < 0).any()

        siniestros_has_zeros = (self.siniestros_df['monto_reclamado'] == 0).any()

        # At least one of these error conditions should be true for a large enough sample
        self.assertTrue(polizas_has_nans or polizas_has_negatives,
                        "No errors were introduced in the policies data.")
        self.assertTrue(siniestros_has_nans or siniestros_has_zeros,
                        "No errors were introduced in the claims data.")

if __name__ == '__main__':
    unittest.main()

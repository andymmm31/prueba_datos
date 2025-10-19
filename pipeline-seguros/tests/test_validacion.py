
import unittest
import pandas as pd
import os

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from validacion import validate_schema, validate_data_quality, POLIZAS_SCHEMA, SINIESTROS_SCHEMA

class TestValidacion(unittest.TestCase):

    def test_validate_schema_success(self):
        """Test schema validation succeeds with correct columns."""
        correct_polizas = pd.DataFrame(columns=POLIZAS_SCHEMA.keys())
        self.assertTrue(validate_schema(correct_polizas, POLIZAS_SCHEMA))

    def test_validate_schema_failure(self):
        """Test schema validation fails with a missing column."""
        incorrect_polizas = pd.DataFrame(columns=['poliza_id', 'cliente_id'])
        self.assertFalse(validate_schema(incorrect_polizas, POLIZAS_SCHEMA))

    def test_validate_data_quality_no_errors(self):
        """Test data quality validation with clean data."""
        polizas_df = pd.DataFrame({
            'poliza_id': ['P01'], 'cliente_id': ['C01'], 'producto': ['AUTO'],
            'suma_asegurada': [1000], 'prima_mensual': [100],
            'fecha_inicio': ['2023-01-01'], 'estado': ['ACTIVA'], 'region': ['CDMX']
        })
        siniestros_df = pd.DataFrame({
            'siniestro_id': ['S01'], 'poliza_id': ['P01'], 'fecha_siniestro': ['2023-02-01'],
            'tipo_siniestro': ['CHOQUE'], 'monto_reclamado': [500], 'estado': ['APROBADO']
        })

        error_rate, _ = validate_data_quality(polizas_df, siniestros_df)
        self.assertEqual(error_rate, 0)

    def test_validate_data_quality_with_errors(self):
        """Test data quality validation with erroneous data."""
        polizas_df = pd.DataFrame({
            'poliza_id': ['P01', 'P02'], 'cliente_id': ['C01', 'C02'],
            'suma_asegurada': [1000, -50],  # Error: negative value
            'prima_mensual': [100, None]    # Error: null value
        })
        siniestros_df = pd.DataFrame({
            'siniestro_id': ['S01'], 'poliza_id': ['P03'], # Error: poliza_id not in polizas_df
            'monto_reclamado': [0] # Error: zero value
        })

        # Manually add missing columns to pass schema validation for this specific test
        for col in POLIZAS_SCHEMA:
            if col not in polizas_df: polizas_df[col] = 'dummy'
        for col in SINIESTROS_SCHEMA:
            if col not in siniestros_df: siniestros_df[col] = 'dummy'

        error_rate, error_count = validate_data_quality(polizas_df, siniestros_df)

        self.assertGreater(error_rate, 0)
        # Expected errors: 1 negative, 1 null, 1 orphan poliza_id, 1 zero amount
        self.assertEqual(error_count, 4)

if __name__ == '__main__':
    unittest.main()

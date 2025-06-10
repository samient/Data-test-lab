import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_line(line):
    """Parse each line of the CSV file."""
    # Skip header and empty lines
    if line.startswith('order_id') or not line.strip():
        return None
    
    # Simple CSV parsing (for more complex cases, use csv module)
    values = line.split(',')
    
    # Basic data validation
    if len(values) != 6:
        logger.warning(f"Skipping malformed line: {line}")
        return None
    
    try:
        return {
            'order_id': values[0],
            'customer_id': values[1],
            'product_id': values[2],
            'quantity': int(values[3]),
            'unit_price': float(values[4]),
            'order_date': values[5],
            'total_sale': int(values[3]) * float(values[4])
        }
    except ValueError as e:
        logger.warning(f"Skipping line due to conversion error: {line}, error: {e}")
        return None

def run_pipeline(input_path, output_path, project, region):
    """Run the Beam pipeline."""
    # Pipeline options
    options = PipelineOptions(
        flags=[],
        runner='DataflowRunner',
        project=test-data-462501,
        region=us-central1,
        temp_location=output_path.replace('/output/', '/temp/'),
        staging_location=output_path.replace('/output/', '/staging/'),
        service_account_email='dataflow-service-account@test-data-462007.iam.gserviceaccount.com'
    )
    
    # Table schema for BigQuery
    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'STRING'},
            {'name': 'customer_id', 'type': 'STRING'},
            {'name': 'product_id', 'type': 'STRING'},
            {'name': 'quantity', 'type': 'INTEGER'},
            {'name': 'unit_price', 'type': 'FLOAT'},
            {'name': 'order_date', 'type': 'STRING'},
            {'name': 'total_sale', 'type': 'FLOAT'}
        ]
    }
    
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'ReadFromGCS' >> beam.io.ReadFromText(input_path)
            | 'ParseLines' >> beam.Map(parse_line)
            | 'FilterValidRecords' >> beam.Filter(lambda x: x is not None)
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table='sales_data.transactions',
                dataset='sales_data',
                project=test-data-462501,
                schema=table_schema,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_APPEND'
            )
            # Additional output to GCS for backup
            | 'FormatOutput' >> beam.Map(lambda x: f"{x['order_id']},{x['total_sale']}")
            | 'WriteToGCS' >> beam.io.WriteToText(
                file_path_prefix=output_path,
                file_name_suffix='.csv',
                num_shards=1
            )
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Input file path in GCS')
    parser.add_argument('--output', required=True, help='Output file path in GCS')
    parser.add_argument('--project', required=True, help='Google Cloud project ID')
    parser.add_argument('--region', required=True, help='Region for Dataflow job')
    
    args = parser.parse_args()
    
    logger.info(f"Starting pipeline with args: {args}")
    run_pipeline(args.input, args.output, args.project, args.region)
    logger.info("Pipeline completed successfully")

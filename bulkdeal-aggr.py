import argparse                             # To handle command-line arguments
import logging                              # For logging error/info messages
import apache_beam as beam                  # Apache Beam SDK for building pipelines
import re                                   # Regex module for parsing text
from apache_beam.io import ReadFromText     # Read input from text files
from apache_beam.io import WriteToText      # Write output to text files
from apache_beam.options.pipeline_options import PipelineOptions    # Beam pipeline options
# Pardo class for parallal processinf by applying user based transformation
class script_val(beam.DoFn):        # Custom DoFn to apply transformation to each element
    def process(self, element):     # This method is called for each element in the PCollection
        try:
            line = element.split('"')   # Split the string by double quotes
            if line[9] == 'BUY':        # Check if the transaction type is BUY
                tp=line[3]+','+line[11].replace(',','')     # Extract fields and remove commas
            else:
                tp=line[3]+',-'+line[11].replace(',','')    # Negate value if not BUY (assumed SELL)
            tp=tp.split()                                   # Split into list
            return tp
        except:
            logging.info('some error occured')              # Log issues (not ideal: should log the actual exception)

# Entry run methoed for triggring pipeline
def run():
    # Command-line argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
    parser.add_argument('--output', dest='output', required=True, help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args()  # Splits known args and Beam-specific args

    # Function to SUM grouped elements
    def sum_groups(word_ones):
        (word, ones) = word_ones                # Unpack grouped data
        return word + ',' + str(sum(ones))      # Sum all values for each key
    ...
    def format_result(bulk_deal):               
        (bulk,deal) = bulk_deal
        return '%s: %d' % (bulk, deal)          # Format for printing/logging

# Function to parse and format given input to BigQuery to readable JSON format.
    def parse_methoed(string_input):            # Typo: should be `parse_method`
        
        value= re.split(",",re.sub('\n\r', '',re.sub(u'"', '',string_input)))   # Clean up and split line
        row = dict(zip(('SYMBOL', 'BUY_SELL_QTY'), values))     # Create dictionary for BigQuery
        return row
    # main pipeline
    with beam.pipeline(options=PipelineOptions(pipeline_args)) as p:
        lines = p | 'Read' >> ReadFromText(known_args.input,skip_header_lines=1)
        counts = (
            lines
            | 'Get required tuple' >> beam.ParDo(script_val())      # Apply custom transformation
            | 'pairWithValue' >> beam.Map(lambda x:(x.split(',')[0],int(x.split(',')[1])))  # Key-value pairs
            | 'Group By Key'  >> beam.GroupByKey()          # Group all values by key
            | 'Sum Groups'  >> beam.map(sum_groups)         # Sum grouped values
            | 'To String' >> beam.map(lamba s: str(s))      # Convert to string
            | 'String to BigQuery Row' >> beam.Map(lambda s: parse_methoed(s))      # Parse to dict for BigQuery
            #| 'format' >> beam.map(format_result)
            #| 'print' >> beam.map(print)
            #| 'Write' >> WriteToText(known_args.output)
        )
        # write to BigQuery Sink
        # Final output: write to BigQuery
        counts | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table='batch_data',
            dataset='dataflow-test',
            project='your-gcp-project-id',  # ← Replace with actual GCP project ID
            schema='SYMBOL:STRING, BUY_SELL_QTY:INTEGER',  # Define schema
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE  # Overwrite table each run
        )
#Trigger enty function here
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)      # Set logging level
    run()       # Trigger the pipeline
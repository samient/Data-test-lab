import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import csv
import logging

class ParseCSV(beam.DoFn):
    def process(self, line):
        fields = list(csv.reader([line]))[0]
        try:
            return [{
                'product': fields[0],
                'region': fields[1],
                'sales': int(fields[2])
            }]
        except Exception as e:
            logging.error(f"Error parsing line: {line} - {e}")
            return []

class SumSales(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "MapToKey" >> beam.Map(lambda x: ((x['region'], x['product']), x['sales']))
            | "SumSales" >> beam.CombinePerKey(sum)
            | "FormatForBigQuery" >> beam.Map(lambda x: {
                'region': x[0][0],
                'product': x[0][1],
                'total_sales': x[1]
            })
        )

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=False)

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "ReadFromGCS" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
        parsed = lines | "ParseCSV" >> beam.ParDo(ParseCSV())
        summarized = parsed | "SumSalesByRegionAndProduct" >> SumSales()

        summarized | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table='test-data-462007:dataset.sales_summary',
            schema='region:STRING, product:STRING, total_sales:INTEGER',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

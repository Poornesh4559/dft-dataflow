import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
import logging

class CombineNames(beam.DoFn):
    def process(self, element):
        name = element['firstname'] +" "+element['lastname']
        age = element['age']
        gender = element['gender']
        
        return [{
            'name': name,
            'age': int(age),
            'gender': gender
        }]

def run_pipeline(argv=None):
    # Create an argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input_table', required=True, help='BigQuery table to read from in the format dataset.table')
    parser.add_argument('--output', dest='output_table', required=True, help='BigQuery table to write to in the format dataset.table')
    
    # Parse the command-line arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Create PipelineOptions using the parsed arguments
    pipeline_options = PipelineOptions(pipeline_args)
    
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read data from the BigQuery table
        rows = p | 'ReadFromBigQuery' >> ReadFromBigQuery(table=known_args.input_table)

        # Transform the data by combining firstname and lastname into name
        transformed_data = rows | 'CombineNames' >> beam.ParDo(CombineNames())
        
        # Write the transformed data to another BigQuery table
        transformed_data | 'WriteToBigQuery' >> WriteToBigQuery(
            table=known_args.output_table,
            schema='name:STRING, age:INTEGER, gender:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()

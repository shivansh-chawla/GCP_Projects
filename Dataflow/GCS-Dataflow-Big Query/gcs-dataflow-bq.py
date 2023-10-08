import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'gcp-practice-456123' ,
    'runner': 'DataflowRunner',
    'region': 'asia-south1',
    'staging_location': 'gs://gcp-practice-456123/temp',
    'temp_location': 'gs://gcp-practice-456123/temp',
    'template_location': 'gs://gcp-practice-456123/df_template/batch_gcs_df_bq'
    }

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = r"C:\Users\chawl\Projects\GCP_Projects\Apache Beam\secret.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount

table_schema = 'airport:STRING, Total_Delayed_time:INTEGER'
table = 'gcp-practice-456123:flights_delay.flights_delay_sum'

Delayed_time = (
  p1
  | "Import Data time" >> beam.io.ReadFromText(r"gs://dataflow-course/input/flights_sample.csv", skip_header_lines = 1)
  | "Split by comma time" >> beam.Map(lambda record: record.split(','))
  | "Filter Delays time" >> beam.Filter(lambda record: int(record[8])>0)
  | "Create a key-value time" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Sum by key time" >> beam.CombinePerKey(sum) 
  | "Write to BQ" >> beam.io.WriteToBigQuery(
                            table,
                            schema=table_schema,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                            custom_gcs_temp_location = 'gs://gcp-practice-456123/temp')
)

p1.run()
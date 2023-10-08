import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'gcp-practice-456123' ,
    'runner': 'DataflowRunner',
    'region': 'asia-south1',
    'staging_location': 'gs://gcp-practice-456123/temp',
    'temp_location': 'gs://gcp-practice-456123/temp',
    'template_location': 'gs://gcp-practice-456123/df_template/batch_gcs_df_gcs' 
    }
    
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = r"C:\Users\chawl\Projects\GCP_Projects\Apache Beam\secret.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount

class Filter(beam.DoFn):
  def process(self,record):
    if int(record[8]) > 0:
      return [record]

Delayed_time = (
  p1
  | "Import Data time" >> beam.io.ReadFromText(r"gs://gcp-practice-456123/input/flights_sample.csv")
  | "Split by comma time" >> beam.Map(lambda record: record.split(','))
  | "Filter Delays time" >> beam.ParDo(Filter())
  | "Create a key-value time" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Sum by key time" >> beam.CombinePerKey(sum)
)

Delayed_num = (
  p1
  |"Import Data" >> beam.io.ReadFromText(r"gs://gcp-practice-456123/input/flights_sample.csv")
  | "Split by comma" >>  beam.Map(lambda record: record.split(','))
  | "Filter Delays" >> beam.ParDo(Filter())
  | "Create a key-value" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Count by key" >> beam.combiners.Count.PerKey()
)

Delay_table = (
    {'Delayed_num':Delayed_num,'Delayed_time':Delayed_time} 
    | "Group By" >> beam.CoGroupByKey()
    | "Save to GCS" >> beam.io.WriteToText(r"gs://gcp-practice-456123/output/flights_output.csv")
)

p1.run()
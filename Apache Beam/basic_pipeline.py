#Basic Pipeline in Apache Beam

import apache_beam as beam

p1 = beam.Pipeline()

data = {
p1
    | beam.io.ReadFromText(r"C:\Users\chawl\Projects\GCP_Projects\Apache Beam\records.txt", skip_header_lines=1)
    | beam.Map(lambda record: record.split(','))
    | beam.Filter(lambda record: int(record[0])>4)
    | beam.Filter(lambda record: str(record[1]=='Ended'))
    | beam.io.WriteToText(r"C:\Users\chawl\Projects\GCP_Projects\Apache Beam\output.txt")
}

p1.run()
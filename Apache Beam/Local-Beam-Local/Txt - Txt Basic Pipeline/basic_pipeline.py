#Basic Pipeline in Apache Beam

import apache_beam as beam

p1 = beam.Pipeline()

data = {
p1
    | "importing data" >> beam.io.ReadFromText(r"C:\Users\chawl\Projects\GCP_Projects\Apache Beam\Local-Beam-Local\Txt - Txt Basic Pipeline\records.txt", skip_header_lines=1)
    | "splitting data" >> beam.Map(lambda record: record.split(','))
    | "duration filter" >> beam.Filter(lambda record: int(record[0])>3)
    | "status filter" >> beam.Filter(lambda record: str(record[1])==" Ended")
    | "writing the data" >> beam.io.WriteToText(r"C:\Users\chawl\Projects\GCP_Projects\Apache Beam\output.txt")
}

p1.run()
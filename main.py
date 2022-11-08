import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from my_options import MyOptions
from pipeline import ComputeWordLength

options = MyOptions()
# ローカルで実行
options.view_as(StandardOptions).runner = "DirectRunner"
p = beam.Pipeline(options=options)

(
    p | "ReadFromText" >> beam.io.ReadFromText(options.input)
    | "Count" >> beam.ParDo(ComputeWordLength())
    | "WriteCountToTxt" >> beam.io.WriteToText(options.output, shard_name_template="")
)

p.run()
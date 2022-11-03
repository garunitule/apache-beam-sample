import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from my_options import MyOptions

options = PipelineOptions()
# ローカルで実行
options.view_as(StandardOptions).runner = "DirectRunner"
p = beam.Pipeline(options=MyOptions())

lines = (
    p | "ReadFromInMemory" >> beam.Create([
    'To be, or not to be: that is the question: ',
    'Whether \'tis nobler in the mind to suffer ',
    'The slings and arrows of outrageous fortune, ', 'Or to take arms against a sea of troubles, '
    ])
)

lines | "WriteToText" >> beam.io.WriteToText("./output", file_name_suffix=".txt")

p.run()
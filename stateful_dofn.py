import apache_beam as beam
from apache_beam.coders.coders import VarIntCoder
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.pipeline import PipelineOptions
import logging

class StatefulDoFn(beam.DoFn):
    MY_STATE = ReadModifyWriteStateSpec('my_state', VarIntCoder())

    def process(self, kv, my_state=beam.DoFn.StateParam(MY_STATE)):
        key, _ = kv
        current = my_state.read() or 0
        my_state.write(current + 1)
        return [(key, current + 1)]

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    with beam.Pipeline(options=PipelineOptions()) as p:
        (p | beam.Create([(x, x) for x in [1, 2, 1, 3, 4, 4]])
        | beam.ParDo(StatefulDoFn())
        | beam.io.WriteToText('output_stateful_dofn.txt')) 
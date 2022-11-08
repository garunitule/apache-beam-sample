import apache_beam as beam

class ComputeWordLength(beam.DoFn):
    def __init__(self):
        pass

    def process(self, element):
        yield len(element)

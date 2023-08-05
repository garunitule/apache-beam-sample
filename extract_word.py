import apache_beam as beam
from apache_beam import pvalue

class ExtractWord(beam.DoFn):
    def process(element):
        if element.startswith("A"):
            yield pvalue.TaggedOutput("a", element)
        elif element.startsWith("B"):
            yield pvalue.TaggedOutput("b", element)

with beam.Pipeline() as p:
    # create a pcollection from a list of words
    plants = (
        p | 'Gardening plants' >> beam.Create([
            {
                'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
            },
            {
                "icon": "🥕", "name": "Carrot", "duration": "biennial"
            },
            {
                "icon": "🍆", "name": "Eggplant", "duration": "perennial"
            },
        ])
    )
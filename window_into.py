import apache_beam as beam

def window(test=None):
  # ここを変えると結果が変わる
  WINDOW_DURATION = 3 * 30 * 24 * 60 * 60
  WINDOW_OFFSET = 1583020800 # March, 2020

  with beam.Pipeline() as pipeline:
    produce = (pipeline
                 | 'Garden plants' >> beam.Create([
                    {'name': 'Strawberry', 'season': 1585699200},  # April, 2020
                    {'name': 'Strawberry', 'season': 1588291200},  # May, 2020
                    {'name': 'Carrot', 'season': 1590969600},  # June, 2020
                    {'name': 'Artichoke', 'season': 1583020800},  # March, 2020
                    {'name': 'Artichoke', 'season': 1585699200},  # April, 2020
                    {'name': 'Tomato', 'season': 1588291200},  # May, 2020
                    {'name': 'Potato', 'season': 1598918400},  # September, 2020
                  ])
                 | 'With timestamps' >> beam.Map(lambda plant: beam.window.TimestampedValue(plant['name'], plant['season']))
                 | 'Window into fixed 2-month windows' >> beam.WindowInto(
                              beam.transforms.window.FixedWindows(WINDOW_DURATION, WINDOW_OFFSET))
                 | 'Count per window' >> beam.combiners.Count.PerElement()
                 | 'Print results' >> beam.Map(print)
                 )

if __name__ == "__main__":
    window()
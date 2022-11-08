from unittest import TestCase

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline import ComputeWordLength

class PipelineTest(TestCase):
    def test_pipeline(self):
        """失敗した場合のみエラーが表示される
        """
        expected = [
            43,
            42,
            45,
            43
        ]

        inputs = [
            'To be, or not to be: that is the question: ',
            'Whether \'tis nobler in the mind to suffer ',
            'The slings and arrows of outrageous fortune, ', 
            'Or to take arms against a sea of troubles, '
        ]

        with TestPipeline() as p:
            actual = (
                p
                | beam.Create(inputs)
                | beam.ParDo(ComputeWordLength())
            )

            assert_that(actual, equal_to(expected))

if __name__ == '__main__':
    tester = PipelineTest()
    tester.test_pipeline()
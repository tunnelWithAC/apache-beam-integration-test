import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from transforms.parse import Parse


def test_parse_appends_element_timestamp():
    with TestPipeline() as p:
        output = (
            p
            | beam.Create([b'conall_0'])
            | beam.Map(lambda x: TimestampedValue(x, 1608051184))
            | Parse())

        assert_that(output, equal_to(['conall_0 - 1608051184']))

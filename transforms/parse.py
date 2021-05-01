import apache_beam as beam
from apache_beam import DoFn, PTransform


class Parse(PTransform):

    def expand(self, pcoll):
        import logging
        return pcoll | "Parse" >> beam.Map(self.parse_fields)

    def parse_fields(self, element, created_timestamp=DoFn.TimestampParam):
        from datetime import datetime
        import logging
        # todo hash byte string to create message_id
        parsed_element = element.decode('utf-8')
        logging.info(f'timestamp: {int(created_timestamp)} \nelement: {parsed_element}')
        return f'{parsed_element} - {int(created_timestamp)}'

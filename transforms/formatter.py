import apache_beam as beam
from apache_beam import DoFn, PTransform

from apache_beam.io.gcp.pubsub import PubsubMessage

class FormatPubsubRead(PTransform):

    def expand(self, pcoll):
        import logging
        return pcoll | "Parse" >> beam.Map(self.parse_fields)

    def parse_fields(self, element, created_timestamp=DoFn.TimestampParam):
        from datetime import datetime
        import json
        import logging
        # todo hash byte string to create message_id
        message = {
            'data': element.decode('utf-8'),
            'created_timestamp': int(created_timestamp),
            'message_id': '230115008107576214'
            #'message_id': hash(element)
        }
        message_str = json.dumps(message)
        # logging.info(f'timestamp: {int(created_timestamp)} \nelement: {parsed_element}')
       
        # return PubsubMessage(
        #     message_str.encode('utf-8'),
        #     { 'timestamp': '1608051184000' }
        # )

        return message_str.encode('utf-8')

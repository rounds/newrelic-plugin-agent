"""
couchbase

"""

from newrelic_plugin_agent.plugins import base


class Couchbase(base.JSONStatsPlugin):

    GUID = 'com.meetme.couchbase'

    def add_datapoints(self, stats):
        for bucket_data in stats:
            bucket_name = bucket_data['name']
            bucket_stats = bucket_data['basicStats']
            if bucket_stats:
                """ Example JSON
                "basicStats":{
                    "quotaPercentUsed":7.548735046386719,
                    "opsPerSec":0,
                    "diskFetches":0,
                    "itemCount":2222,
                    "diskUsed":229366539,
                    "dataUsed":203511808,
                    "memUsed":79154224
                }
                """
                self.add_gauge_bucket_metric(
                    bucket_name, 'quotaPercentUsed', 'percent', bucket_stats['quotaPercentUsed']
                )
                self.add_gauge_bucket_metric(
                    bucket_name, 'opsPerSec', 'ops', bucket_stats['opsPerSec']
                )
                self.add_gauge_bucket_metric(
                    bucket_name, 'diskFetches', 'count', bucket_stats['diskFetches']
                )
                self.add_gauge_bucket_metric(
                    bucket_name, 'itemCount', 'count', bucket_stats['itemCount']
                )
                self.add_gauge_bucket_metric(
                    bucket_name, 'diskUsed', 'Byte', bucket_stats['diskUsed']
                )
                self.add_gauge_bucket_metric(
                    bucket_name, 'dataUsed', 'Byte', bucket_stats['dataUsed']
                )
                self.add_gauge_bucket_metric(
                    bucket_name, 'memUsed', 'Byte', bucket_stats['memUsed']
                )

                # Summary metrics
                self.add_gauge_value('Summary/%s/quotaPercentUsed' % bucket_name, 'percent',
                                     bucket_stats['quotaPercentUsed'],
                                     min_val=0, max_val=0)
                self.add_gauge_value('Summary/%s/diskUsed' % bucket_name, 'byte',
                                     bucket_stats['diskUsed'])

    def add_gauge_bucket_metric(self, bucket_name, metric_name, units, metric_value):
        if metric_value:
            self.add_gauge_value('%s/%s' % (bucket_name, metric_name), units, metric_value)

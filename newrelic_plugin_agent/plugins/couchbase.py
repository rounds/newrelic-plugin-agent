"""
couchbase

"""
from newrelic_plugin_agent.plugins import base


class Couchbase(base.JSONStatsPlugin):

    GUID = 'com.meetme.newrelic_couchbase_agent'

    # metrics are according to api reference v3.0 or v3.1
    METRICS = [
        {'type': 'cluster', 'label': 'storageTotals.ram.total', 'suffix': 'bytes'},
        {'type': 'cluster', 'label': 'storageTotals.ram.used', 'suffix': 'bytes'},
        {'type': 'cluster', 'label': 'storageTotals.ram.usedByData', 'suffix': 'bytes'},
        {'type': 'cluster', 'label': 'storageTotals.ram.quotaTotal', 'suffix': 'bytes'},
        {'type': 'cluster', 'label': 'storageTotals.ram.quotaUsed', 'suffix': 'bytes'},

        {'type': 'cluster', 'label': 'storageTotals.hdd.total', 'suffix': 'bytes'},
        {'type': 'cluster', 'label': 'storageTotals.hdd.used', 'suffix': 'bytes'},
        {'type': 'cluster', 'label': 'storageTotals.hdd.usedByData', 'suffix': 'bytes'},
        {'type': 'cluster', 'label': 'storageTotals.hdd.quotaTotal', 'suffix': 'bytes'},
        {'type': 'cluster', 'label': 'storageTotals.hdd.free', 'suffix': 'bytes'},

        {'type': 'cluster', 'label': 'counters.rebalance_success', 'suffix': 'count'},
        {'type': 'cluster', 'label': 'counters.rebalance_start', 'suffix': 'count'},
        {'type': 'cluster', 'label': 'counters.rebalance_fail', 'suffix': 'count'},
        {'type': 'cluster', 'label': 'counters.rebalance_node', 'suffix': 'count'},

        {'type': 'nodes', 'label': 'systemStats.cpu_utilization_rate', 'suffix': 'count'},
        {'type': 'nodes', 'label': 'systemStats.swap_total', 'suffix': 'byte'},
        {'type': 'nodes', 'label': 'systemStats.swap_used', 'suffix': 'byte'},
        {'type': 'nodes', 'label': 'systemStats.mem_total', 'suffix': 'byte'},
        {'type': 'nodes', 'label': 'systemStats.mem_free', 'suffix': 'byte'},

        {'type': 'nodes', 'label': 'interestingStats.couch_docs_actual_disk_size', 'suffix': 'byte'},
        {'type': 'nodes', 'label': 'interestingStats.couch_docs_data_size', 'suffix': 'byte'},
        {'type': 'nodes', 'label': 'interestingStats.couch_views_actual_disk_size', 'suffix': 'byte'},
        {'type': 'nodes', 'label': 'interestingStats.couch_views_data_size', 'suffix': 'byte'},
        {'type': 'nodes', 'label': 'interestingStats.mem_used', 'suffix': 'byte'},
        {'type': 'nodes', 'label': 'interestingStats.ops', 'suffix': 'count'},
        {'type': 'nodes', 'label': 'interestingStats.curr_items', 'suffix': 'count'},
        {'type': 'nodes', 'label': 'interestingStats.curr_items_tot', 'suffix': 'count'},
        {'type': 'nodes', 'label': 'interestingStats.vb_replica_curr_items', 'suffix': 'count'},

        {'type': 'nodes', 'scoreboard': True, 'score_value': 'active', 'label': 'clusterMembership', 'suffix': 'count'},
        {'type': 'nodes', 'scoreboard': True, 'score_value': 'healthy', 'label': 'status', 'suffix': 'count'},

        {'type': 'buckets', 'label': 'basicStats.quotaPercentUsed', 'suffix': 'percent'},
        {'type': 'buckets', 'label': 'basicStats.opsPerSec', 'suffix': 'ops'},
        {'type': 'buckets', 'label': 'basicStats.diskFetches', 'suffix': 'percent'},
        {'type': 'buckets', 'label': 'basicStats.itemCount', 'suffix': 'percent'},
        {'type': 'buckets', 'label': 'basicStats.diskUsed', 'suffix': 'byte'},
        {'type': 'buckets', 'label': 'basicStats.dataUsed', 'suffix': 'byte'},
        {'type': 'buckets', 'label': 'basicStats.memUsed', 'suffix': 'byte'},
    ]

    def add_datapoints(self, data):
        """Add data points for all couchbase nodes.

        :param dict stats: stats for all nodes

        """
        # fetch metrics for each metric type (cluster, nodes, buckets)
        for typ, stats in data.iteritems():
            # set items to fetch stats from,
            # and set item key name, where the item's name will be fetched from
            if typ == 'cluster':
                items = stats
                name_key = 'name'
            elif typ == 'nodes':
                items = stats['nodes']
                name_key = 'hostname'
            elif typ == 'buckets':
                items = stats
                name_key = 'name'

            for metric in [m for m in self.METRICS if m['type'] == typ]:
                # count score for scoreboard metrics,
                # otherwise just add the gauge value from the API repsonse
                #
                # A "scoreboard" metric is a counter of a specific value for
                # a field. E.g How many times the pair {"status": "healthy"}
                # is found in a list of objects.
                #
                # NOTE that scoreboard metrics are meant to be used on lists
                # of objects e.g. a list of nodes.
                if metric.get('scoreboard', False):
                    self.add_gauge_value(
                        '%s/_scoreboard/%s' % (typ, metric['label']),
                        metric['suffix'],
                        sum(d[metric['label']] == metric['score_value']
                            for d in items))
                else:
                    # add gauge value for current metric.
                    # NOTE cluster metrics are not repeated,
                    # as there is a single cluster.
                    # nodes and bucket metrics are repeated,
                    # as there are (usually) several of them
                    if typ == 'cluster':
                        self._add_gauge_value(
                            metric, typ, items[name_key], items)
                    else:
                        for item in items:
                            self._add_gauge_value(
                                metric, typ, item[name_key], item)

    def _add_gauge_value(self, metric, typ, name, items):
        """Adds a gauge value for a nested metric.

        Some stats are missing from memcached bucket types,
        thus we use dict.get()

        :param dict m: metric as defined at the top of this class.
        :param str typ: metric type: cluster, nodes, buckets.
        :param str name: cluster, node, or bucket name.
        :param dict items: stats to lookup the metric in.

        """
        label = metric['label']
        value = items
        for key in label.split('.'):
            value = value.get(key, 0)
        self.add_gauge_value('%s/%s/%s' % (typ, name, label),
                             metric['suffix'], value)

    def fetch_data(self):
        """Fetch data from multiple couchbase stats URLs.

        Returns a dictionary with three keys: cluster, nodes, buckets.
        Each key holds the JSON response from the API request.

        :rtype: dict

        """
        data = {}
        for path, typ in [('pools/default', 'cluster'),
                          ('pools/nodes', 'nodes'),
                          ('pools/default/buckets', 'buckets')]:
            res = self.http_get(self.stats_url + path)
            data[typ] = res and res.json() or {}

        return data

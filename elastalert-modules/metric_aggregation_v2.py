
'''
Created on 2019. 10. 7.

@author: user
'''
from elastalert.util import EAException
from elastalert.ruletypes import BaseAggregationRule
from elastalert.util import elastalert_logger

class MetricAggregationRule(BaseAggregationRule):
    """ A rule that matches when there is a low number of events given a timeframe. """
    required_options = frozenset(['metric_agg_key', 'metric_agg_type'])
    allowed_aggregations = frozenset(['min', 'max', 'avg', 'sum', 'cardinality', 'value_count'])

    def __init__(self, *args):
        super(MetricAggregationRule, self).__init__(*args)
        self.ts_field = self.rules.get('timestamp_field', '@timestamp')
        if 'max_threshold' not in self.rules and 'min_threshold' not in self.rules:
            raise EAException("MetricAggregationRule must have at least one of either max_threshold or min_threshold")

        self.metric_key = 'metric_' + self.rules['metric_agg_key'] + '_' + self.rules['metric_agg_type']
        
        if self.rules['use_derivative_aggregation'] == True:
            self.metric_derivative = self.metric_key + '_deriv'
            
        if not self.rules['metric_agg_type'] in self.allowed_aggregations:
            raise EAException("metric_agg_type must be one of %s" % (str(self.allowed_aggregations)))

        self.rules['aggregation_query_element'] = self.generate_aggregation_query()

    def get_match_str(self, match):
        message = 'Threshold violation, %s:%s %s (min: %s max : %s) \n\n' % (
            self.rules['metric_agg_type'],
            self.rules['metric_agg_key'],
            match[self.metric_key],
            self.rules.get('min_threshold'),
            self.rules.get('max_threshold')
        )
        return message

    def generate_aggregation_query(self):
        aggs_query = {};
        aggs_query['test'] = {self.rules['metric_agg_type']: {'field': self.rules['metric_agg_key']}}
        
        if self.rules['test_aggregation'] == True:
            aggs_query[ self.metric_derivative] = {'derivative':{'buckets_path': 'test'}}
        
        elastalert_logger.info('------------- [generate_aggregation_query] aggregation_query:{}'.format(aggs_query))

        return aggs_query;
        #return {self.metric_key: {self.rules['metric_agg_type']: {'field': self.rules['metric_agg_key']}}}

    def check_matches(self, timestamp, query_key, aggregation_data):
        elastalert_logger.info('------------- [check_matches] timestamp:{}'.format(timestamp))
        elastalert_logger.info('------------- [check_matches] query_key:{}'.format(query_key))
        elastalert_logger.info('------------- [check_matches] aggregation_data:{}'.format(aggregation_data))
        
        if "compound_query_key" in self.rules:
            self.check_matches_recursive(timestamp, query_key, aggregation_data, self.rules['compound_query_key'], dict())

        else:
            if self.rules['use_derivative_aggregation'] == True:
                if self.metric_derivative in aggregation_data.keys():
                    metric_val = aggregation_data[self.metric_derivative]['value']
            else:   
                metric_val = aggregation_data[self.metric_key]['value']

            if self.crossed_thresholds(metric_val):
                if self.rules['use_derivative_aggregation'] == True:
                    match = {self.rules['timestamp_field']: timestamp,
                             self.metric_derivative: metric_val}
                else:
                    match = {self.rules['timestamp_field']: timestamp,
                             self.metric_key: metric_val}
                    
                    
                if query_key is not None:
                    match[self.rules['query_key']] = query_key
                self.add_match(match)

    def check_matches_recursive(self, timestamp, query_key, aggregation_data, compound_keys, match_data):
        if len(compound_keys) < 1:
            # shouldn't get to this point, but checking for safety
            return

        match_data[compound_keys[0]] = aggregation_data['key']
        if 'bucket_aggs' in aggregation_data:
            for result in aggregation_data['bucket_aggs']['buckets']:
                self.check_matches_recursive(timestamp,
                                             query_key,
                                             result,
                                             compound_keys[1:],
                                             match_data)

        else:
         
            if self.rules['use_derivative_aggregation'] == True:
                metric_val = aggregation_data[self.metric_derivative]['value']
            else:   
                metric_val = aggregation_data[self.metric_key]['value']

            if self.crossed_thresholds(metric_val):
               
                if self.rules['use_derivative_aggregation'] == True:
                    match_data[self.rules['timestamp_field']] = timestamp
                    match_data[self.metric_derivative] = metric_val
                else:
                    match_data[self.rules['timestamp_field']] = timestamp
                    match_data[self.metric_key] = metric_val

                # add compound key to payload to allow alerts to trigger for every unique occurence
                compound_value = [match_data[key] for key in self.rules['compound_query_key']]
                match_data[self.rules['query_key']] = ",".join([str(value) for value in compound_value])

                self.add_match(match_data)

    def crossed_thresholds(self, metric_value):
        if metric_value is None:
            return False
        if 'max_threshold' in self.rules and metric_value > self.rules['max_threshold']:
            return True
        if 'min_threshold' in self.rules and metric_value < self.rules['min_threshold']:
            return True
        return False


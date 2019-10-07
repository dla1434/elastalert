'''
reated on 2019. 9. 6.

@author: user
'''
from datetime import datetime
from pytz import timezone
from croniter import croniter
from elastalert.ruletypes import BaseAggregationRule
from elastalert.util import EAException
from elastalert.util import elastalert_logger
# elastalert.util includes useful utility functions
# such as converting from timestamp to datetime obj

class LjhTestRule(BaseAggregationRule):                       
                                            
    # By setting required_options to a set of strings
    # You can ensure that the rule config file specifies all
    # of the options. Otherwise, ElastAlert will throw an exception
    # when trying to load the rule.
    #required_options = set(['time_start', 'time_end', 'usernames'])
                                                            
    # add_data will be called each time Elasticsearch is queried.  
    # data is a list of documents from Elasticsearch, sorted by timestamp,
    # including all the fields that the config specifies with "include"
    
    def __init__(self, *args):
        super(LjhTestRule, self).__init__(*args)
        self.ts_field = self.rules.get('timestamp_field', '@timestamp')
        self.rules['aggregation_query_element'] = self.generate_aggregation_query()

    def generate_aggregation_query(self):
        aq = {}
        aq['ljh_test']={'max':{'field':'jolokia.jmx.heapmemoryusage.used'}}
        aq['ljh_test_deriv']={'derivative':{'buckets_path':'ljh_test'}}
        elastalert_logger.info('------------- [generate_aggregation_query] aggregation_query:{}'.format(aq))
        return aq
        
    
    def check_matches(self, timestamp, query_key, aggregation_data):
        elastalert_logger.info('------------- [check_matches] timestamp:{}'.format(timestamp))
        elastalert_logger.info('------------- [check_matches] query_key:{}'.format(query_key))
        elastalert_logger.info('------------- [check_matches] aggregation_data:{}'.format(aggregation_data))
        if 'ljh_test_deriv' in aggregation_data.keys() and aggregation_data['ljh_test_deriv'] is not None:
            if aggregation_data['ljh_test_deriv']['value'] > 2000000:
                self.add_match(aggregation_data)
        
                             
    
    

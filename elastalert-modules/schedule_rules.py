'''
reated on 2019. 9. 6.

@author: user
'''
from datetime import datetime
from pytz import timezone
from croniter import croniter
from elastalert.ruletypes import RuleType
from elastalert.util import EAException
from elastalert.util import elastalert_logger
# elastalert.util includes useful utility functions
# such as converting from timestamp to datetime obj

class ScheduleRule(RuleType):                       
                                            
    # By setting required_options to a set of strings
    # You can ensure that the rule config file specifies all
    # of the options. Otherwise, ElastAlert will throw an exception
    # when trying to load the rule.
    #required_options = set(['time_start', 'time_end', 'usernames'])
                                                            
    # add_data will be called each time Elasticsearch is queried.  
    # data is a list of documents from Elasticsearch, sorted by timestamp,
    # including all the fields that the config specifies with "include"
    
    def __init__(self, rules, args=None):
        RuleType.__init__(self, rules, args=args)
        
        ## timezone asia/seoul
        self.quartz_schedule = self.rules['quartz_schedule']
        self.timeframe = self.rules['timeframe']
        self.index = self.rules['index']
        self.event = {}
        
    def add_data(self, data):
        pass
    
    def add_count_data(self, data):
        
        if len(data) > 1:
            raise EAException('add_count_data can only accept one count at a time')
        for ts, count in data.items():
            elastalert_logger.info('ts %s, count  %d' %(ts, count))
            self.schedule_check(ts, count)
                             
    # The results of get_match_str will appear in the alert text
    def get_match_str(self, match):          
        message = "%s not working, please check your system" %(match.get('schedule_check_time'))
        return message
                                   
    # garbage_collect is called indicating that ElastAlert has already been run up to timestamp
    # It is useful for knowing that there were no query results from Elasticsearch because
    # add_data will not be called with an empty list                      
    def garbage_collect(self, timestamp):
        pass   
    
    def schedule_check(self, ts, count):
        
        ## timezone asia/seoul
        timezoneKorTs = ts.astimezone(timezone('Asia/Seoul'))
        
        iter = croniter(self.quartz_schedule, timezoneKorTs)
        end_time_kor = iter.get_next(datetime)
        start_time_kor = end_time_kor - self.timeframe
        
        end_time = end_time_kor.astimezone(timezone('UTC'))
        start_time = start_time_kor.astimezone(timezone('UTC'))
        elastalert_logger.debug("schduleTime %s ~ %s" %(start_time_kor, end_time_kor))
        elastalert_logger.debug("schduleTime UTC %s ~ %s" %(start_time, end_time))
        
        if start_time < ts and end_time >= ts :
            
            elastalert_logger.debug('Schedule Time')
            current_event_key = end_time.strftime("%Y-%m-%d %H:%M:%S")
         
            if not (self.event.get(current_event_key) is None):
                self.event[current_event_key] = self.event.get(current_event_key) + count;
            else:
                self.event[current_event_key] = count;
        
        else :
            elastalert_logger.debug('Not Schedule Time')

            prev_event_time_kor = iter.get_prev(datetime)
            prev_event_time = prev_event_time_kor.astimezone(timezone('UTC'))
            prev_event_key = prev_event_time.strftime("%Y-%m-%d %H:%M:%S")
            

            if not (self.event.get(prev_event_key) is None):
                elastalert_logger.debug('eventCount: %d' %self.event.get(prev_event_key))
            
                if (self.event.get(prev_event_key) < 1):
                    
                    elastalert_logger.info('Alert Occur. time: %s', prev_event_time_kor)
                    
                    match = {self.rules['timestamp_field']: prev_event_time,
                             'index': self.index,
                             'count': self.event.get(prev_event_key),
                             'schedule_check_time': prev_event_time_kor}
                    
                    self.add_match(match);
               
                del self.event[prev_event_key]
                
            else:
                elastalert_logger.debug('Not Event')
    
    
    

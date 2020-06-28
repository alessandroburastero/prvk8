'''
Created on Jun 26, 2020

@author: doy
'''
from prvk8.settings import KAFKA_URL, KAFKA_TOPIC, KAFKA_GROUP
from prvk8.kafka import Broker
import threading
from prvk8 import prvk8_logger
from prvk8.models import Messages

class MessageListener(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.broker = Broker(KAFKA_URL, None, None, None)
        
        threading.Thread(target=self.__subscribe).start()
        
    def __subscribe(self):
        prvk8_logger.debug('subscribing')
        self.broker.subscribe(self, KAFKA_TOPIC, KAFKA_GROUP)
        
    def on_message(self, offset:int, obj:object, body:str)->bool:
        prvk8_logger.debug('message received')
        Messages.objects.create(message=body)
        return True
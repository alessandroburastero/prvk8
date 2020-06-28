'''
Created on May 24, 2019
 
@author: doy
'''
from pykafka.connection import SslConfig
from pykafka.client import KafkaClient
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable,\
    KafkaException, ProduceFailureError
import time
import json
import abc
from pykafka.producer import Producer
import traceback
from prvk8 import prvk8_logger


class BrokerSubscriber(metaclass=abc.ABCMeta):
    '''
    interface for all broker subscriber
    '''    
    @abc.abstractmethod
    def on_message(self, offset:int, obj:object, body:str) -> bool:
        '''
        override for message processing
        @return: False if message should be received again in future (do not commit offset) 
        '''
        return False
    
 
class Broker:
    '''
    represents a broker connection manager
    '''
     
    def __init__(self, url:str, ca_file:str, cert_file:str, key_file:str):
        config = SslConfig(ca_file, cert_file, key_file) if ca_file is not None else None
        self.url = url
        self.config = config
        self.client = KafkaClient(hosts=self.url, ssl_config=self.config)
        self.__producers = {}
 
     
    def subscribe(self, listener:BrokerSubscriber, topic_name:str, consumer_group=None):
        
        topic = self.client.topics[topic_name]
        
        while True:
            try:
                prvk8_logger.info(f'Broker.subscribe: subscribing to {topic_name}@{self.url}')
                consumer = topic.get_balanced_consumer(consumer_group=consumer_group, managed=True) 
#                 consumer = topic.get_simple_consumer(consumer_group=consumer_group) 
                for message in consumer:
                    if message is not None:
                        try:                            
                            obj = None
                            body = None
                            try:
                                obj = json.loads(message.value)
                            except ValueError as e:
                                body = message.value.decode()
                                pass
                            

                            #process message
                            if listener.on_message(message.offset, obj, body):
#                                 prvk8_logger.debug(f'Broker.subscribe: dispatching message {body}')
                                #commit the offset only if the processing of the message completes with success
                                if consumer_group is not None: consumer.commit_offsets()
                                
                            
                        except Exception as e:
                            prvk8_logger.critical(f"Broker.subscribe: unexpected error reading message: OFFSET: {message.offset} VALUE: {message.value}")
                            prvk8_logger.exception('', exc_info=e)                            
                            
            except (KafkaException) as e:
                traceback.print_exc()
                prvk8_logger.warning(f'Broker.subscribe: connection to {topic_name}@{self.url} lost... trying to reconnect in 5 seconds')                 
                time.sleep(5)
                
                self.close()
                self.client = KafkaClient(hosts=self.url, ssl_config=self.config)
                topic = self.client.topics[topic_name]
                
 
    def send_xml(self, topic_name:str, xml:str) -> bool: 
        return self.__send(topic_name, xml)
    
    
    def send_obj(self, topic_name:str, obj:object) -> bool: 
        return self.__send(topic_name, json.dumps(obj))

    def __get_producer(self, topic_name) -> Producer:
        if topic_name in self.__producers:
            return self.__producers[topic_name]
        topic = self.client.topics[topic_name]
        producer = topic.get_producer(sync=True)
        self.__producers[topic_name] = producer
        return producer

    def __update_producer(self, topic_name) -> Producer:
        if topic_name in self.__producers:
            self.__producers[topic_name].stop()            
        topic = self.client.topics[topic_name]
        producer = topic.get_producer(sync=True)
        self.__producers[topic_name] = producer
        return producer
    
    def __send(self, topic_name:str, body) -> bool:
        producer = self.__get_producer(topic_name)
        try:
            producer.produce(str.encode(body))
#             producer.stop()
            return True
        except (SocketDisconnectedError, LeaderNotAvailable, ProduceFailureError) as e:   
            
            prvk8_logger.exception('', exc_info=e)
                     
            prvk8_logger.warning(f'__Broker.send: connection to {self.url} lost... try to reopen in 1 seconds')
            time.sleep(1)
            
            producer = self.__update_producer(topic_name)
            try:
                producer.produce(str.encode(body))
                return True                
            except (KafkaException) as e:
                prvk8_logger.critical(f'Broker.send: unable to reopen connection to {self.url}... skip dispatching following message')
                prvk8_logger.critical(body)             
        except Exception as e:
            prvk8_logger.exception("Broker.send: unexpected error dispatching messages:", exc_info=e)
 
        return False
    
    def close(self):
        for topic_name in self.__producers:
            self.__producers[topic_name].stop()
        self.__producers = {}
    
    
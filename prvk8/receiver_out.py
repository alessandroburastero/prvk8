'''
Created on Jun 26, 2020

@author: doy
'''
from prvk8.settings import KAFKA_URL, KAFKA_TOPIC_OUT, KAFKA_TOPIC
from prvk8.kafka import Broker, BrokerSubscriber



class OutConsumer(BrokerSubscriber):

    def on_message(self, offset:int, obj: object, body:str):
        print(f'MESSAGE RECEIVED (offset: {offset})')
        if obj: print(f'\tOBJ: {obj}')
        if body: print(f'\tBODY: {body}')
        
        return True
        

if __name__ == '__main__':
    broker = Broker('172.17.0.1:9092', None, None, None)
    
    broker.send_xml(KAFKA_TOPIC, "CIAO!!!")
    broker.close()
    
#     broker.subscribe(OutConsumer(), KAFKA_TOPIC_OUT, 'receiver_out')
'''
Created on Jun 26, 2020

@author: doy
'''
from prvk8.settings import KAFKA_URL, KAFKA_TOPIC_OUT, KAFKA_TOPIC
from prvk8.kafka import Broker, BrokerSubscriber
import os



class OutConsumer(BrokerSubscriber):

    def on_message(self, offset:int, obj: object, body:str):
        print(f'MESSAGE RECEIVED (offset: {offset})')
        if obj: print(f'\tOBJ: {obj}')
        if body: print(f'\tBODY: {body}')
        
        return True
        

if __name__ == '__main__':
    
    ssl_dir = '/ca'
    ssl_ca_file = os.path.join(ssl_dir, 'ca.cert.crt') if ssl_dir else None
    ssl_cert_file = os.path.join(ssl_dir, 'doy.kafka.crt') if ssl_dir else None
    ssl_key_file = os.path.join(ssl_dir, 'doy_kafka.key') if ssl_dir else None
    url = '52.17.179.228:9093'
    
    broker = Broker(url, ssl_ca_file, ssl_cert_file, ssl_key_file);

    #broker = Broker('172.17.0.1:9092', None, None, None)

    for topic in broker.client.topics:
        print(topic)

    
    #broker.send_xml(KAFKA_TOPIC, "CIAO!!!")
    #broker.close()
    
    #broker.subscribe(OutConsumer(), KAFKA_TOPIC_OUT, 'receiver_out')
'''
Created on Aug 6, 2019

@author: doy
'''
from rest_framework import viewsets, serializers
from prvk8.models import Messages
from prvk8.kafka import Broker
from prvk8.settings import KAFKA_URL, KAFKA_TOPIC_OUT

class MessagesSerializer(serializers.ModelSerializer):
    class Meta:
        model = Messages
        fields = '__all__'
        
    def create(self, validated_data):
        message = super(MessagesSerializer, self).create(validated_data)
        broker = Broker(KAFKA_URL, None, None, None)
        broker.send_xml(KAFKA_TOPIC_OUT, message.message)
        return message

class MessagesViewSet(viewsets.ModelViewSet):
    serializer_class = MessagesSerializer
    queryset = Messages.objects.all() 

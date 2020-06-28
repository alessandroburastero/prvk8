'''
Created on Jun 26, 2020

@author: doy
'''
from django.db import models


class Messages(models.Model):
    message = models.TextField()
    

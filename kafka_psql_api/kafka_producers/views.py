from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from kafka_producers import producer_api #import message_producer
import json
# Create your views here.

def index(request):
    if request.method == "POST":
        raw_body = request.body.decode('utf-8')
        json_body = json.loads(raw_body)
        producer_api.message_producer(json_body)
        return JsonResponse({"response":json_body,"status":200})


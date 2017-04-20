import paho.mqtt.client as mqtt
import json
import thread
import time

user = "calplug"
password = "Calplug2016"
host = "cpmqtt2.calit2.uci.edu" #example "wop.calplug.uci.edu"
port = 1883 #change to your port number
#mac_list = ['600194111f87'] add your mac address(s)  old one :'22cc88d900', '5ccf7f0ac27c', 
mac_list = ['600194111f87']

keepalive = 60
sTopicSub = 'out/devices/'
sTopicPub = 'in/devices/'

PAYLOAD_POST = '{"method": "post","params":{}}'
PAYLOAD_GET = '{"method": "get","params":{}}'


#thread will die after 2 seconds
def disconnect_countdown(client):
    #print("WAITING TO COUNT DOWN")
    time.sleep(2)
    client.disconnect()
    #print("------DISCONNECTING---------")
    time.sleep(5)
    exit()

def on_connect(client, userdata, flags, rc):
    thread.start_new_thread( disconnect_countdown, (client,)) 
    client.subscribe(sTopicSub+"#")
    for mac_id in mac_list:
        #print(mac_id)
        #print("Connected with result code "+str(rc))
 # reconnect then subscriptions will be renewed.
        client.publish(sTopicPub + mac_id + '/0/cdo/FirmwareVersion', payload = PAYLOAD_GET)
        client.publish(sTopicPub + mac_id + '/1/OnOff/OnOff', payload = PAYLOAD_GET)
        client.publish(sTopicPub +mac_id + '/1/SimpleMeteringServer/CurrentSummationDelivered', payload = PAYLOAD_GET)
        client.publish(sTopicPub + mac_id + '/1/SimpleMeteringServer/InstantaneousDemand', payload = PAYLOAD_GET)
        client.publish(sTopicPub + mac_id + '/1/SimpleMeteringServer/RMSCurrent', payload = PAYLOAD_GET)
        client.publish(sTopicPub + mac_id + '/1/SimpleMeteringServer/Voltage', payload = PAYLOAD_GET)
	client.publish(sTopicPub + mac_id + '/1/SimpleMeteringServer/PowerFactor', payload = PAYLOAD_GET)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    #print(msg.topic+" "+str(msg.payload))
    my_topic = msg.topic[msg.topic.rfind('/')+1:]
    my_mac_id = msg.topic.replace("out/devices/", "")
    my_mac_id = my_mac_id.replace("/1/SimpleMeteringServer/"+my_topic, "")
    my_mac_id = my_mac_id.replace("/0/SimpleMeteringServer/"+my_topic, "")
    my_mac_id = my_mac_id.replace("/1/OnOff/OnOff", "")
    my_value = str(msg.payload.decode("utf8"))
    my_value = my_value.replace('{"response": {"value":',"")
    my_value = my_value.replace("}","")
    

    #print("MY MAC: " +  my_mac_id)
    #print("MY TOPIC: " + my_topic)
    #print("MY VALUE: " + my_value)
    
    #added line so that it publishes every time it gets a message
    #note that the way that this is set up the topic will only be the last part of the OG topics(much shorter)
    # for example /in/devices/<mac_address>/1/SimpleMeteringServer/InstantaneousDemand --> InstantaneousDemand
    client.publish(my_topic, payload = my_value)
    

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(user, password)
client.connect(host, port, keepalive)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
 # manual interface.
client.loop_forever()

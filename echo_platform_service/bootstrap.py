#!/usr/bin/python

import sys
import socket
import subprocess
import time
import fileinput

# The program needs to be executed as
# bootstrap.py <device_uuid> <registry_url> <mqtt_client> <resource_update_frequency<>

device_uuid = sys.argv[1]
registry_url = sys.argv[2]
mqtt_client = sys.argv[3]
kafka_ip = sys.argv[4]
resource_update_frequency = sys.argv[5]

# to retrieve the local IP address
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
local_ip = s.getsockname()[0]
s.close()

# change the IP address in nifi.properites
with open('/nifi-1.2.0/conf/nifi.properties', 'r') as in_file:
    text = in_file.read()

with open('/nifi-1.2.0/conf/nifi.properties', 'w') as out_file:
    out_file.write(text.replace('REMOTE_HOST_IP', local_ip))
print "Changed nifi.properties file. Added local IP."


nificommand = subprocess.Popen(['/nifi-1.1.2/bin/nifi.sh', 'start'])
print "Starting NiFi."

while True:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', 8080))
    if result == 0:
        break
    else:
        time.sleep(4)

print "Starting the platform service"
subprocess.call(['/echo_platform_service/echo_platform_service.py', device_uuid, registry_url, mqtt_client, kafka_ip,
                 resource_update_frequency, local_ip])

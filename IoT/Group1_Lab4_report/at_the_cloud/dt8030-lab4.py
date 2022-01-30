###
# Copyright 2017, Google, Inc.
# Licensed under the Apache License, Version 2.0 (the `License`);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an `AS IS` BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###

#!/usr/bin/python

import datetime
import jwt
import paho.mqtt.client as mqtt
import sys
import time
from sense_hat import SenseHat


########################################################################
# Define project-based variables.
# You need to edit this block of variables to run your script
#    Description:
#
#  - ssl_private_key_filepath: The complete path to the private key file
#  - ssl_algorithm: Either RS256 or ES256. We normally use RS256, but it
#                   depends on how do you had generated the digital
#                   certificate
#  - root_cert_filepath: The complete path to the Google Root
#                        certificate
#  - project_id: Your project ID
#  - gcp_location: The region used when the registry was created
#  - registry_id: The ID of the device registry
#  - device_id: The ID of the device

ssl_private_key_filepath = '/home/pi/demo_private.pem'   # /home/pi/demo_private.pem
ssl_algorithm = 'RS256'              # RS256
root_cert_filepath = '/home/pi/roots.pem'         # /home/pi/roots.pem
project_id = 'proj-saqher20-1'                 # your project ID
gcp_location = 'europe-west1'               # europe-west1
registry_id = 'raspberry-events'                # the registry name (raspberry-pi-2)
device_id = 'rasp1'                  # the device ID (rasp2)

########################################################################
# This code is used to create a connection to the Google Cloud

# Get the current time (UTC)
cur_time = datetime.datetime.utcnow()

# Create the authentication token
def create_jwt():
    token = {
        'iat': cur_time,
        'exp': cur_time + datetime.timedelta(hours=24),
        'aud': project_id
    }
    # Read the private certificate file
    with open(ssl_private_key_filepath, 'r') as f:
        private_key = f.read()
    # Encrypt the data for authentication
    return jwt.encode(token, private_key, ssl_algorithm)


# These variables are used to store the location of the corresponding
# device and topic (URL)
_CLIENT_ID = 'projects/{}/locations/{}/registries/{}/devices/{}'.format(
    project_id, gcp_location, registry_id, device_id)
_MQTT_TOPIC = '/devices/{}/events'.format(device_id)

# Create a MQTT Client to connect to the cloud
client = mqtt.Client(client_id=_CLIENT_ID)
# Set the authentication details
client.username_pw_set(
    username='unused',
    password=create_jwt())

# These functions are used by the MQTT Client to show messages
# - On error
# - When connecting to the cloud
# - When sending data to the cloud


def error_str(rc):
    return '{}: {}'.format(rc, mqtt.error_string(rc))


def on_connect(unusued_client, unused_userdata, unused_flags, rc):
    print('on_connect', error_str(rc))


def on_publish(unused_client, unused_userdata, unused_mid):
    print('on_publish')


client.on_connect = on_connect
client.on_publish = on_publish
client.tls_set(ca_certs=root_cert_filepath)
client.connect('mqtt.googleapis.com', port=8883, keepalive=30)
client.loop_start()

# Create an object to interact with the SenseHat
sense = SenseHat()

DATA_INTERVAL = 10

# Repeat this code until user press CTRL+C
# You should implement your code inside the try ... except code block

while True:

    try:

        # Put your code here! Be creative!
        acceleration = sense.get_accelerometer_raw()
        x = acceleration['x']
        y = acceleration['y']
        z = acceleration['z']

        x = abs(x)
        y = abs(y)
        z = abs(z)

        direction = sense.get_compass()
        
        # create the message to send to the cloud(JSON format)
        payload = '{{ "timestamp": {}, "device_id": "{}", "moved": {}, "accel_x": {}, "accel_y": {}, "accel_z": {}, "direction": {} }}'.format(
            int(round(time.time() * 1000)),           # timestamp of the event
            device_id,             # the device_id (you can use the device_id variable)
            1,                          # whether the device was moved (1: True, 0: False)
            x,                        # acceleration in the X-axis
            y,                        # acceleration in the Y-axis
            z,                        # acceleration in the Z-axis
            direction)                        # direction from magnetometer sensor
        
        # The next line send the data to the Cloud (uncomment it when you code is done!)
        client.publish(_MQTT_TOPIC, payload, qos=1)

        # Use the follow print statement to inspect the message you are going to send to
        # the GCP Pub/Sub topic.
        print("{}\n".format(payload))
        
        time.sleep(DATA_INTERVAL)
        sense.clear()

    except KeyboardInterrupt:
        # Stop the Googgle Cloud Client when CTRL+C was pressed
        client.loop_stop()
        print("Closing.")
        # Finish the program execution
        sys.exit(0)

 #!/usr/bin/env/python
import boto3
from botocore.client import Config
from minio import Minio
from minio.commonconfig import Tags
from minio.select import SelectRequest, CSVInputSerialization, CSVOutputSerialization
import csv
import numpy as np

s3_client = boto3.client('s3')
s3 = boto3.resource('s3',
                    endpoint_url='http://10.0.2.15:9000',
                    aws_access_key_id='minioadmin',
                    aws_secret_access_key='minioadmin',
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

client = Minio('10.0.2.15:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)

value = input("\n enter the category you want to search:  ")
value1 = "weather.csv"
name = []
i = s3.Bucket('datalake')
for i_object in i.objects.all():
     #print(i_object.key)

     tags = client.get_object_tags("datalake", value1)  # metadata has the list of tags
     metadata = []
     for i in tags.keys():
         metadata.append(tags[i])
     #print(metadata)

     if value in metadata:
        name.append(i_object.key)

print("\n objects belonging to Weather category are: ")
print(name)







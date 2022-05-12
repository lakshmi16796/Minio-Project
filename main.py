#!/usr/bin/env/python
from datetime import timedelta

import boto3
from botocore.client import Config
from minio import Minio
from minio.commonconfig import Tags
from minio.select import SelectRequest, CSVInputSerialization, CSVOutputSerialization
import csv
import pandas as pd
import numpy as np

s3_client = boto3.client('s3')
s3 = boto3.resource('s3',
                    endpoint_url='http://10.0.2.15:9000',
                    aws_access_key_id='minioadmin',
                    aws_secret_access_key='minioadmin',
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')


#function to add objects to buckets
def Add_object():
    path = input("enter the objet path name:  ")
    name = input("enter the object name:  ")
    s3.Bucket('datalake').upload_file(path, name)
    print("Object added successfully")


client = Minio('10.0.2.15:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)


#function to search the file and add suitable meta data tags

metadata = []
keywords = ["rain", "cloudy"]
def Add_tags_Intelligently():
    flag = 0
    name_meta = input("enter the name of the object for which you want to add metadata:  ")
    with client.select_object_content(
            "datalake",
            name_meta,
            SelectRequest(
                "select * from S3Object",
                CSVInputSerialization(),
                CSVOutputSerialization(),
                request_progress=True,
            ),
    ) as result:
        for data in result.stream():
            string1 = data.decode()
            for i in range(0, 2):
                if keywords[i] in string1:
                    flag =1
                    #print('Word found.')
                    metadata.append(keywords[i])
            continue


    if flag == 1:
        def unique(metadata):
            x = np.array(metadata)
            print(np.unique(x))

        print("keywords found in the object are: ")
        unique(metadata)
        tags1 = Tags.new_object_tags()

        for i in range(0,2):
            tags1["key1"] = "weather"
            tags1["key2"] = metadata[0]
            tags1["key3"] = metadata[1]
            client.set_object_tags("datalake", name_meta, tags1)

        print("keywords added as metadata tags to objects successfully")

    else:
        print("\n Could not match any keywords in weather category. Please check the csv file uploaded")

#function to attach metadata tags for tect files
def Add_tags_Intelligently_text():
    metadata1 = []
    keywords1 = ['cyclone', 'rainy', 'cloudy', 'flood', 'ocean', 'Air']
    name = input("enter the object name: ")
    flag = 0
    file1 = open("/home/lakshmi/" + name, "r")
    for line in file1:
        #print(line)
        for i in range(0, 6):
            if keywords1[i] in line:
                flag = 1
                metadata1.append(keywords1[i])
                #break


    if flag == 1:
        print("\n The text file belong to category of weather")
        tags2 = Tags.new_object_tags()
        tags2["key1"] = "Weather"
        client.set_object_tags("datalake", name, tags2)

        def unique(metadata1):
            x = np.array(metadata1)
            print(np.unique(x))

        print("\n keywords found in the object are: ")
        unique(metadata1)
        print("\n keywords added as metadata tags to objects successfully")

    else:
        print("\n The document doesn't belong to weather category, please check")


#function to add user defined tags
def Add_mytags():
    name_meta = input("enter the name of the object for which you want to add metadata:  ")
    tag_Key = input("enter the key name:  ")
    tag_value = input("enter the value for the key:  ")
    tags2 = Tags.new_object_tags()
    tags2[tag_Key] = tag_value
    client.set_object_tags("datalake", name_meta, tags2)
    print("Tags added successfully")



#getting url of object and printing column name
def show_scientific_metadata():
    obj_name = input("\n enter the object name: ")

    data = pd.read_csv("/home/lakshmi/" + obj_name)
    print(data.dtypes)

    df = pd.read_csv("/home/lakshmi/" + obj_name)
    print("\nNumber of rows ", len(df.index))
    row_count, column_count = df.shape
    print("Number of columns  ", column_count)
    print("\n Column Fields ")
    for col in data.columns:
        print(col)



while True:
    print("\nMAIN MENU")
    print("1. Add object")
    print("2. Add metadata tags intelligently to csv files")
    print("3. Add user defined metadata tags")
    print("4. Print scientific metadata of csv files")
    print("5. Add metadata tags intelligently to text files")

    choice = int(input("\n Enter the Choice:"))

    if choice == 1:
        Add_object()
    if choice == 2:
        Add_tags_Intelligently()
    if choice == 3:
        Add_mytags()
    if choice == 4:
        show_scientific_metadata()
    if choice == 5:
        Add_tags_Intelligently_text()








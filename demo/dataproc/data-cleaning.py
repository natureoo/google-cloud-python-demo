# !/usr/bin/env python
# coding: utf-8

import sys
import os

# reload(sys)
# sys.setdefaultencoding('utf8')

# sys.path.append('/usr/lib/python2.7/dist-packages')
# sys.path.append('/usr/local/lib/python2.7/dist-packages')
print(sys.path)

# os.system('sudo apt-get install python-pandas -y')
# os.system('sudo apt-get install python-numpy -y')
os.system('sudo apt-get install python3-pip -y')

os.system('sudo pip3 install pandas')
os.system('sudo pip3 install numpy')
os.system('sudo pip3 install google-cloud-storage')
os.system('sudo pip3 install gcsfs')


from pyspark.sql import SparkSession
import pyspark
import numpy as np
from functools import reduce

try:
    from google.cloud import storage
except ImportError:
    print('------------------------------'+sys.path+'------------------------------')

import subprocess

print('******************************test******************************')

spark = SparkSession       .builder       .appName('DataClean')       .getOrCreate()

bucket = 'mpptmp'
tmpbucket = spark._jsc.hadoopConfiguration().get('fs.gs.system.bucket')

input_files='gs://{}/dataproc/input/test.csv'.format(bucket)
# input_files='gs://{}/test1.csv'.format(tmpbucket)
output_directory = 'gs://{}/dataproc/output/'.format(bucket)
output_file = output_directory + '/output.csv'

output_dataset = 'dataproc_dataset'
output_table = 'dataproc_output'



df = spark.read.csv(input_files,header='true')
print(df.head())

def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs()

    # for blob in blobs:
    #     print(blob.name)
    return blobs

def rename_blob(bucket_name, blob_name, new_name):
    """Renames a blob."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print('Blob {} has been renamed to {}'.format(
        blob.name, new_blob.name))


blobs = list_blobs(bucket)



pandas_df = df.toPandas()
to_drop = ['Edition Statement',
           'Corporate Author',
           'Corporate Contributors',
           'Former owner',
           'Engraver',
           'Contributors',
           'Issuance type',
           'Shelfmarks']

pandas_df.drop(to_drop, inplace = True, axis = 1)
pandas_df.head()


pandas_df.set_index('Identifier', inplace = True)


unwanted_characters = ['[', ',', '-']

def clean_dates(item):
    dop= str(item.loc['Date of Publication'])
    
    if dop == 'nan' or dop[0] == '[':
        return np.NaN
    
    for character in unwanted_characters:
        if character in dop:
            character_index = dop.find(character)
            dop = dop[:character_index]
    
    return dop

pandas_df['Date of Publication'] = pandas_df.apply(clean_dates, axis = 1)


def clean_author_names(author):
    
    author = str(author)
    
    if author == 'nan':
        return 'NaN'
    
    author = author.split(',')

    if len(author) == 1:
        name = filter(lambda x: x.isalpha(), author[0])
        return reduce(lambda x, y: x + y, name)
    
    last_name, first_name = author[0], author[1]

    first_name = first_name[:first_name.find('-')] if '-' in first_name else first_name
    
    if first_name.endswith(('.', '.|')):
        parts = first_name.split('.')
        
        if len(parts) > 1:
            first_occurence = first_name.find('.')
            final_occurence = first_name.find('.', first_occurence + 1)
            first_name = first_name[:final_occurence]
        else:
            first_name = first_name[:first_name.find('.')]
    
    last_name = last_name.capitalize()
    
    return first_name+last_name


pandas_df['Author'] = pandas_df['Author'].apply(clean_author_names)



def clean_title(title):
    
    if title == 'nan':
        return 'NaN'
    
    if title[0] == '[':
        title = title[1: title.find(']')]
        
    if 'by' in title:
        title = title[:title.find('by')]
    elif 'By' in title:
        title = title[:title.find('By')]
        
    if '[' in title:
        title = title[:title.find('[')]

    title = title[:-2]
        
    title = list(map(str.capitalize, title.split()))
    return ' '.join(title)
    
pandas_df['Title'] = pandas_df['Title'].apply(clean_title)

pub = pandas_df['Place of Publication']
pandas_df['Place of Publication'] = np.where(pub.str.contains('London'), 'London',
    np.where(pub.str.contains('Oxford'), 'Oxford',
        np.where(pub.eq('Newcastle upon Tyne'),
            'Newcastle-upon-Tyne', df['Place of Publication'])))

pandas_df.to_csv(output_file)


# Shell out to bq CLI to perform BigQuery import.
subprocess.check_call(
    'bq load --source_format CSV '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files}'.format(
        dataset=output_dataset, table=output_table, files=output_file
    ).split())


blobs = list_blobs('bucket')

# for blob in blobs:
#     if blob.name.endswith('csv'):
#         rename_blob(bucket, blob.name, blob.name+'.procceed')

for blob in blobs:
    if blob.name.endswith('csv'):
        rename_blob(bucket, blob.name, blob.name+'.succ')


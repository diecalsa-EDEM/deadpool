
from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from elasticsearch import Elasticsearch 
import json
from pyproj import Proj, transform
from datetime import datetime

class LocationConcat(beam.DoFn):
    """
    Filter data for inserts
    """

    def process(self, element):
        
        date = datetime.now()
        item = json.loads(element)
        
        #Conversion de coordenadas
        inProj = Proj('epsg:25830')
        outProj = Proj('epsg:4258')
        
        #Conversión de coordenadas
        x2,y2 = transform(inProj, outProj,item['geometry']['coordinates'][0],item['geometry']['coordinates'][1])
        
        return [{'timestamp':date.strftime("%Y-%m-%dT%H:%M:%S.000+01:00"),
                 'plazas':int(item['properties']['plazas']),
                 'tipo':item['properties']['tipo'],
                 'id':item['properties']['id'],
                 'location':[y2,x2]               
                 }]


class IndexDocument(beam.DoFn):
   
    es=Elasticsearch([{'host':'localhost','port':9200}])
    
    def process(self,element):
        
        res = self.es.index(index='public_bikes',body=element)
        
        print(res)
 
        
    
def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  
  #1 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      default='projects/hackathon-diego/topics/public_bike_stations',
                      help='Input file to process.')
  #2 Replace your hackathon-edem with your project id 
  parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      default='projects/hackathon-diego/subscriptions/public_bike_streaming',
                      help='Input Subscription')
  
  
  
    
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
   
  google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
  #3 Replace your hackathon-edem with your project id 
  google_cloud_options.project = 'hackathon-diego'
  google_cloud_options.job_name = 'myjob'
 
  # Uncomment below and add your bucket if you want to execute on Dataflow
  #google_cloud_options.staging_location = 'gs://edem-bucket-roberto/binaries'
  #google_cloud_options.temp_location = 'gs://edem-bucket-roberto/temp'

  pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
  #pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
  pipeline_options.view_as(StandardOptions).streaming = True

 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


 

  p = beam.Pipeline(options=pipeline_options)


  # Read the pubsub messages into a PCollection.
  biciStations = p | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  # Print messages received
 
  
  
  biciStations = ( biciStations | beam.ParDo(LocationConcat()))
  
  biciStations | 'Print Quote' >> beam.Map(print)
  
  # Store messages on elastic
  biciStations | 'Bici Stations Stored' >> beam.ParDo(IndexDocument())
  
  
  
 
  result = p.run()
  result.wait_until_finish()

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

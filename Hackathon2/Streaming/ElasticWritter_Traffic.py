#Prueba de datos con kibana

from __future__ import absolute_import
import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import DirectOptions
from elasticsearch import Elasticsearch 
import json
import ast
import utm
from datetime import datetime


class processTraficData(beam.DoFn):

    def process(self, element):

        date = datetime.now()

        item = json.loads(element)
        
        #Conversion de coordenadas

        #Pasamos de string a array de coordenadas (float)
        coordenadas = []
        coord = ast.literal_eval(item['coordinates'])
        
        for i in coord:
            x,y   = utm.to_latlon(i[0],i[1],30, 'S')
            coordenadas.append([y,x])
        
        
        if item['idtramo']=='':
            item['idtramo']=0
        
        if item['estado']=='':
            item['estado']=0
            
        return [{'idtramo':int(item['idtramo']),
                 'denominacion':item['denominacion'],
                 'modified':item['modified'],
                 'estado':int(item['estado']),
                 'timestamp':date.strftime("%Y-%m-%dT%H:%M:%S.000+01:00"),
                 'location':{
                         "type":"linestring",
                         "coordinates":coordenadas
                         }
                 }]


class IndexTraficDocument(beam.DoFn):
   
    es=Elasticsearch([{'host':'localhost','port':9200}])
    
    def process(self,element):
        
        res = self.es.index(index='traffic_status',body=element)
        
        print(res)
    

def run_trafic(argv=None, save_main_session=True):
  #Trafic data
  trafic_parser = argparse.ArgumentParser()
  
  #1 Replace your hackathon-edem with your project id 
  trafic_parser.add_argument('--input_topic',
                      dest='input_topic',
                      #1 Add your project Id and topic name you created
                      # Example projects/versatile-gist-251107/topics/iexCloud',
                      default='projects/hackathon-diego/topics/trafic',
                      help='Input file to process.')
  #2 Replace your hackathon-edem with your project id 
  trafic_parser.add_argument('--input_subscription',
                      dest='input_subscription',
                      #3 Add your project Id and Subscription you created you created
                      # Example projects/versatile-gist-251107/subscriptions/quotesConsumer',
                      default='projects/hackathon-diego/subscriptions/stream',
                      help='Input Subscription')
  
  known_args, pipeline_args = trafic_parser.parse_known_args(argv)

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
  
  pipeline_options.view_as(DirectOptions).direct_num_workers = 4
 
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
 

  p2 = beam.Pipeline(options=pipeline_options)


  # Read the pubsub messages into a PCollection.
  traficstatus = p2 | beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

  # Print messages received
 
  
  
  traficstatus = ( traficstatus | beam.ParDo(processTraficData()))
  
  traficstatus | 'Print Quote' >> beam.Map(print)
  
  # Store messages on elastic
  traficstatus | 'Bici Stations Stored' >> beam.ParDo(IndexTraficDocument())
  
  result = p2.run()
  result.wait_until_finish()
  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run_trafic()

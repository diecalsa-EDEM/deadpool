# Hackathon 2: Improve Smart City application

![Exercise architecture](Hackathon/img/hackaton_logo.png)

## Setup

We are  going to use already provided ElasticStreaming Virtual Machine.

Once the Virtual Machine has started, start NiFi, Elasticsearch, Kibana and Beam:

1. Open a terminal and execute: `Software/nifi-1.9.2/bin/nifi.sh run'` for running NiFi
2. Open a new terminal and execute: `Software/elasticsearch-7.4.2/bin/elasticsearch'` for running Elasticsearch
3. Open a new terminal and execute: `Software/kibana-7.4.2-linux-x86_64/bin/kibana'` for running Kibana
4. Open a new terminal and execute: 
    `python '/home/edem/Repositorio/deadpool/Hackathon/Streaming/ElasticWritter_BikeMeasurement.py' &
     python '/home/edem/Repositorio/deadpool/Hackathon/Streaming/ElasticWritter_BikeMeasurement.py' &
     python '/home/edem/Repositorio/deadpool/Hackathon/Streaming/ElasticWritter_BikeMeasurement.py' &
     python '/home/edem/Repositorio/deadpool/Hackathon/Streaming/ElasticWritter_BikeMeasurement.py'`
     

## Useful Information:

* Urls:

	* Kibana: http://localhost:5601
	* Nifi:   http://localhost:8080/nifi





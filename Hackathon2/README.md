# Hackathon 2: Improve Smart City application

![Deadpool logo](img/hackaton_logo.png)

## Setup production

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


Valencia City Hall has already a control dashboard to visualize Valenbici stations status on real time. 
With this tool, they schedule bicycle movements to ensure availability, and also can do analytics to improve the network adding more slots or creating new stations. 

Here you can have high level architecture of the application provided.

![Exercise architecture](img/Diagrama.png)

## Setup

We are  going to use already provided ElasticStreaming Virtual Machine, but we need to configure it to use 6GB of RAM. 

* Start virtual machine and logon. 

* Open Web Browser and go to  http://console.cloud.google.com/

* Create a new project called hackathon and move to it. 
<img src="https://github.com/rlopezherrero/GFT-EDEM-MasterData/blob/master/Hackathon/img/CreateProject.png" width="75%"><br/>

* Go to pub/sub on left panel and create topic calling it valenbisi.
<img src="./img/CreateTopic.png" width="50%"><br/>

* Click to the topic already created go down and click on Create Subscription called streaming, left rest as default:
<img src="./img/CreateSubscription1.png" width="75%">
<br/>
<img src="./img/CreateSubscription2.png" width="75%">

* Donwload key to be able to consume and populate messages to pub/sub. 
	* Go to left panel to IAM & admin --> Service accounts
	* Click on Create Service  Account
	* Create service account called streaming, and click on Continue:<br/>
	<img src="./img/CreateKey.png" width="75%"><br/>
	
	* Add permissions to pub/sub read and consume, and click Continue:<br/>
	<img src="./img/CreateKey2.png" width="75%"><br/>
	
	* Click on Create key, select JSON as key type and finally on Done. That will download a Json file with your credentials:<br/>
	<img src="./img/CreateKey3.png" width="50%"><br/>
	
	
* Enable pub/sub API
	* Going to API & Services on left panel 
	* Click on Enable Apis & Services
	* Look for Pub/Sub and enable it. 

* Open a terminal and move the json key downloaded to Crendentials folder. Run the following:
```
cp Downloads/your_downloaded_credentials.json Credentials/mobilityApp.json
```  

* Edit /home/edem/bash.rc file commenting previous crendentials and adding this one as google application credentials:
```
#export GOOGLE_APPLICATION_CREDENTIALS=/home/edem/Credentials/iexCloudApp.json
export GOOGLE_APPLICATION_CREDENTIALS=/home/edem/Credentials/mobilityApp.json
```


Create a directory to donwload the Mobility application. Run the following:
```
mkdir hackathon
cd hackathon
git clone https://github.com/rlopezherrero/GFT-EDEM-MasterData.git
```

* Install elastic module for python. Run the following:
```
pip install elasticsearch
```

## Run the platform

* Launch elasticsearch:
	* Open a terminal and run the following
	```
	Software/elasticsearch-7.4.2/bin/elasticsearch
	```  

* Launch kibana:
```
Software/kibana-7.4.2-linux-x86_64/bin/kibana
```  

* Open web browser to validate that kibana has been launched successfully --> http://localhost:5601

* Go to Dev Tools on left pannel and create a mapping for location geo point. 
```
PUT valenbisi
{
  "mappings": {
    "properties": {
      "location": {
        "type": "geo_point"
      }
    }
  }
}
```

* Setup the Dashboards. Go to Management / Kibana (saved objects) / Import 
```
/home/edem/Exercises/hackathon/GFT-EDEM-MasterData/Hackathon/kibana/export.ndjson
```  


## Launching the application

Here you can find a detailed diagram of the application provided:

![Exercise architecture](img/DiagramaDetallado.png)

* Launch Streaming application:
	* Open a terminal and configure google credentials running the following:
	* Launch spyder running 
	```
	spyder
	```  
	* Open mobility application downloaded available on /home/edem/Exercises/hackathon/GFT-EDEM-MasterData/Hackathon/Streaming/ElasticWritter.py
	* Configure your project-id on the code following the comments (1,2,3)
	* Launch it and check the console to  see that is running without errors.


* Launch Nifi:
	* Launch Nifi application
	```
	Software/nifi-1.9.2/bin/nifi.sh run
	```  
	* Load Valenbici template available on /home/edem/Exercises/hackathon/GFT-EDEM-MasterData/Hackathon/nifi/mobilityIngestion.xml
	* Go to the Pub/Sub publisher Box, click on properties and configure the project to use your google project id created. 
	* Finally launch the workflow. 

* Validate on  Spyder console that you see the messages printed. 

* Go to Kibana dashboard and see that you see it updated on real time. 
![Kibana Dashboard](img/KibanaDashboard.png)



## Useful Information:

* Urls:

	* Kibana: http://localhost:5601
	* Nifi:   http://localhost:8080/nifi




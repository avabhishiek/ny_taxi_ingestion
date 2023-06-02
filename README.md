# ny_taxi_ingestion
Reading the NY taxi data via url and storing it in the locally hosted postgres database

To Launch this in your system, please follow below steps:
1. Navigate to the folder where the docker-compose.yml is located and intiate the build & deployment by **docker-compose up -d** command.
2. Once the services are up, you can view them using docker ps
3. If all the services are up, please bash into the application container, it would be called 'compose_image' (you can change it in the docker-compose.yml by calling docker exec -it <container-id> bash.
4. launch test.py script by **python test.py <year>** , year is optional parameter, default is current year

  

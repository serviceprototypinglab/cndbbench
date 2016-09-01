# Experimental cloud-native database comparison
## Overview

This is a testbed in the context of the Cloud-Native Applications research initiative of the Service Prototyping Lab
in Zurich University of Applied Sciences (https://blog.zhaw.ch/icclab/category/research-approach/themes/cloud-native-applications/).
We go to compare different database systems using docker to check their viability for use in cloud-native applications. We will do different tests:

- Performance test
  - Inserts test
  - Selects test
- Scalability test
- Resilience test

The database systems that we compare are:

- MongoDB local
- MongoDB cloud (Kubernetes)
- MongoDB cluster (5 nodes) using Kubernetes and docker
- CouchDB local
- CouchDB cloud (Kubernetes)
- Crate.io local
- Crate.io cloud (Kubernetes)
- Crate.io cluster (5 nodes) using docker
- PostgreSQL local
- PostgreSQL cloud (Kubernetes)
- MySQL local
- MySQL cloud (Kubernetes)
- MySQL cloud (Amazon RDS)
- Aurora

## How
### Containers

We use 6 different containers:

1. MongoDB container: It is a simple container with the official image of 'mongo' in DockerHub.
     It is listening in the port 27017.
2. MongoDB limit RAM container: It is the same that 1, but with a limit in the memory RAM. 
3. CouchDB container: It is a simple container with the official image of 'couch' in DockerHub.
     It is listening in the port 4200.
4. CouchDB limit RAM container: It is the same that 3, but with a limit in the memory RAM.  
5. Crate container: It is a simple container with the official image of 'crate' in DockerHub.
     It is listening in the port 5984.
6. Crate limit RAM container: It is the same that 5, but with a limit in the memory RAM. 
7. Postgres container. It is a simple container with the official image of ‘postgresql’ in DockerHub.
     It is listening in the port 5432.
8. Postgres limit RAM container: It is the same that 7, but with a limit in the memory RAM. 
9. MySQL container. It is a simple container with the official image of 'mysql' in DockerHub.
     It is listening in the port 3306. We changed the configuration variable max_allowed_packets. We changed it to the maximum value.
10. MySQL limit RAM container: It is the same that 9, but with a limit in the memory RAM. 
11. Benchmark container. We use a Dockerfile for create this container.
     From the python official image we add the folder Benchmark (logic of the application). And we add a command for run the code. 
     This container has two volumes too. One is call 'sharedData' where is the dataset that we will use in our test.
     And the volume 'results' where we save the results of our test.
     For more information about the code, the data set or the results see below.
     
### Composition:

We use docker-compose for:
-   Define the images 
-   Define the ports of the containers
-   Define the connection and the dependencies between containers
-   Define the name of the containers
-   Define the volumes in the Benchmark container
-   Define the limit of the memory RAM (if it is necessary)

You can see the file docker-compose.yaml.
Note: The configuration of this file is changing with the different tests.

## Data:

It is a description about the data set that we use:
   - The data that we go to use is in the volume sharedData.
   - The format of the files is JSON.
   - Each file is a array with JSON objects and it represents one table or collection.
   - For the tables with foreign keys:
       - we have a JSON file with the join of the tables 
       - we have one JSON file for each table. 
   - We will use the first for the document databases and the second for the SQL databases.
   - For the SQL databases with have two extra JSON files:
      - creates.json: It has the SQL create table statement.
      - inserts.json: It has the SQL insert statement. 
      (without the data. For get the data we will use the correct file JSON for each table.)
    
## Results

The results are in the volume results. 
There are two ways to show the results:
   1. In JSON format. We will save all the measures in JSON files.
   2. With graphics.
      Using the results saved in JSON files we create graphics comparing the same test in different database.
     
## Logic

In the Benchmark folder we have all the code. It is the logic of the benchmark container.
The files inside the folder are:

- config.json is the file where you can find the name of the tables or collections,
    and all the configuration that you need for connect to tha database (host, port, user, password, dbname, ...)    
- Databases.py:
    In the DocumentDb.py we have the methods for the usual functions that a document database do.
    In Mongo.py and Couch.py are implemented this methods.
    We have the same for the SQL databases with Sqldb.py and Crate.py, Postgres.py, Mysqldb.py.    
- InsertTest.py:
    In this code we measure the time for insert the data in each database.
    And we write the results in a JSON file.
    We insert the data one per one or all together. 
    Also we measure the time for create or delete the table or collections.
- SelectTest.py:
    In this code we measure the time for some selects queries.
- Joins.py:
    We measure the time for a join query in some sql databases and the query that produce the same result in the 
    documents database.     
- DatabaseScalability.py:
    We run the selects test (one loop) 100 times at the same time using threads.    
- ResilienceTest.py:
The resilience test that we do here are:
    - Limit RAM memory.
    - Limit disk size.
    - Kill the container.           
  We provoke these faults when we are inserting data. We will do it, inserting data one per one or all together.     
- Graphics.py  
  It is for create the graphics with the results of the tests. 

## Tests
### Performance
- Inserts
    - Before
      - Local database:
        We need use the correct docker-compose.yml:
         - For MongoDB: docker-compose-mongo.yml
         - For CouchDB: docker-compose-couch.yml
         - For Crate: docker-compose-crate.yml
         - For PostgreSQL: docker-compose-postgres.yml
         - For MySQL: docker-compose-mysql.yml
      - Cloud database:
        - With kubernetes:
          - Create a pod and a service using the correct yaml files for each database. The pods are database-pod.yaml and the 
            svc are database-svc.yaml
        - With amazon:
          - Create a instance of MySQL in Amazon RDS.
          - Create a Aurora instance.
      - Cluster:
        - Mongo:
          - It is a cluster in kubernetes. Follow this instructions for create one. (link)
        - Crate:
          - It is a cluster using docker. Create a cluster with crate is very easy. You must start 5 docker crate containers.
    Check that you have the correct configuration in config.json. The corrects host, ports, ...     
    - Test.
    
      In Test.py add the methods insert_{database}. Where database in {mongo,crate,mysql,couch,postgres}
    - After:

      In the folder results is created a json file where you have the times for:
      - create the database
      - create the tables or collections
      - delete the tables or collections
      - insert 100 rows(or documents) one per one
      - insert all the data
- Selects
    - Before
    
    Run the insert test.
    - Test
    
    In test.py add the method selects_{database}. Where database in {mongo, crate, mysql, couch, postgres}.
    You can add more queries in the method if you want.
    - After
    
    In the folder results is created a json file where you can find the times of the queries. 
### Scalability
- Before

Run the insert test.
- Test

Run the class databaseScalability.py. Where database is in {mongo, crate, mysql, couch, postgres}.
- After

You can see the results in the folder results.
### Resilience
- Kill containers
    - Before
        - Local database:
        
          We need use the correct docker-compose.yml:
           - For MongoDB: docker-compose-mongo.yml
           - For CouchDB: docker-compose-couch.yml
           - For Crate: docker-compose-crate.yml
           - For PostgreSQL: docker-compose-postgres.yml
           - For MySQL: docker-compose-mysql.yml
        Check that you have the correct configuration in config.json. The corrects host, ports, ...   
    - Test
    
    Run the correct method in the ResilienceTest.py. Kill the container in the middle of the process. 
    - After
    
    Check if the data inside the database is correct.
- Ram memory limit
    - Before
    
        - Local database:
        
          We need use the correct docker-compose.yml:
           - For MongoDB: docker-compose-mongo-limit.yml
           - For CouchDB: docker-compose-couch-limit.yml
           - For Crate: docker-compose-crate-limit.yml
           - For PostgreSQL: docker-compose-postgres-limit.yml
           - For MySQL: docker-compose-mysql-limit.yml
        Check that you have the correct configuration in config.json. The corrects host, ports, ...   
    - Test
    
    Run the correct method in the ResilienceTest.py.
    - After
    
    Check if the data inside the database is correct.
- Disk size limit
    - Before
    
        You need a disk with no a lot space. You can use a virtual machine for that.
        - Local database:
        
          We need use the correct docker-compose.yml:
           - For MongoDB: docker-compose-mongo.yml
           - For CouchDB: docker-compose-couch.yml
           - For Crate: docker-compose-crate.yml
           - For PostgreSQL: docker-compose-postgres.yml
           - For MySQL: docker-compose-mysql.yml
        Check that you have the correct configuration in config.json. The corrects host, ports, ...   
    - Test
    
    Run the correct method in the ResilienceTest.py.
    - After
    
    Check if the data inside the database is correct.


## Prerequisites

We need have installed in our machine:
- Docker engine 
- Docker-compose (minimum version 1.6.2)

To update docker-compose to the minimum version on Linux, run:
- sudo apt-get install pip;
- sudo pip install -U pip;
- sudo pip -U install docker-compose.

Afterwards, run 
- ~/.local/bin/docker-compose --version to check.

## Instructions

1. Git clone the repository to your directory.
2. Open the terminal and go to your directory.
3. Execute the  next line:
    - docker build -t benchmark ./Benchmark/
    
We this command we create the image benchmark using our Dockerfile.
And now we have all the images, the officials for the databases and the benchmark image.
4. Execute the next line:
    - docker-compose up 
    
We this command we start of the containers using the definition in docker-compose.yml.

Note: The step 3 is only necessary the first time for create the image benchmark.

## Next steps

We can extends the project in different ways:

1. Add different data sets.
2. Add more databases to compare.
3. Add different selects queries. 
4. Create updates queries.

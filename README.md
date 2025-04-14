# Case Study for Senior Data Engineer at TaxFix
This repository contains solution to the case study provided for Senior Data Engineer position at Taxfix. The case study is to create a pipeline to extract data from [Faker API](https://fakerapi.it/), anonymize, store and generate a report out of it. This repository contains an ETL pipeline developed in Python and data analysis done using SQL.  

#### Table of Contents  
1. [How to run locally](#how-to-run-locally)  
2. [How to make the project production-ready](#how-to-make-the-project-production-ready)
3. [CI/CD Pipeline](#cicd-pipeline)
4. [Changes to make our code production-ready](#changes-to-make-our-code-production-ready)

#### How to run locally  
Requirements:  
In order to run the ETL pipeline & tests locally, you need the following in your system:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.x
- PIP3
- virtualenv

Run ETL inside docker:
- Go to the desired location in your system where you want to clone this repository: `cd ~/path_to/my_folder`
- Clone this repository: `git clone https://github.com/zubair-jaleel/taxfix-case-study.git`
- Get inside the project folder: `cd ./taxfix-case-study`
- Make sure your docker daemon is running (just open Docker Desktop)
- Build docker image named *faker-etl*: `docker build -t faker-etl .`
- Run the docker image in a container named 'faker-etl': `docker run --name faker-etl faker-etl:latest`
- Watch out for the logs, where it says extracting, anonymizing and finally prints out the results of the analysis (it takes a couple of minutes to complete)

Run Python test cases:
- In the project root folder create a python virtual environment named '.venv': `python3 -m venv .venv`
- Activate the environment: `source .venv/bin/activate`
- Install the python packages listed in 'requirements.txt' file: `pip3 install -r requirements.txt`
- Run the tests to see if our code passes all the tests: `pytest tests/`

#### How to make the project production-ready
In order to make this ETL pipeline production-ready it has to be fault-tolerant, scalable, idempotent, version controlled, deployed in CI/CD pipeline, should include observability features like logging, monitoring and alerting, and should adhere to Data Governance best practices. Here I propose an ELT pipeline architecture which could achieve this goal.

**ELT pipeline architecture**
![ELT architecture proposal image](/docs/ETL_Architecture_Proposal.png)  

Explanation:
- The data extraction is done by python scripts running in a docker container which pulls the data from Faker API and store it in S3 bucket. We will not do any analysis in this step because data validation and analysis is done in data warehouse.
- I have chosen AWS as our cloud provider to host our ETL data landscape because it offers variety of services for data engineering/science applications and it has the largest cloud community. I use terraform as IaaC tool to deploy & manage our cloud resources.
- I have chosen ECS Fargate over K8s because of its simplicity, cost and we don't require lot of instances of same ETL running in parallel.
- We build and push the docker image into AWS ECR (Elastic Container Registry) and define ECS (Elastic Container Service) task in CI/CD workflow. The project must contain Dockerfile to build docker image and task-definition.json to define ECS task.
- AWS MWAA (airflow) schedules DAG which has a task to run ETL data extraction to extract and load data into S3 and then another task to load data into snowflake. Any error in the pipeline is notified/alerted to respective stakeholder.
- I have chosen snowflake as our data warehouse because of its features like scaling, data security, governance, granular access control, clustering, zero-copy cloning, time-travel, query performance tuning, etc. It has support for AI/ML and containerisation, however there are other cloud data warehouse providers pioneering in this space.
- I use dbt for data transformations within snowflake because of its features like modular design, version control integration, data validation/testing, built-in documentation and data lineage.
- In Snowflake, we have layered architecture where the data from S3 is first loaded into landing layer table and it might have duplicates. It is then merged into staging layer table using its unique key to eliminate duplicates. We then make the raw data into analysis-ready data by flattening json, standardizing and reorganising. This is done in transform layer. From here, Data Analysts/Scientists will take care of further data processing. They can model the data in analytics layer as per their modeling choice (Dimensional, Data Vault). They can take the data from analytics layer and put it into datamarts in reporting layer. This is the serving layer for our reporting and data science applications.
- To ensure data quality in snowflake I use Data Metric Function with native alerts. We can also do data quality checks with great-expections python library as a separate step in our dataflow orchestrated by airflow, however Snowflake DMF suffice our requirements. I hope to detect issues like data accuracy, completeness, consistency, timeliness, validity and uniqueness.

#### CI/CD Pipeline
![CD/CD pipeline](/docs/ETL_CI_CD_pipeline.png)  

Explanation:
- We develop ETL locally and test it in our docker desktop, if it looks good we push our changes to a remote feature/bugfix branch. A PR will be created, reviewed and approved for merging into our master branch `dev` which will trigger GitHub CI/CD workflow to deploy it in AWS dev account. We must deploy respective changes in our airflow and snowflake dev environments, only then we can test our pipeline end-to-end in dev.
- Running pytest should be part of the CI/CD worflow, we must deploy the code in AWS only if it passes all the tests.
- Once we test our pipeline in dev and ready for deployment into prod, we create another PR to merge `dev` into `prod` branch, which will again be reviewed and approved. Merging to prod will trigger GitHub CI/CD workflow to deploy it in AWS prod account. Similarly we must deploy respective changes in our airflow and snowflake prod environments.

#### Changes to make our code production-ready
Currently our code extracts API data, performs analysis in-memory and outputs the result to console. In order to make it production-ready as proposed, we have to make below changes:
- We have to store the API data as compressed json files in S3. For this we must add code to create compressed json files and store it into S3 bucket using boto3 S3 client.
- API tokens should be stored in AWS secret manager. AWS access/secret keys, credentials and anyother sensitive information should be stored in GitHub repository secrets. It can be retrieved and replaced in our CI/CD worflow using jinja templating variables while building docker image.
- We must add task-definition.json file which contains all parameters to define ECS task.
- We must change our ETL to accept runtime arguments from airflow config parameters and use it in our code. One usecase for this is backfilling from certain date or only certain part of data.
- For incremental data load, we have to get the last extracted timestamp or max column value from snowflake table. For this, we must add code to create connection and query snowflake. Snowflake credentials must be stored and retrieved from GitHub secrets during CI/CD workflow.

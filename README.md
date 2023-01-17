
# This Project shows an example implementation of sorting files in an S3 bucket into two different buckets based on logic involving the content of the files using dynamic task mapping method introduced in Airflow 2.4.

* Create tasks dynamically without knowing the number of files in an S3 bucket before hand
* Use the AWS Provider Package to Interact with AWS-S3
* Use Dynamic Task Mapping to Generate Dynamic Tasks
* Transfer data from a source S3 bucket to the destination bucket


💻 Setup Requirements

You need to have the following:

* Docker and Docker compose on your computer (cf: get Docker)

* The Astro CLI
* Access to a web browser
* AWS Account

To execute this project locally
===============================


### 1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 3 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks



### 2. Verify that all 3 Docker containers were created by running 'docker ps'.


Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

### 3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

### 4. Create a bucket in AWS S3 , and set the name of the bucket in S3_INGEST_BUCKET, line 13 of 2_4_exemple_dag_expand_kwargs.py.

You sou also need to change the name of the buckets in S3_INTEGER_BUCKET and S3_NOT_INTEGER_BUCKET .

### 5. Access the Airflow UI in the menu click Admin> Connections ande crate a new connection with the name 'aws_default', define the Connection Type as Amazon Web Services and put your AWS keys : AWS Access Key ID and AWS Secret Access Key.

Don't forget to click in the button Test before click in Save to verify if your connection is working.




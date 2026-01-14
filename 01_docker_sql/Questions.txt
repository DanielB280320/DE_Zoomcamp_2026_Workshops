### Module 1 Homework: Docker & SQL

<b> Question 1. Understanding docker first run </b>

Run docker with python 3.12.8:
    docker run -it --entrypoint=bash python:3.12.8

Version of pip in the image: 
    pip -V
    pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)

<b> Question 2. Understanding Docker networking and docker-compose </b>

What is the hostname and port that pgadmin should use to connect to the postgres database?

    Hostname: postgres
    Port: 5432
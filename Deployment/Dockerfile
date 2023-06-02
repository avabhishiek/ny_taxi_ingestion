FROM python:3 
#THe base image python 3comes with a unix os pre-installed

WORKDIR /script/
#Commenting the copy as I am mounting the codes as volume allowing for faster testing
COPY /script /script 
COPY requirements.txt .

RUN pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org -r requirements.txt


#RUN apt-get update && apt-get install -y psycopg2
#RUN pip install --no-cache-dir psycopg2 pandas requests --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org
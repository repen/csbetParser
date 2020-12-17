#docker build -t pythoncsgo:latest .
#docker run --privileged -v $(pwd)/parser:/usr/src/parser --name parsercsgo --network="host" -d pythoncsgo:latest
#docker start parsercsgo && docker exec parsercsgo python3 main.py && docker stop parsercsgo
#docker run --rm -d -v csbet:/usr/src/data --name parsercsgo --network=mynet pythoncsgo:latest
#FROM python:3.6-slim
#docker run --rm -d -v csbet:/usr/src/data --name parsercsgo --network=mynet pythoncsgo:latest
FROM python:3.6-alpine


ENV path /usr/src
#COPY parser ${path}
WORKDIR ${path}


COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt


ENV APP_PATH ${path}

#CMD tail -f /dev/null
CMD ["python", "main.py"]
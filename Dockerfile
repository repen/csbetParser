#docker build -t csbet_parser:latest .
#docker run -v betscsgo:/usr/src/data --name csparser csbet_parser:latest
FROM python:3.6-alpine


ENV path /usr/src
COPY parser ${path}
WORKDIR ${path}


COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt


ENV BASE_DIR ${path}

#CMD tail -f /dev/null
CMD python main.py
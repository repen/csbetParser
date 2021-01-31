#docker build -t csparser_tester:latest .
#docker run --rm -d -v betscsgo_tester:/usr/src/data --name csparsertester csparser_tester:latest
FROM python:3.8


ENV path /usr/src
COPY parser ${path}
WORKDIR ${path}


COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt


ENV BASE_DIR ${path}

#CMD tail -f /dev/null
CMD python main.py
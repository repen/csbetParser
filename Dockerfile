#docker build -t csparser:latest .
#docker run -d -v betscsgo_tester:/usr/src/data --name csparserapp csparser:latest
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
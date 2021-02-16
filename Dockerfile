#docker build -t csparser:latest .
#docker run -d -v betscsgo_tester:/usr/src/data --name csparserapp csparser:latest
FROM python:3.8

ENV YADISK_TOKEN not
ENV path /usr/src

WORKDIR ${path}


COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
COPY parser ${path}

ENV BASE_DIR ${path}

#CMD tail -f /dev/null
CMD python main.py
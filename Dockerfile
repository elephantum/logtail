FROM ubuntu:14.04

RUN apt-get update # 2014-12-12
RUN apt-get -y install python-pip python-dev

RUN pip install schedule
RUN pip install graphiteudp==0.0.2
RUN pip install numpy==1.8.0
RUN pip install pandas==0.13.1
RUN pip install python-dateutil==2.2
RUN pip install pytz==2013.9
RUN pip install six==1.5.2
RUN pip install socketcache==0.1.0
RUN pip install wsgiref==0.1.2

ADD . /src/app

VOLUME /var/log
VOLUME /tmp/app

CMD ["python","/src/app/example_log_parser.py"]

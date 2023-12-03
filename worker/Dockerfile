FROM python

COPY requirements.txt /worker/requirements.txt

RUN pip3 install -r /worker/requirements.txt

COPY . /worker

WORKDIR /worker
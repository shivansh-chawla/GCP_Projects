FROM python:3.7

ENV APP_HOME /OverTheAir
WORKDIR $APP_HOME
COPY OverTheAir/deploy_changes.py ./
COPY OverTheAir/config.json ./

RUN python3 deploy_changes.py
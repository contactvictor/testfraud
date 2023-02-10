FROM python:3.11
WORKDIR /app
COPY ./requirements.txt /app/
RUN pip install -r requirements.txt
COPY ./ /app/
#CMD ["python", "server.py"]
#EXPOSE 8888
EXPOSE 5000

ENTRYPOINT ["python3"]
CMD ["server.py"]

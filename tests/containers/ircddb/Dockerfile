# A stand-in for an ircDDB server, so a gateway under test never touches the real network
FROM python:3-slim
COPY server.py /usr/local/bin/server.py
EXPOSE 9007
ENTRYPOINT ["python3", "-u", "/usr/local/bin/server.py"]

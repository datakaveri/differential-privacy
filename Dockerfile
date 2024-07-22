# syntax=docker/dockerfile:1

FROM python:3.12-slim

WORKDIR /flask-server

COPY . .

RUN pip3 install -r requirements.txt

EXPOSE 5000

# CMD [ "python", "scripts/app.py"]
CMD ["flask", "--app=scripts/app.py", "run", "--host=0.0.0.0"]
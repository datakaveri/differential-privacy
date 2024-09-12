FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 6000
CMD ["flask", "--app=iudx_dp_server.py", "run", "--host=0.0.0.0", "--port=6000"]
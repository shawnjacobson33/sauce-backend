FROM python:3.12-slim

COPY requirements.txt ./

RUN pip install --no-cache-dir -r ./requirements.txt

COPY . ./app

CMD ["streamlit", "run", "./app/app.py"]

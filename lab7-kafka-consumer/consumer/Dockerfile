FROM python:3.8-slim
RUN apt-get update

# Change working directory to /opt/app
WORKDIR /opt/app/

# Install python requirements.
COPY requirements.txt /opt/app/
RUN pip install --upgrade pip
RUN pip install -r ./requirements.txt

# Copy the rest files.
COPY .. /opt/app/

# Run main file.
CMD python main.py
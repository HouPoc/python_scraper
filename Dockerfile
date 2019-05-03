# Use an official Python runtime as a parent image
FROM python:3.7-slim

# Set the working directory
WORKDIR /container

# Copy the current directory contents into the container at container
COPY . /container

VOLUME /container/scrape_result
# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

EXPOSE 80
# Define environment variable
ENV NAME python_scraper

# Run app.py when the container launches
CMD ["python3", "-u" ,"main.py"]
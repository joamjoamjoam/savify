# Use an official Python runtime as a base image
#FROM --platform=linux/amd64 debian:10
FROM --platform=linux/arm64/v8 debian:10

# Install any needed packages specified in requirements.txt

# Clone repo to container
COPY . /savify
WORKDIR /savify

RUN apt update
RUN apt install -y git python3 python3-pip ffmpeg python3-requests python3-setuptools

# Install dependencies and setup savify from source
RUN python3 setup.py install

# Define environment variable as placeholder variables
ENV SPOTIPY_CLIENT_ID=
ENV SPOTIPY_CLIENT_SECRET=

# Execute savify when container is started
ENTRYPOINT ["/savify/savespotify"]

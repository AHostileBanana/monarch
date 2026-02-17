##########################################################
# Build MonarchMoney from Source 
##########################################################

# Use a full Python image as the builder stage
FROM python:3.14-trixie AS builder

# Install git and openssh-client within the builder stage
RUN apt-get update && apt-get install -y git openssh-client

# Set the working directory; creates it too
# perhaps /app was already on the image and perhaps it was already the workdir? 
WORKDIR /app
COPY requirements.txt /app/

# Install dependencies and build your application (if necessary)
# e.g., pip install -r requirements.txt
# e.g., pip install . (if you have a setup.py file)
RUN pip install -r requirements.txt    

COPY monarchmoney.py.patch /app/

RUN git clone https://github.com/hammem/monarchmoney.git

COPY monarchmoney.py.patch /app/monarchmoney/monarchmoney

# installs to /usr/local/lib/python3.14/site-packages
RUN cd /app/monarchmoney/monarchmoney && \
  patch monarchmoney.py < ../../monarchmoney.py.patch && \
  cd /app/monarchmoney && \
  python setup.py install

##########################################################
# Build Monarch from Source, pulling in monarchmoney
##########################################################
FROM python:3.14-slim AS final

COPY --from=builder /usr/local/lib/python3.14/site-packages/monarchmoney /usr/local/lib/python3.14/site-packages/monarchmoney
COPY --from=builder /usr/local/lib/python3.14/site-packages/monarchmoney-0.1.15-py3.14.egg-info /usr/local/lib/python3.14/site-packages/monarchmoney-0.1.15-py3.14.egg-info

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt

# do this last as it changes most often and we don't want
# to invalidate prior layers
COPY monarch.py entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

SHELL ["/bin/bash","-c"]
ENTRYPOINT ./entrypoint.sh
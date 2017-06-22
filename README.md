# Privacy Please: Preserving User Privacy in a Smart Building Era

# Table of Contents
1. [Introduction](README.md#introduction)
2. [Details of Implementation](README.md#details-of-implementation)
3. [Repo directory structure](README.md#repo-directory-structure)


# Introduction
This application is designed to preserve user privacy by encrypting streaming WiFi logs. The challenge is to encrypt time ranges in the logs and then to make queries over the encrypted data. The application supports the following features:

**Feature 1:** Encrypt time range fields in WiFi logs.

**Feature 2:** Queries involve a time range. Be able to query over encrypted time ranges.

**Feature 3:** Support 2 Smart building applications: "User presence check" and "People counting".

The project's URL is available at [http://privacyplease.website](http://privacyplease.website).


# Details of Implementation
The project used Kafka, Spark Streaming, MySQL, Flask and a Proxy server to implement an end-to-end data pipeline on Amazon Web Services (AWS).

## Pipeline
A producer (written in Python) reads WiFi logs from a csv file and streams to Kafka. Each line in the stream has the following tab-separated fields:

 - Client IP Address
 - Client MAC Address
 - Association Time
 - Vendor
 - Access Point Name
 - Device Name
 - Map Location
 - SSID
 - Profile
 - VLAN ID
 - Protocol
 - Session Duration
 - Policy Type
 - Avg. Session Throughput (Kbps)
![](images/pipeline.png)

The consumer (Spark Streaming) receives the data stream, encrypts the stream and inserts the encrypted data into a MySQL database. Only the following fields are considered in this project:

 - Client MAC Address
 - Access Point Name
 - Time Ranges
 - Session Duration

When the user runs either "User presence check" or "People counting" application, Flask generates plaintext queries and send them to the Proxy server. The Proxy server converts the plaintext queries into ciphertext queries, send them to MySQL database and retrieves encrypted data. The Proxy server decrypts the data and returns the plaintext results to Flask. Data processing is performed in a privacy-preserving fashion without them being ever revealed in plaintext to the data database server. Finally, Flask displays the plaintext data to the user.


## Repo Directory Structure

The directory structure for my repo looks like this:

    |____flask
    | |____app
    | | |____static
    | | | |____styles
    | | |____templates
    | | |____views.py
    | |____run.py
    | |____tornadoapp.py
    |____spark
    | |____spark-processing.py
    |____stream
    | |____stream.py

 - `flask`: Contains all necessary files (including style and HTML template files) to fun Flask. [*Tornado Server*](http://www.tornadoweb.org/en/stable/) is used to help my Flask apps to handle multiple users without crashing it.
 - `spark`: This is my consumer. It receives the data stream from Kafka, encrypts the stream and inserts the encrypted data into a MySQL database.
 - `stream`: Reads the data (WiFi logs) from a csv file, formats it and streams to Kafka. Takes a Kafka topic in as an argument.

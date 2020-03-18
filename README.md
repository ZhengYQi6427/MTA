# MTA Subway Trip Planner

In this project, we will provide suggestions on whether to switch to express trains or not in certain express stations.
The major sub-modules of this system include:

1. Hardware platform Installed in Each Subway (Local Station Gateway) - This hardware platform consists of an Intel Edison board, along with a range of sensors and actuators. The Intel Edison board also provides WiFi and Bluetooth Low Energy (BLE) connectivity, which allows the sensor data streams to be sent over to a cloud database for storage and data analytics.

2. Cloud Database – The cloud database can store a multitude of data streams. There are many cloud database services available, including Facebook Parse, Google Firebase, Microsoft Azure, and IBM Bluemix. We will be using Amazon Web Services (AWS). 

3. Data Analytics Engine – A major advantage of storing the large amount of data streams on the cloud is that they can be fed into a data analytics engine or machine learning software. AWS has their own suite of machine learning tools that we can leverage to build a real-time prediction engine to determine whether or not a subway rider should switch to the express line at 96th street or stay on the 1 line.

4. Open-Source MTA Data Stream – The NYC MTA provides real-time data of the arrival and departure times of each subway at each station. This information is streamed to our AWS cloud to be used in conjunction with the local sensor data on each subway to build our real-time subway arrival prediction engine.

(Some private parts of the project is not included)

# flight-delay-predict

Link Training Data: https://drive.google.com/uc?id=1rSmpN_7dqb08XBCILyAmxXLjprwBj1P5

# install dependencies:
pip install kafka-python pandas
pip install git+https://github.com/dpkp/kafka-python.git
pip install pyspark
pip install cassandra-driver

# note
install python 3.11.8 for the best compapility with pyspark-cassandra
set PYSPARK_PYTHON with the path to python.exe in env variables

# Need install WSL

# Need to end of Line Sequence to LF

# Turn off spark temp file deletion
Open your %SPARK_HOME%\config folder and add the following config to log4j2.properties.
If the file does not exist, make a copy of log4j2.properties.template then rename it to log4j2.properties and add the config.

logger.shutdownhookmanager.name = org.apache.spark.util.ShutdownHookManager
logger.shutdownhookmanager.level = OFF
logger.sparkenv.name = org.apache.spark.SparkEnv
logger.sparkenv.level = ERROR

# Make sure streaming\kafka-checkpoint and streaming\spark-temp folders are created

# Setup dataset
Dataset link: https://www.kaggle.com/datasets/phamtheds/predict-flight-delays?select=stream_data.csv
Create a folder "kg-flightdelay-dataset" in "Final_Data", download stream_data.csv from kaggle link above and put it in "kg-flightdelay-dataset"

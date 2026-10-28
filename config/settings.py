import os

# laptop pe ye default values chalti hain, docker mein environment se badal jaati hain
kafka_server = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
data_folder = os.environ.get("DATA_FOLDER", "C:/pipeline-data/")
quarantine_folder = os.environ.get("QUARANTINE_FOLDER", "data/quarantine/")
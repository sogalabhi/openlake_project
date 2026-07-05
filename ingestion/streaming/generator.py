import json
import pandas as pd
import time
from kafka import KafkaProducer

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers='localhost:19092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    csv_path = "data/online_retail_II.csv"

    df = pd.read_csv(csv_path)

    for index, row in df.iterrows():
        # row is a pandas Series
        # convert to dict for JSON serialization
        row_dict = row.to_dict()
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}
        producer.send('live_orders', value=row_dict)
        time.sleep(0.5)
        # if index%10 == 0:
        print(f"{index} done")

    producer.flush()

    print("All messages sent and flushed")
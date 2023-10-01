from google.cloud import bigtable
import os
import logging
import base64
import json
import datetime

def insert_big_table(event, context):
    project_id = os.environ.get("GCP_PROJECT")
    instance_name = "qcar-streamingdata"
    table_name = "metrics"
    pubsub_message = base64.b64decode(event['data']).decode()
    pubsub_message_dic = json.loads(pubsub_message)

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_name)
    
    count_table = "count"
    table = instance.table(count_table)
    
    key = "row1".encode()
    column = "rowkeycount".encode()

    row = table.read_row(key)
    cell = row.cells["count"][column][0]
    print(cell.value.decode("utf-8"))
    
    table = instance.table(table_name)
    
    logging.info(f"Got the details, step 1 complete.")

    timestamp = datetime.datetime.utcnow()
    column_family_id = "metrics"
    
    row_key = cell.value.decode("utf-8")
    introw_key = int(row_key) + 1
    row_key = str(introw_key)

    speed = pubsub_message_dic["speed"]
    distance = pubsub_message_dic["distance"]
    strspeed = str(speed)
    strdistance = str(distance)

    row = table.direct_row(row_key)
    row.set_cell(column_family_id, "speed", strspeed, timestamp)
    row.set_cell(column_family_id, "distance", strdistance, timestamp)

    row.commit()

    print("Successfully wrote row {}.".format(row_key))

    table = instance.table(count_table)
    
    column_family_id = "count"
    row = table.direct_row(key)
    row.delete_cell(column_family_id, "rowkeycount")
    row.set_cell(column_family_id, "rowkeycount", row_key)

    row.commit()

    print("Successfully wrote row {}.".format(key))

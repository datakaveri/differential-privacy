import json
import math

# Open the large JSON file for reading
with open('suratITMSDPtest.json', 'r') as f:

    # Load the entire JSON object
    json_obj = json.load(f)

    # Set the number of chunks to split the object into
    num_chunks = 10

    # Calculate the number of items in each chunk
    chunk_size = math.ceil(len(json_obj) / num_chunks)

    # Split the JSON object into smaller chunks
    chunks = [json_obj[i:i+chunk_size] for i in range(0, len(json_obj), chunk_size)]

    # Write each chunk to a separate file
    for i, chunk in enumerate(chunks):
        with open(f'split_file_{i}.json', 'w') as new_file:
            json.dump(chunk, new_file)

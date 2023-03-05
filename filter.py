import json
import time
"""
This code use to filter latitude, longitude and date of P200 messages
The filter operation is performed to 'parsed_w_precise_coords.json'
"""

def load_data(data):
    file = open(data)
    data = json.load(file)
    return data

def filter(data):
    extracted_data = []
    for obj in data:
        # create a new dictionary to store the extracted data for each object
        new_obj = {}
        new_obj["date"] = obj["date"]
        new_obj["zipcode"] = obj["zipcode"]
        new_obj["lat"] = obj["latitude"]
        new_obj["lng"] = obj["longitude"]

        # add the new dictionary to the list
        extracted_data.append(new_obj)
    
    with open("extracted_data.json", "w") as file:
        json.dump(extracted_data, file, indent=4)
    
def main():
    data = load_data('parsed_w_precise_coords.json')
    filter(data)

if __name__ == '__main__':
    main()
#This file is for reading and preparing data from files.
import re
import csv
import json

class FileReading():
    
    def file_reading_failure(self):
        with open('docs/equpment_failure_sensors.txt', 'r') as file:
            reader = csv.reader(file)
            #Skip the first line
            next(reader)
            
            regex = re.compile(r"\[(.*?)\]\s+(.*?)\s+sensor\[(\d+)\]")
            list_logs = []

            for line in reader:
                match = regex.search(line[0])

                # If there is a match, extract the information.
                if match:
                    timestamp, error, sensor = match.groups()
                    log = {
                        "datahour": timestamp,
                        "typelog": error,
                        "sensor": sensor
                    }
                    list_logs.append(log)

                else:
                    print("There was no match.")
                    print("MATCH ------- ", match)

            return list_logs
        
    def file_reading_sensors(self):
        with open('docs/equipment_sensors.csv', 'r') as file:
            reader = csv.reader(file)

            next(reader)

            list_sensors = []
            for line in reader:
                dictionary = {
                    "equipment_id": line[0],
                    "sensor_id": line[1]
                }
                list_sensors.append(dictionary)

            return list_sensors
        
    def file_reading_equipment(self):
        with open('docs/equipment.json', encoding='utf-8') as file:
            reader = json.load(file)
            list_equipment = []
            for dic in reader:
                dictionary = {
                    "equipment_id": dic["equipment_id"],
                    "name": dic["name"],
                    "group_name": dic["group_name"]
                }
                list_equipment.append(dictionary)

            return list_equipment


            

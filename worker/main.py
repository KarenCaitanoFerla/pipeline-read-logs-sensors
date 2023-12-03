from src.file_reading import FileReading
from src.database_operations import ConectionDataBase
from src.databasequery import DataBaseQuery

conn = ConectionDataBase()
conn.init_databaselogs()

file = FileReading()

print("Starting extraction and insertion")

list_logs = file.file_reading_failure()
conn.insert_data_on_table(list_logs, "logsfailure")

list_sensors = file.file_reading_sensors()
conn.insert_data_on_table(list_sensors, "sensors")

list_equipment = file.file_reading_equipment()
conn.insert_data_on_table(list_equipment, "equipment")

#Question 1
search = DataBaseQuery()
total_failures_query = search.total_failures()
total_failure = conn.execute_query(total_failures_query)
print(f'ANSWER 1) Total equipment failures that happened: {total_failure[0]}')

#Question 2
equipment_id_name_query = search.equipment_id_name_query()
equipment_id_name = conn.execute_query(equipment_id_name_query)
print(f'ANSWER 2) Equipment name had most failures: {equipment_id_name}')

#Question 3
media_equipments_query = search.group_equipment()
media_equipments = conn.execute_query_all_results(media_equipments_query)
print('ANSWER 3) Average amount of failures across equipment group, ordered by the number of failures in ascending order:')
for equipment_mean in media_equipments:
    print(equipment_mean)

#Question 4
group_most_error_sensor_query = search.group_most_error_sensor()
group_most_error_sensor =  conn.execute_query_all_results(group_most_error_sensor_query)
print("ANSWER 4) Rank the sensors which present the most number of errors by equipment name in an equipment group:")
for group in group_most_error_sensor:
    print(group)


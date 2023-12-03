class DataBaseQuery():

    def total_failures(self):
        word = 'ERROR'
        query = f"SELECT COUNT(*) FROM logsfailure WHERE typelog LIKE '%{word}%'"
        return query
    
    def equipment_id_name_query(self):
        query = """select equipment.name, count(*) as error_quant from equipment 
                        inner join sensors on equipment.equipment_id = sensors.equipment_id
                        inner join logsfailure on sensors.sensor_id = logsfailure.sensor
                    where logsfailure.typelog = "ERROR"
                    group by equipment.name order by error_quant desc limit 1
        """
        return query
    
    def name_equipment_sensor(self, sensor_id):
        query = f"""
            SELECT equipment_id
            FROM sensors
            WHERE sensor_id = '{sensor_id}';
        """
        return query
    
    def equipment_id_name(self, equipment_id):
        query = f"""
            SELECT name
            FROM equipment
            WHERE equipment_id = '{equipment_id}';
        """
        return query
    
    def group_equipment(self):
        query = """
            select equipment.group_name, count(*)/group_quant_equipment.quant_equipment as error_mean from equipment 
                inner join sensors on equipment.equipment_id = sensors.equipment_id
                inner join logsfailure on sensors.sensor_id = logsfailure.sensor
                inner join (select equipment.group_name, count(*) as quant_equipment from equipment
                            group by equipment.group_name) as group_quant_equipment on equipment.group_name = group_quant_equipment.group_name
            where logsfailure.typelog = "ERROR"
            group by equipment.group_name order by error_mean asc
        """
        return query

    def group_most_error_sensor(self):
        query = """
            select group_name, name, sensor_id, error_quant from  (SELECT
                    group_name,
                    name,
                    sensor_id,
                    error_quant,
                    ROW_NUMBER() OVER (PARTITION BY group_name ORDER BY error_quant DESC) AS ranking
                FROM
                    (select equipment.group_name,
                    equipment.name,
                    sensors.sensor_id,
                    count(*) as error_quant 
                from equipment
                inner join sensors on equipment.equipment_id = sensors.equipment_id
                inner join logsfailure on sensors.sensor_id = logsfailure.sensor
            where logsfailure.typelog = "ERROR"
            group by equipment.group_name, equipment.name, sensors.sensor_id
            ) as errors_per_sensor
            ) as rank_errors_group WHERE ranking = 1 order by error_quant desc;
        """
        return query

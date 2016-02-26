DROP TABLE hospitals;
CREATE EXTERNAL TABLE hospitals(id INT, name VARCHAR(100), address VARCHAR(200), 
                        city VARCHAR(50), state VARCHAR(10), zipcode VARCHAR(10), 
                        county_name VARCHAR(50), phone_number VARCHAR(50), hospital_type VARCHAR(100),
                        ownership VARCHAR(100), emergency_services BOOLEAN)
                        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                        WITH SERDEPROPERTIES (
                            "separatorChar" = ",",
                            "quoteChar" = '"',
                            "escapeChar" = '\\'
                        )
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/hospitals';

DROP TABLE measures;
CREATE EXTERNAL TABLE measures(name VARCHAR(100), id VARCHAR(50), 
                start_quarter VARCHAR(20), start_date VARCHAR(50),
                end_quarter VARCHAR(20), end_date VARCHAR(50))
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                WITH SERDEPROPERTIES (
                    "separatorChar" = ",",
                    "quoteChar" = '"',
                    "escapeChar" = '\\'
                )
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/measures';

DROP TABLE effective_care;
CREATE EXTERNAL TABLE effective_care(hospital_id INT, hospital_name VARCHAR(100), hospital_address VARCHAR(200), 
                        hospital_city VARCHAR(50), hospital_state VARCHAR(10), hospital_zipcode VARCHAR(10), 
                        hospital_county_name VARCHAR(50), hospital_phone_number VARCHAR(50),
                        condition VARCHAR(100), measure_id VARCHAR(50), measure_name VARCHAR(100),
                score INT, sample INT, footnote VARCHAR(200), measure_start_date VARCHAR(50), measure_end_date VARCHAR(50))
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                "separatorChar" = ",",
                "quoteChar" = '"',
                "escapeChar" = '\\'
            )
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/effective_care'; 

DROP TABLE readmissions;
CREATE EXTERNAL TABLE readmissions(hospital_id INT, hospital_name VARCHAR(100), hospital_address VARCHAR(200), 
                        hospital_city VARCHAR(50), hospital_state VARCHAR(10), hospital_zipcode VARCHAR(10), 
                        hospital_county_name VARCHAR(50), hospital_phone_number VARCHAR(50),
                        measure_name VARCHAR(100), measure_id VARCHAR(50), compared_to_national VARCHAR(50),
                        denominator INT, score INT, lower_estimate INT, higher_estimate INT)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                "separatorChar" = ",",
                "quoteChar" = '"',
                "escapeChar" = '\\'
            )
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/readmissions'; 

DROP TABLE surveys;
CREATE TABLE surveys (hospital_id INT, hospital_name VARCHAR(100), hospital_address VARCHAR(200), 
                        hospital_city VARCHAR(50), hospital_state VARCHAR(10), hospital_zipcode VARCHAR(10), 
                        hospital_county_name VARCHAR(50), 
                        comm_nurses_ach VARCHAR(20), comm_nurses_imp VARCHAR(20), comm_nurses_dim VARCHAR(20), 
                        comm_doctors_ach VARCHAR(20), comm_doctors_imp VARCHAR(20), comm_doctors_dim VARCHAR(20),
                        resp_staff_ach VARCHAR(20), resp_staff_imp VARCHAR(20), resp_staff_dim VARCHAR(20), 
                        pain_mgmt_ach VARCHAR(20),pain_mgmt_imp VARCHAR(20), pain_mgmt_dim VARCHAR(20),
                        comm_meds_ach VARCHAR(20),comm_meds_imp VARCHAR(20),comm_meds_dim VARCHAR(20),
                        clean_ach VARCHAR(20), clean_imp VARCHAR(20), clean_dim VARCHAR(20),
                        discharge_ach VARCHAR(20), discharge_imp VARCHAR(20), discharge_dim VARCHAR(20),
                        overall_ach VARCHAR(20), overall_imp VARCHAR(20), overall_dim VARCHAR(20), 
                        hcahps_base INT, hcahps_consistency INT)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                "separatorChar" = ",",
                "quoteChar" = '"',
                "escapeChar" = '\\'
            )
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/surveys'; 
                        

from email import header
from hashlib import new
import pandas as pd
import time
import configuration_V2
import os 
import logging
from datetime import datetime
import numpy as np
import random
from kafka import KafkaProducer

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename = "./log/simulator_V2_log.log", level = int(configuration_V2.get_value_config("logging","log-level")), format = LOG_FORMAT, filemode="w")
logger = logging.getLogger()

TOPIC_NAME = 'building-info'
KAFKA_SERVER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

def load_data():
    """
    Creates a data frame from a csv specified in the config.ini
    
    returns: data frame
    """
    logger.info("Entering load_data function")

    df = pd.read_csv(configuration_V2.get_file_config("input_file"))

    logger.info("Exiting load_data function")
    return df

def write_output(df):
    """
    Args: df --> Data frame
    
    Writes data frame to csv
    """
    logger.info("Entering write_output function")
    logger.debug("df: " + str(df))
    
    df.to_csv(configuration_V2.get_file_config("output_file"))

    logger.info("Exiting write_output function")

def get_random_delay(lower, upper):
    """
    Args: lower --> lower bound time delay, upper --> upper bound time delay, rand --> int

    returns calculated time delay
    """
    logger.info("Entering get_random_delay function")
    logger.debug("lower: " + str(lower) + " upper: " + str(upper))
    rand = random.randint(0,1)

    logger.info("Exiting get_random_delay function")
    return (lower + rand * (upper - lower))

def select_sensors_to_update(current, prev):
    '''
    Args: current --> list of current sensor column values, prev --> list of previous sensor column values

    return --> list of indices where the value current column and previous colum values are different
    '''
    logger.info("Entering select_sensors_to_update function")
    logger.debug("current: " + str(current) + " prev: " + str(prev))
    #get list of indices of values in the current list that are not in the prev list

    logger.info("Exiting select_sensors_to_update function")
    return [idx for idx, elem in enumerate(prev) if elem != current[idx]]


def write_outputs(updated_list,current_list, columns ):
    '''
    Args: updated_list --> list of indices where the value current column and previous colum values are different
            current_list --> list of current sensor column values, columns --> list of column names

            Itterates over updated_list,  for each value in updated_list create a dataframe with the current time and value of the sensor at index.
            Write dataframe to csv.
    '''
    logger.info("Entering write_outputs function")
    logger.debug("updated_list: " + str(updated_list) + " current_list: " + str(current_list) + " columns: " + str(columns))


    for idx in updated_list:
        
        now = datetime.now()
        dt_string = now.strftime("%Y-%m-%d-%H-%M-%S-%f")
        current_sensor = {"period": [dt_string],columns[idx]: current_list[idx]}
        sensor_df = pd.DataFrame(data=current_sensor)

        sensor_df_string = sensor_df.to_string()
        byte_string = bytes(sensor_df_string, 'utf-8')
        producer.send(TOPIC_NAME,byte_string)

    logger.info("Exiting write_outputs function")


def main():
    logger.info("Entering Main")

    df = load_data()
    #new_df = pd.DataFrame()
    head = list(df.columns[1:])
    #head = list(df.columns)
    first_itteration = True
    
    for index, row in df.iterrows():

        series = row.copy()
        series = series.drop(['period'])

        current_val_list = series.tolist()
       
        if first_itteration:

            position = 0
            for val in series:
                now = datetime.now()
                dt_string = now.strftime("%Y-%m-%d-%H-%M-%S-%f")
                current_sensor = {"period": [dt_string],head[position]: val}
                sensor_df = pd.DataFrame(data=current_sensor)
                #sensor_df.to_csv(f"./output/V2/{head[position]}-{dt_string}.csv", index=False)
               
                position +=1

                sensor_df_string = sensor_df.to_string()
                byte_string = bytes(sensor_df_string, 'utf-8')
                producer.send(TOPIC_NAME,byte_string)
        else:
            UL = select_sensors_to_update(current_val_list,prev_val_list)
            write_outputs(UL,current_val_list,head)
      

        prev_val_list = current_val_list.copy()
        current_val_list.clear()
        first_itteration = False
        time.sleep(get_random_delay(int(configuration_V2.get_value_config("time-delay",'tdl')),int(configuration_V2.get_value_config("time-delay",'tdu'))))
        producer.flush()
        #time.sleep(20)


    logger.info("Exiting Main")



if __name__ == "__main__":
    main()

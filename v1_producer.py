"""
    Priyanka Gorentla
    Date: 09/22/23
    
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

"""

import pika
import sys
import webbrowser
import csv
import time

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)
# Define Global Variables

# decide if you want to show the offer to open RabbitMQ admin site
# Input "True" or "False"
show_offer = "False"

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    logger.info("Process Startes")
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        logger.info(f"Answer is {ans}.")

def send_message(host: str, queue_nameA: str,queue_nameB: str,queue_nameC: str,input_file: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_nameA,B,C (str): the name of the queues
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(
        host="localhost",
        credentials=pika.PlainCredentials(username="guest", password="Vijjulu@12")))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to delete the queues
        ch.queue_delete(queue=queue_nameA)
        ch.queue_delete(queue=queue_nameB)
        ch.queue_delete(queue=queue_nameA)
        # use the channel to declare the durable queues
        ch.queue_declare(queue="01-smoker", durable=True)
        ch.queue_declare(queue="02-food-A", durable=True)
        ch.queue_declare(queue="02-food-B", durable=True)

        # open the input file
        input_file = open(input_file, "r")
        # read the input file
        reader = csv.reader(input_file,delimiter=",")
        # skip the headers
        header = next(reader)
        # get message from each row of file
        for row in reader:
            # read a row from the file
            TimeStamp, Smoker_Temp, Food_A_Temp, Food_B_Temp = row

             # define three messages
            first_message = TimeStamp, Smoker_Temp
            second_message = TimeStamp, Food_A_Temp
            third_message = TimeStamp, Food_B_Temp
            # encode messages
            first_message_encode = ",".join(first_message).encode()
            second_message_encode = ",".join(second_message).encode()
            third_message_encode = ",".join(third_message).encode()

            # send the message to an individual queue
            ch.basic_publish(exchange="", routing_key=queue_nameA, body=first_message_encode)
            ch.basic_publish(exchange="", routing_key=queue_nameB, body=second_message_encode)
            ch.basic_publish(exchange="", routing_key=queue_nameC, body=third_message_encode)

            #logging messages
            logger.info(f" [x] Sent {first_message} to {queue_nameA}")
            logger.info(f" [x] Sent {second_message} to {queue_nameB}")
            logger.info(f" [x] Sent {third_message} to {queue_nameC}")
            # wait 30 seconds before sending next temp
            time.sleep(30)
        # close the file
        input_file.close()
        
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # show the offer to open Admin Site if show_offer is set to true, else open automatically
    if show_offer == "True":
        offer_rabbitmq_admin_site()
    else:
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()
    # Use the send_message function to start the process
    send_message("localhost", "01-smoker", "02-food-A", "03-food-B", "smoker-temps.csv")   
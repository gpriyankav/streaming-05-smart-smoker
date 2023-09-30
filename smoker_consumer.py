"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Priyanka Gorentla 
    Modified on : 22nd September 2023  

"""

import pika
import sys
import time
from collections import deque

smoker_deque = deque(maxlen = 5)

# Configure logging
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """Defining our behavior for the alerts"""
    #Decode the message and split using a comman delimited system
    tempmesg = body.decode().split(",")
    try:
        #Convert temperature to float
        individualtemp = float(tempmesg[1])
        #Adding message 
        smoker_deque.append(individualtemp)
        #Checking the length of queue
        if len(smoker_deque) == 5: 
         #Calculate the change in temperature
         changetemp = (smoker_deque[0] - smoker_deque[4])
         #Print alert if smoker temperature decreases by more than 15 degrees F in 2.5 minutes
         if changetemp > 15:
                print("Smoker Alert!! This occurred at " + tempmesg[0])
        # acknowledge the message was received and processed 
        # (now it can be deleted from the queue)
        # when done with task, tell the user
        logger.info(" [x] smoker temp.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    #Pass our empty strings to avoid a value error
    except ValueError:
        pass    


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "01-smoker"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=hn,
        credentials=pika.PlainCredentials(username="guest", password="Vijjulu@12")))

    # except, if there's an error, do this
    except Exception as e:
        logger.info()
        logger.info("ERROR: connection to RabbitMQ server failed.")
        logger.info(f"Verify the server is running on host={hn}.")
        logger.info(f"The error says: {e}")
        logger.info()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn, on_message_callback=callback)

        # print a message to the console for the user
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.info()
        logger.info("ERROR: something went wrong.")
        logger.info(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info()
        logger.info(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "01-smoker")

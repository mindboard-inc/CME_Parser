
from data_to_aws_v6 import TickLogger as log
import time, traceback
import datetime
closeTiming = datetime.time(16,59,00).strftime('%H:%M:%S')
def every(delay,logger):
  next_time = time.time() + delay
  while True:
    time.sleep(max(0, next_time - time.time()))
    try:
      sleep_timer(logger)
    except Exception as e:
      print(str(e))
    next_time += (time.time() - next_time) // delay * delay + delay

def sleep_timer(logger):
  millis = datetime.datetime.now ().time ().strftime ('%H:%M:%S')
  delta = datetime.datetime.strptime (closeTiming, '%H:%M:%S') - datetime.datetime.strptime (millis,'%H:%M:%S')
  if delta.seconds == 0 or (delta.seconds <86400 and delta.seconds>82800):
    print("Time to sleep")
    log._sleep(logger)


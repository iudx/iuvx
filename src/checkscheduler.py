import sys
from time import sleep
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess as sp
import signal
import os
sched = BackgroundScheduler()
sched.start()        # start the scheduler
 
# define the function that is to be executed
# it will be executed in a thread by the scheduler
def ffmpeg_archiver(Origin_IP,Stream_ID,Stream_IP):
    print Origin_IP,Stream_ID,Stream_IP

 
def main():
    # job = sched.add_date_job(my_job, datetime(2013, 8, 5, 23, 47, 5), ['text'])
    job = sched.add_job(my_job, run_date='2019-01-15 14:14:05',args=[Origin_IP,Stream_ID,Stream_IP])
    while True:
        sleep(1)
        sys.stdout.write('.'); sys.stdout.flush()
 

 
if __name__ == "__main__":
    main()
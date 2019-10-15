#!/usr/bin/python2
import pymongo
import subprocess as sp
import time


''' Start the load balancer server '''


# def op(p):
#    if p is not None:
#        outs, _ = p.communicate(timeout=2)
#        for line in p.stdout.readlines():
#            print(line,)
#        retval = p.wait()


if __name__ == "__main__":
    print("Dropping Existing Database.....")
    mongoclient = pymongo.MongoClient('mongodb://localhost:27017/')
    mongoclient.drop_database("ALL_Streams")
    ''' Kill all python processes '''
    p0 = sp.Popen(["pkill", "-f", "src/python"], shell=False)
    time.sleep(2)

    print("Starting HTTP Server.........")
    #p1 = sp.Popen(["nohup", "python", "src/HTTPserver.py", "&"], shell=False)

    print("Starting HTTP Server.........")
    p2 = sp.Popen(["celery", "-A", "loadbalancercelery",
                   "worker", "--workdir", "./src", "-f celery.out", "--loglevel", "info"],
                  shell=False)

    print("Starting LoadBalancer...........")
    p3 = sp.Popen(["nohup", "python", "src/celeryLBmain.py", "&"], shell=False)

    print("Video Server Setup Complete....")

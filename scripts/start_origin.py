#!/usr/bin/python3
import subprocess as sp


''' Start the load balancer server '''

# def op(p):
#    if p is not None:
#        outs, _ = p.communicate(timeout=2)
#        for line in p.stdout.readlines():
#            print(line,)
#        retval = p.wait()


if __name__ == "__main__":

    sp.Popen(["nohup", "python", "./src/originffmpegspawner.py", "&"], shell=False)
    print("started spawner")
    sp.Popen(["nohup", "python", "./src/originffmpegkiller.py", "&"], shell=False)
    print("started killer")
    sp.Popen(["nohup", "python", "./src/originstatchecker.py", "&"], shell=False)
    print("started statter")
    sp.Popen(["nohup", "python", "./src/originffmpegarchiver.py", "&"],
             shell=False)
    print("started archiver")
    sp.Popen(["celery", "-A", "OriginCelery", "worker", "--workdir", "./src", "--loglevel", "info"],
              shell=False)
    print("Starting origin celery")

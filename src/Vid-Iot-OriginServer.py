
import subprocess as sp
import os
import signal
import sys

if __name__=="__main__":
	print "Starting Origin FFMPEG Spawner........."
	p1=sp.Popen(["python","originffmpegspawner.py"],shell=False)
	print "Starting Origin FFMPEG Killer..........."
	p2=sp.Popen(["python","originffmpegkiller.py"],shell=False)
	print "Starting Origin Stat Checker........"
	p3=sp.Popen(["python","originstatchecker.py"],shell=False)
	print "Origin Setup Complete...."
	if raw_input()=="q":
		os.kill(p1.pid,signal.SIGKILL)
		os.kill(p2.pid,signal.SIGKILL)
		os.kill(p3.pid,signal.SIGKILL)
		print "Origin Terminated"
		sys.exit()
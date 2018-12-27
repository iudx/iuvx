import os
import signal
import time


if __name__=="__main__":
	while(True):
		try:
			time.sleep(10)
			os.waitpid(-1, os.WNOHANG)
		except Exception as exc:
			continue

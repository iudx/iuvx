import requests
import sys
import time

if __name__=="__main__":
        if sys.argv[1]=="-c":
                with open(sys.argv[2],"r") as f:
                        for i in f:
                                streaminfo=i.split()
				print streaminfo
                                r=requests.get("http://10.156.14.141:5000/streams?ip="+str(streaminfo[0])+"&id="+str(streaminfo[1])+"&opr=add")
				time.sleep(5)
        else:
                r=requests.get("http://10.156.14.141:5000/streams?ip="+str(sys.argv[1])+"&id="+str(sys.argv[2])+"&opr=add")


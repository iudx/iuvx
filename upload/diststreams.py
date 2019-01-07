import requests
import sys
import time

if __name__=="__main__":
        if sys.argv[1]=="-c":
                with open(sys.argv[2],"r") as f:
                        for i in f:
                                streaminfo=i.split()
                                print streaminfo
                                r=requests.get("http://10.156.14.141:5000/reqstream?id="+str(streaminfo[1]))
                                time.sleep(5)
        else:
                r=requests.get("http://10.156.14.141:5000/reqstream?id="+str(sys.argv[2]))


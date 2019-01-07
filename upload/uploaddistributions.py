import requests
import sys


if __name__=="__main__":
        if sys.argv[1]=="-c":
                with open(sys.argv[2],"r") as f:
                        for i in f:
                                distinfo=i.split()
                                r=requests.get("http://10.156.14.141:5000/dist?ip="+str(distinfo[0])+"&id="+str(distinfo[1])+"&opr=add")
        else:
                r=requests.get("http://10.156.14.141:5000/dist?ip="+str(sys.argv[1])+"&id="+str(sys.argv[2])+"&opr=add")


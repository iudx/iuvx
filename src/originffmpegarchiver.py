import os
import sys
from MQTTPubSub import MQTTPubSub
from apscheduler.schedulers.background import BackgroundScheduler
import json
import time
from datetime import datetime
import logging

import OriginCelery as oc

origin_ffmpeg_archive = [0, ""]
origin_ffmpeg_archive_del = [0, ""]

log = logging.getLogger('apscheduler.executors.default')
log.setLevel(logging.INFO)  # DEBUG
fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)


s.close()

FNULL = open(os.devnull, 'w')


class OriginArchiver():
    def __init__(self, origin_id, mqtt_ip, mqtt_port):
        ''' Init the router '''
        self.origin_id = origin_id
        self.action = "idle"
        self.msg = ""
        self.mqParams = {}
        self.mqParams["url"] = mqtt_ip
        self.mqParams["port"] = int(mqtt_port)
        self.mqParams["timeout"] = 60
        self.mqParams["topic"] = [("origin/ffmpeg/archive/add", 0),
                                  ("origin/ffmpeg/archive/delete", 0)]
        self.mqParams["onMessage"] = self.on_message
        self.client = MQTTPubSub(self.mqParams)

        self.scheduler = BackgroundScheduler()
        self.scheduler.add_jobstore('mongodb', collection='ffmpeg_jobs')
        self.scheduler.start()

    def time_diff(self, time1, time2):
        fmt = '%H:%M:%S'
        print(time1, time2)
        d2 = datetime.strptime(time2, fmt)
        d1 = datetime.strptime(time1, fmt)
        diff = d2-d1
        return int(diff.total_seconds())

    def ffmpeg_archiver(self, msg, length):
        oc.OriginFfmpegArchive.delay(msg, length)

    def router(self):
        while(True):
            if self.action == "origin/ffmpeg/archive/add":
                if (self.msg["start_date"] is None and
                        self.msg["end_date"] is not None):
                    print("Everyday")
                    starthour = self.msg["start_time"].split(":")[0]
                    startminute = self.msg["start_time"].split(":")[1]
                    startsecond = self.msg["start_time"].split(":")[2]
                    length = self.time_diff(str(self.msg["start_time"]),
                                            str(self.msg["end_time"]))
                    try:
                        self.scheduler.add_job(self.ffmpeg_archiver,
                                               'cron', year="*",
                                               month="*", day="*",
                                               hour=starthour,
                                               minute=startminute,
                                               second=startsecond,
                                               id=str(self.msg["user_ip"]) +
                                               "_" +
                                               str(self.msg["stream_id"]) +
                                               "_" +
                                               str(self.msg["job_id"]),
                                               args=[self.msg, length])
                    except Exception as e:
                        print(e)
                        print("Repeated Job_ID, won't be added....")

                elif self.msg["start_date"] is None:
                    print("End date given")
                    length = self.time_diff(
                        str(self.msg["start_time"]), str(self.msg["end_time"]))
                    print(length)
                    starthour = self.msg["start_time"].split(":")[0]
                    startminute = self.msg["start_time"].split(":")[1]
                    startsecond = self.msg["start_time"].split(":")[2]
                    try:
                        self.scheduler.add_job(self.ffmpeg_archiver, 'cron',
                                               year="*",
                                               month="*",
                                               day="*", hour=starthour,
                                               minute=startminute,
                                               second=startsecond,
                                               end_date=self.msg["end_date"],
                                               id=str(self.msg["user_ip"]) +
                                               "_" +
                                               str(self.msg["stream_id"]) +
                                               "_" + str(self.msg["job_id"]),
                                               args=[self.msg, length])
                    except Exception as e:
                        print(e)
                        print("Repeated Job_ID, won't be added....")
                elif self.msg["end_date"] is None:
                    print("Start Date given")
                    length = self.time_diff(
                        str(self.msg["start_time"]), str(self.msg["end_time"]))
                    print(length)
                    starthour = self.msg["start_time"].split(":")[0]
                    startminute = self.msg["start_time"].split(":")[1]
                    startsecond = self.msg["start_time"].split(":")[2]
                    try:
                        self.scheduler.add_job(self.ffmpeg_archiver, 'cron',
                                               year="*",
                                               month="*", day="*",
                                               hour=starthour,
                                               minute=startminute,
                                               second=startsecond,
                                               start_date=self.msg["start_date"],
                                               id=str(self.msg["user_ip"]) +
                                               "_" + str(self.msg["stream_id"]) +
                                               "_" + str(self.msg["job_id"]),
                                               args=[self.msg, length])
                    except Exception as e:
                        print(e)
                        print("Repeated Job_ID, won't be added....")

            if self.action == "origin/ffmpeg/archive/delete":
                self.scheduler.remove_job(
                    str(self.msg["user_ip"]) + "_" +
                    str(self.msg["stream_id"]) + "_" +
                    str(self.msg["job_id"]))
                print("Job Deleted")

            else:
                self.action = "idle"
                self.msg = ""
                continue
            self.action = "idle"
            self.msg = ""
            time.sleep(0.001)

    def on_message(self, client, userdata, message):
        ''' MQTT Callback function '''
        self.msg = message.payload.decode("utf-8")
        self.msg = json.loads(self.msg)
        if self.msg["origin_id"] == self.origin_id:
            self.action = message.topic.decode("utf-8")

    def monitorTaskResult(self, res):
        ''' Celery task monitor '''
        while(True):
            if res.ready():
                ret = res.get()
                if not ret:
                    pass
                elif isinstance(ret, dict):
                    self.client.publish(ret["topic"],
                                        json.dumps(ret["msg"]))
                    time.sleep(0.1)
                elif isinstance(ret, list):
                    for retDict in ret:
                        self.client.publish(retDict["topic"], retDict["msg"])
                        time.sleep(30)
                break


def main():
    mqtt_ip = os.environ["LB_IP"]
    mqtt_port = os.environ["MQTT_PORT"]
    if mqtt_ip is None or mqtt_port is None:
        print("Error! LB_IP and LB_PORT not set")
        sys.exit(0)
    origin_id = os.environ["ORIGIN_ID"]
    originKiller = OriginArchiver(origin_id, mqtt_ip, mqtt_port)
    originKiller.router()


if __name__ == "__main__":
    main()

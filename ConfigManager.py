import math
import subprocess
import urllib2
import requests
from threading import Thread
from Queue import Queue
import time

from flask import Flask
from flask import jsonify
from flask import request

app = Flask(__name__)

SHOULD_METRICS_COLLECTED = Queue()
MEM_USAGE = Queue()
thread_metrics = None


def get_mem_usage():
    while 1:
        if SHOULD_METRICS_COLLECTED.get() == 1:
            wanted_metrics = ["node_memory_MemTotal_bytes", "node_memory_MemFree_bytes"]
            value = get_metrics("http://10.148.0.15:9100/metrics", wanted_metrics)
            value = math.ceil(
                (float(value["node_memory_MemTotal_bytes"]) - float(value["node_memory_MemFree_bytes"])) / float(
                    value["node_memory_MemTotal_bytes"]) * 100)

            if value > MEM_USAGE.get():
                MEM_USAGE.empty()
                MEM_USAGE.put(value)
                MEM_USAGE.task_done()
            time.sleep(5)


def write_template(template):
    with open("output.yaml", "w") as fi:
        for i in range(len(template)):
            fi.write(template[i] + "\n")
        fi.close()


def write_statistic(epoch, accuracy, time, step_time, num_of_ps, num_of_worker, start_time, end_time, memusage):
    if epoch == "1":
        fi = open("stats.txt", "w")
    else:
        fi = open("stats.txt", "a")

    original_string = "Epoch #" + epoch + " = " + "accuracy: " + accuracy + ", time(s): " + time + ", num_ps: " + num_of_ps + ", num_worker: " + num_of_worker + ", avg_mem_usage: " + memusage
    string = original_string + ", start_time: " + start_time + ", end_time: " + end_time + "\n"
    fi.write(string)
    fi.close()

    return original_string


def get_metrics(url, wanted_metrics=None):
    content = urllib2.urlopen(url).read()
    content = content.split("\n")[:-1]
    metrics = {}
    if wanted_metrics is None:
        for each in content:
            if "#" in each:
                continue
            metric = each.split(" ")
            metrics[metric[0]] = metric[1]
    else:
        for each in content:
            if "#" in each:
                continue
            metric = each.split(" ")
            if metric[0] in wanted_metrics:
                metrics[metric[0]] = metric[1]
    return metrics


def get_worker_ps_replica(num_ps, num_worker, threshold, ratio, minimum):
    if MEM_USAGE.get() >= threshold:
        return num_ps, num_worker

    additional_ps = 0
    additional_worker = 0
    while additional_ps + additional_worker < minimum:
        if ratio > 1:
            additional_worker = additional_worker + 1
            additional_ps = int(additional_worker / ratio)
        else:
            additional_ps = additional_ps + 1
            additional_worker = int(additional_ps / ratio)

    return num_ps + additional_ps, num_worker + additional_worker, MEM_USAGE.get()


@app.route("/", methods=["GET", "POST"])
def root():
    return jsonify("The ConfigurationListener is working.")


@app.route("/notify", methods=["POST"])
def notify_upon_start():
    MEM_USAGE.empty()
    MEM_USAGE.put(0)
    MEM_USAGE.task_done()

    SHOULD_METRICS_COLLECTED.empty()
    SHOULD_METRICS_COLLECTED.put(1)
    SHOULD_METRICS_COLLECTED.task_done()

    global thread_metrics
    if thread_metrics is None:
        thread_metrics = Thread(target=get_mem_usage)
        thread_metrics.start()
    return jsonify("Notification accepted.")


@app.route("/modify", methods=["POST"])
def modify():
    SHOULD_METRICS_COLLECTED.empty()
    SHOULD_METRICS_COLLECTED.put(0)
    SHOULD_METRICS_COLLECTED.task_done()

    tfjob_meta_name = request.form["tfjob_meta_name"]
    tfjob_current_epoch = request.form["tfjob_current_epoch"]
    tfjob_current_epoch_accuracy = request.form["tfjob_current_epoch_accuracy"]
    tfjob_current_epoch_time = request.form["tfjob_current_epoch_time"]
    tfjob_current_epoch_step_time = request.form["tfjob_current_epoch_step_time"]
    tfjob_start_time = request.form["tfjob_start_time"]
    tfjob_end_time = request.form["tfjob_end_time"]
    assert tfjob_meta_name is not None
    assert tfjob_current_epoch is not None
    assert tfjob_current_epoch_accuracy is not None
    assert tfjob_current_epoch_time is not None

    ex = subprocess.Popen(
        ["kubectl", "get", "tfjob", tfjob_meta_name, "-o", "yaml", "--export"],
        stdout=subprocess.PIPE
    )
    template = ex.stdout.read().split("\n")

    tfjob_current_epoch = int(tfjob_current_epoch)
    tfjob_total_epoch = int(template[25].split(":")[1].split("\"")[1])
    tfjob_worker_replica = int(template[48].split(" ")[-1])
    tfjob_ps_replica = int(template[15].split(" ")[-1])

    num_ps, num_worker, mem_usage = get_worker_ps_replica(
        num_ps=tfjob_ps_replica,
        num_worker=tfjob_worker_replica,
        threshold=60,
        ratio=1,
        minimum=1
    )

    stats = write_statistic(
        epoch=str(tfjob_current_epoch),
        accuracy=str(tfjob_current_epoch_accuracy),
        time=str(tfjob_current_epoch_time),
        step_time=str(tfjob_current_epoch_step_time),
        num_of_ps=str(tfjob_ps_replica),
        num_of_worker=str(tfjob_worker_replica),
        start_time=str(tfjob_start_time),
        end_time=str(tfjob_end_time),
        memusage=str(mem_usage)
    )
    requests.post("https://api.telegram.org/bot844758581:AAFnTEBzBZcCGOTpLwuysk7tvTkEwGmBpoY/sendMessage", data={
        "chat_id": "418704212",
        "text": stats
    })

    if (tfjob_current_epoch + 1) > tfjob_total_epoch:
        subprocess.call(["kubectl", "delete", "tfjob", tfjob_meta_name])

        message = "Final epoch (#" + str(tfjob_total_epoch) + ") has reached. Training is done."
        requests.post("https://api.telegram.org/bot844758581:AAFnTEBzBZcCGOTpLwuysk7tvTkEwGmBpoY/sendMessage", data={
            "chat_id": "418704212",
            "text": message
        })
        print message
        return jsonify(message)
    else:

        tfjob_meta_name_split = tfjob_meta_name.split("epoch")
        tfjob_new_meta_name = tfjob_meta_name_split[0] + "epoch" + str(tfjob_current_epoch + 1)
        c = ConfigManager(tfjob_new_meta_name, template)

        c.set_worker_replica(str(num_worker))
        c.set_ps_replica(str(num_ps))
        c.set_current_epoch(str(tfjob_current_epoch + 1))

        write_template(c.template)

        subprocess.call(["kubectl", "delete", "tfjob", tfjob_meta_name])
        subprocess.call(["kubectl", "apply", "-f", "output.yaml"])

        message = "Generating configuration for epoch #" + str(tfjob_current_epoch + 1) + " with " + str(
            num_ps) + " PS and " + str(num_worker) + " WORKERS"
        requests.post("https://api.telegram.org/bot844758581:AAFnTEBzBZcCGOTpLwuysk7tvTkEwGmBpoY/sendMessage", data={
            "chat_id": "418704212",
            "text": message
        })

        message = "Configuration generated and applied for epoch #" + str(tfjob_current_epoch + 1)
        print message
        return jsonify(message)


class ConfigManager:
    def __init__(self, meta_name, template):
        self.meta_name = meta_name
        self.template = template
        self.edit_template_value(7, self.meta_name)

    def edit_template_value(self, index, value):
        strings = self.template[index].split(":")
        self.template[index] = strings[0] + ": " + value + "\n"

    def set_ps_replica(self, number):
        self.edit_template_value(15, number)

    def set_worker_replica(self, number):
        self.edit_template_value(48, number)

    def set_current_epoch(self, epoch):
        epoch = "\"" + epoch + "\""
        self.edit_template_value(27, epoch)
        self.edit_template_value(60, epoch)

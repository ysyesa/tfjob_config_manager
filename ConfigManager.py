import math
import subprocess
import urllib2

from flask import Flask
from flask import jsonify
from flask import request

app = Flask(__name__)


def write_template(template):
    with open("output.yaml", "w") as fi:
        for i in range(len(template)):
            fi.write(template[i] + "\n")
        fi.close()


def write_statistic(epoch, accuracy, time, step_time, num_of_ps, num_of_worker, start_time, end_time):
    if epoch == "1":
        fi = open("stats.txt", "w")
    else:
        fi = open("stats.txt", "a")

    string = "Epoch #" + epoch + " = " + "accuracy: " + accuracy + ", time(s): " + time + ", step_time: " + step_time + ", num_ps: " + num_of_ps + ", num_worker: " + num_of_worker + ", start_time: " + start_time + ", end_time: " + end_time + "\n"
    fi.write(string)
    fi.close()


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
    wanted_metrics = ["node_memory_MemTotal_bytes", "node_memory_MemFree_bytes"]
    metrics1 = get_metrics("http://10.148.0.14:9100/metrics", wanted_metrics)
    metrics2 = get_metrics("http://10.148.0.15:9100/metrics", wanted_metrics)

    memusage1 = math.ceil(
        (float(metrics1["node_memory_MemTotal_bytes"]) - float(metrics1["node_memory_MemFree_bytes"])) / float(
            metrics1["node_memory_MemTotal_bytes"]) * 100)
    memusage2 = math.ceil(
        (float(metrics2["node_memory_MemTotal_bytes"]) - float(metrics2["node_memory_MemFree_bytes"])) / float(
            metrics2["node_memory_MemTotal_bytes"]) * 100)

    if memusage1 >= threshold and memusage2 >= threshold:
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

    return num_ps + additional_ps, num_worker + additional_worker


@app.route("/", methods=["GET", "POST"])
def root():
    return jsonify("The ConfigurationListener is working.")


@app.route("/modify", methods=["POST"])
def modify():
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

    tfjob_total_epoch = int(template[25].split(":")[1].split("\"")[1])
    tfjob_worker_replica = int(template[48].split(" ")[-1])
    tfjob_ps_replica = int(template[15].split(" ")[-1])

    tfjob_current_epoch = int(tfjob_current_epoch)
    if (tfjob_current_epoch + 1) > tfjob_total_epoch:
        message = "Final epoch (#" + str(tfjob_total_epoch) + ") has reached. Training is done."
        print message
        return jsonify(message)
    else:

        tfjob_meta_name_split = tfjob_meta_name.split("epoch")
        tfjob_new_meta_name = tfjob_meta_name_split[0] + "epoch" + str(tfjob_current_epoch + 1)
        c = ConfigManager(tfjob_new_meta_name, template)

        num_ps, num_worker = get_worker_ps_replica(
            num_ps=tfjob_ps_replica,
            num_worker=tfjob_worker_replica,
            threshold=60,
            ratio=1,
            minimum=1
        )

        write_statistic(
            epoch=str(tfjob_current_epoch),
            accuracy=str(tfjob_current_epoch_accuracy),
            time=str(tfjob_current_epoch_time),
            step_time=str(tfjob_current_epoch_step_time),
            num_of_ps=str(tfjob_ps_replica),
            num_of_worker=str(tfjob_worker_replica),
            start_time=str(tfjob_start_time),
            end_time=str(tfjob_end_time)
        )

        c.set_worker_replica(str(num_worker))
        c.set_ps_replica(str(num_ps))
        c.set_current_epoch(str(tfjob_current_epoch + 1))

        write_template(c.template)

        subprocess.call(["kubectl", "delete", "tfjob", tfjob_meta_name])
        subprocess.call(["kubectl", "apply", "-f", "output.yaml"])

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

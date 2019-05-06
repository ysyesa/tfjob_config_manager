from flask import Flask
from flask import request
from flask import jsonify
import subprocess
import random
import urllib2
import math

app = Flask(__name__)


def write_template(template):
    with open("output.yaml", "w") as fi:
        for i in range(len(template)):
            fi.write(template[i] + "\n")
        fi.close()


def write_accuracy(epoch, accuracy):
    with open("accuracy.yaml", "a") as fi:
        string = "Epoch #" + epoch + " accuracy: " + accuracy + "\n"
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


def get_master_worker_ps_replica(master, worker, ps):
    THRESHOLD = 70

    wanted_metrics = ["node_memory_MemTotal_bytes", "node_memory_MemFree_bytes"]
    metrics1 = get_metrics("http://10.148.0.14:9100/metrics", wanted_metrics)
    metrics2 = get_metrics("http://10.148.0.15:9100/metrics", wanted_metrics)

    memusage1 = math.ceil((float(metrics1["node_memory_MemTotal_bytes"]) - float(metrics1["node_memory_MemFree_bytes"])) / float(metrics1["node_memory_MemTotal_bytes"]) * 100)
    memusage2 = math.ceil((float(metrics2["node_memory_MemTotal_bytes"]) - float(metrics2["node_memory_MemFree_bytes"])) / float(metrics2["node_memory_MemTotal_bytes"]) * 100)

    if memusage1 < THRESHOLD and memusage2 < THRESHOLD:
        return master, worker + 1, ps + 1
    else:
        return master, worker, ps


@app.route("/", methods=["GET", "POST"])
def root():
    return jsonify("The ConfigurationListener is working.")


@app.route("/modify", methods=["POST"])
def modify():
    tfjob_meta_name = request.form["tfjob_meta_name"]
    tfjob_current_epoch = int(request.form["tfjob_current_epoch"])
    tfjob_current_epoch_accuracy = request.form["tfjob_current_epoch_accuracy"]
    assert tfjob_meta_name is not None
    assert tfjob_current_epoch is not None

    write_accuracy(str(tfjob_current_epoch), tfjob_current_epoch_accuracy)

    ex = subprocess.Popen(
        ["kubectl", "get", "tfjob", tfjob_meta_name, "-o", "yaml", "--export"],
        stdout=subprocess.PIPE
    )
    template = ex.stdout.read().split("\n")

    tfjob_total_epoch = int(template[25].split(":")[1].split("\"")[1])
    tfjob_master_replica = int(template[15].split(" ")[-1])
    tfjob_worker_replica = int(template[85].split(" ")[-1])
    tfjob_ps_replica = int(template[50].split(" ")[-1])

    if (tfjob_current_epoch + 1) > tfjob_total_epoch:
        message = "Final epoch (#" + str(tfjob_total_epoch) + ") has reached. Training is done."
        print message
        return jsonify(message)
    else:

        c = ConfigManager(tfjob_meta_name, template)
        master, worker, ps = get_master_worker_ps_replica(tfjob_master_replica, tfjob_worker_replica, tfjob_ps_replica)

        c.set_master_replica(str(master))
        c.set_worker_replica(str(worker))
        c.set_ps_replica(str(ps))
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

    def set_master_replica(self, number):
        self.edit_template_value(15, number)

    def set_ps_replica(self, number):
        self.edit_template_value(50, number)

    def set_worker_replica(self, number):
        self.edit_template_value(85, number)

    def set_current_epoch(self, epoch):
        epoch = "\"" + epoch + "\""
        self.edit_template_value(27, epoch)
        self.edit_template_value(62, epoch)
        self.edit_template_value(97, epoch)

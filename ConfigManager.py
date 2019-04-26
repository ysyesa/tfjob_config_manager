from flask import Flask
from flask import request
from flask import jsonify
import subprocess
import random

app = Flask(__name__)


def write_template(template):
    with open("output.yaml", "w") as fi:
        for i in range(len(template)):
            fi.write(template[i])
        fi.close()


def get_master_worker_ps_replica():
    index = random.randint(0, 5)
    if index % 3 == 0:
        return 1, 2, 1
    elif index % 3 == 1:
        return 1, 2, 2
    elif index % 3 == 2:
        return 1, 4, 2


@app.route("/", methods=["GET", "POST"])
def root():
    return jsonify("The ConfigurationListener is working.")


@app.route("/modify", methods=["POST"])
def modify():
    tfjob_meta_name = request.form["tfjob_meta_name"]
    tfjob_current_epoch = int(request.form["tfjob_current_epoch"])
    assert tfjob_meta_name is not None
    assert tfjob_current_epoch is not None

    ex = subprocess.Popen(
        ["kubectl", "get", "tfjob", tfjob_meta_name, "-o", "yaml", "--export"],
        stdout=subprocess.PIPE
    )
    template = ex.stdout.read().split("\n")

    tfjob_total_epoch = int(template[25].split(":")[1].split("\"")[1])

    if (tfjob_current_epoch + 1) > tfjob_total_epoch:
        message = "Final epoch (#" + str(tfjob_total_epoch) + ") has reached. Training is done."
        print message
        return jsonify(message)
    else:

        c = ConfigManager(tfjob_meta_name, template)
        master, worker, ps = get_master_worker_ps_replica()

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
        self.edit_template_value(48, number)

    def set_worker_replica(self, number):
        self.edit_template_value(81, number)

    def set_current_epoch(self, epoch):
        epoch = "\"" + epoch + "\""
        self.edit_template_value(27, epoch)

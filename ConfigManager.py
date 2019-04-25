from flask import Flask
from flask import request
from flask import jsonify
import subprocess
import random

app = Flask(__name__)


def load_template():
    template = []
    with open("template.yaml", "r") as fi:
        line = fi.readline()
        while line:
            template.append(line)
            line = fi.readline()
        fi.close()
    return template


def write_template(template):
    with open("output.yaml", "w") as fi:
        for i in range(len(template)):
            fi.write(template[i])
        fi.close()


TEMPLATE = load_template()


@app.route("/", methods=["GET", "POST"])
def root():
    return jsonify("The ConfigurationListener is working.")


@app.route("/modify", methods=["POST"])
def modify():
    tfjob_meta_name = request.form["tfjob_meta_name"]
    tfjob_master_image = request.form["tfjob_master_image"]
    tfjob_worker_image = request.form["tfjob_worker_image"]
    tfjob_ps_image = request.form["tfjob_ps_image"]
    tfjob_path_dataset = request.form["tfjob_path_dataset"]
    tfjob_path_tensorboard = request.form["tfjob_path_tensorboard"]
    tfjob_total_epoch = int(request.form["tfjob_total_epoch"])
    tfjob_next_epoch = int(request.form["tfjob_next_epoch"])

    assert tfjob_meta_name is not None
    assert tfjob_master_image is not None
    assert tfjob_worker_image is not None
    assert tfjob_ps_image is not None
    assert tfjob_path_dataset is not None
    assert tfjob_path_tensorboard is not None
    assert tfjob_total_epoch is not None
    assert tfjob_next_epoch is not None

    if tfjob_next_epoch > tfjob_total_epoch:
        message = "Final epoch (#" + str(tfjob_total_epoch) + ") has reached. Training is done."
        print message
        return jsonify(message)
    else:

        c = ConfigManager(tfjob_meta_name, TEMPLATE)
        master, worker, ps = c.get_master_worker_ps_replica()

        c.set_master_replica(str(master))
        c.set_master_image(tfjob_master_image)
        c.set_worker_replica(str(worker))
        c.set_worker_image(tfjob_worker_image)
        c.set_ps_replica(str(ps))
        c.set_ps_image(tfjob_ps_image)
        c.set_path_dataset(tfjob_path_dataset)
        c.set_path_tensorboard(tfjob_path_tensorboard)

        write_template(c.template)

        subprocess.call(["kubectl", "delete", "tfjob", tfjob_meta_name])
        subprocess.call(["kubectl", "apply", "-f", "output.yaml"])

        message = "Configuration generated and applied for epoch #" + str(tfjob_next_epoch)
        print message
        return jsonify(message)


class ConfigManager:
    def __init__(self, meta_name, template):
        self.meta_name = meta_name
        self.template = template
        self.edit_template_value(3, self.meta_name)

    def edit_template_value(self, index, value):
        strings = self.template[index].split(":")
        self.template[index] = strings[0] + ": " + value

    def set_master_replica(self, number):
        self.edit_template_value(7, number)

    def set_master_image(self, image):
        self.edit_template_value(12, image)

    def set_worker_replica(self, number):
        self.edit_template_value(31, number)

    def set_worker_image(self, image):
        self.edit_template_value(36, image)

    def set_ps_replica(self, number):
        self.edit_template_value(55, number)

    def set_ps_image(self, image):
        self.edit_template_value(60, image)

    def set_path_dataset(self, path):
        self.edit_template_value(26, path)
        self.edit_template_value(50, path)
        self.edit_template_value(74, path)

    def set_path_tensorboard(self, path):
        self.edit_template_value(29, path)
        self.edit_template_value(53, path)
        self.edit_template_value(77, path)

    def get_master_worker_ps_replica(self):
        index = random.randint(0, 5)
        if index % 3 == 0:
            return 1, 2, 1
        elif index % 3 == 1:
            return 1, 2, 2
        elif index % 3 == 2:
            return 1, 4, 2

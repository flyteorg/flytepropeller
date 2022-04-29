#!/usr/bin/env python

# TODO - add tracking of "Enqueueing owner for object"

from abc import ABC, abstractmethod
from datetime import datetime
import json
import random
import re
import sys

header = """Timestamp   Line    Duration    Heirarchical Log Layout
----------------------------------------------------------------------------------------------------"""
printfmt = "%-11s %-7d %-11s %s"

# define propeller log blocks
class Block(ABC):
    def __init__(self, line_number):
        self.children = []
        self.start_time = None
        self.end_time = None
        self.line_number = line_number

    def get_id(self):
        return self.__class__.__name__

    @abstractmethod
    def handle_log(self, log):
        pass

    def parse(self):
        while True:
            log, line_number = parser.next()
            if not log:
                return

            if "msg" not in log:
                continue

            self.handle_log(log, line_number)
            if self.end_time:
                return

class Workflow(Block):
    def __init__(self):
        super().__init__(-1)

    def handle_log(self, log, line_number):
        global enqueue_msgs

        # match {"level":"info", "msg":"==\u003e Enqueueing workflow [NAMESPACE/ID]}"}
        if "Enqueueing workflow" in log["msg"]:
            enqueue_msgs.append((line_number, log["ts"]))
            if not self.start_time:
                self.start_time = log["ts"]
                self.line_number = line_number

        # match {"level":"info", "msg":"Processing Workflow"}
        if "Processing Workflow" in log["msg"]:
            block = Processing(log["ts"], line_number)
            block.parse()
            self.children.append(block)

class IDBlock(Block):
    def __init__(self, id, start_time, end_time, line_number):
        super().__init__(line_number)
        self.id = id
        self.start_time = start_time
        self.end_time = end_time

    def handle_log(self, log):
        pass

    def get_id(self):
        return self.id

class Processing(Block):
    def __init__(self, start_time, line_number):
        super().__init__(line_number)
        self.start_time = start_time
        self.last_recorded_time = start_time

    def handle_log(self, log, line_number):
        global enqueue_msgs

        # match {"level":"info", "msg":"==\u003e Enqueueing workflow [NAMESPACE/ID]}"}
        if "Enqueueing workflow" in log["msg"]:
            enqueue_msgs.append((line_number, log["ts"]))

        # match {"level":"info", "msg":"Completed processing workflow"}
        if "Completed processing workflow" in log["msg"]:
            self.end_time = log["ts"]

        # match {"level":"info", "msg":"Handling Workflow [ID], id: [project:\"PROJECT\" domain:\"DOMAIN\" name:\"ID\" ], p [STATUS]"}
        if "Handling Workflow" in log["msg"]:
            match = re.search(r'p \[([\w]+)\]', log["msg"])
            if match:
                block = StreakRound(f"{match.group(1)}", log["ts"], line_number)
                block.parse()
                self.children.append(block)

class StreakRound(Block):
    def __init__(self, phase, start_time, line_number):
        super().__init__(line_number)
        self.phase = phase
        self.start_time = start_time
        self.last_recorded_time = start_time

    def get_id(self):
        return f"{self.__class__.__name__}({self.phase})"

    def handle_log(self, log, line_number):
        global enqueue_msgs

        # match {"level":"info", "msg":"Catalog CacheEnabled: recording execution [PROJECT/DOMAIN/TASK_ID/TASK_VERSION]"}
        if "Catalog CacheEnabled. recording execution" in log["msg"]:
            id = "CacheWrite(" + log["json"]["node"] + ")"
            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"], line_number))
            self.last_recorded_time = log["ts"]

        # match {"level":"info", "msg":"Catalog CacheHit: for task [PROJECT/DOMAIN/TASK_ID/TASK_VERSION]"}
        if "Catalog CacheHit" in log["msg"]:
            id = "CacheHit(" + log["json"]["node"] + ")"
            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"], line_number))
            self.last_recorded_time = log["ts"]

        # match {"level":"info", "msg":"Catalog CacheMiss: Artifact not found in Catalog. Executing Task."}
        if "Catalog CacheMiss" in log["msg"]:
            id = "CacheMiss(" + log["json"]["node"] + ")"
            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"], line_number))
            self.last_recorded_time = log["ts"]

        # match {"level":"info", "msg":"Change in node state detected from [STATUS] -\u003e [STATUS], (handler phase [STATUS])"}
        # or {"level":"info", "msg":"Change in node state detected from [STATUS] -\u003e [STATUS]"}
        if "Change in node state detected" in log["msg"]:
            id = "UpdateNodePhase(" + log["json"]["node"]

            match = re.search(r'\[([\w]+)\] -> \[([\w]+)\]', log["msg"])
            if match:
                id += f",{match.group(1)},{match.group(2)})"

            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"], line_number))
            self.last_recorded_time = log["ts"]

        # match {"level":"info", "msg":"==\u003e Enqueueing workflow [NAMESPACE/ID]}"}
        if "Enqueueing workflow" in log["msg"]:
            enqueue_msgs.append((line_number, log["ts"]))

        # match {"level":"info", "msg":"Handling Workflow [ID] done"}
        if "Handling Workflow" in log["msg"]:
            self.end_time = log["ts"]

        # match {"level":"debug", "msg":"node succeeding"}
        if "node succeeding" in log["msg"]:
            id = "UpdateNodePhase(" + log["json"]["node"] + ",Succeeding,Succeeded)"
            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"], line_number))
            self.last_recorded_time = log["ts"]

        # match {"level":"debug", "msg":"Sending transition event for plugin phase [STATUS]"}
        if "Sending transition event for plugin phase" in log["msg"]:
            id = "UpdatePluginPhase(" + log["json"]["node"]

            match = re.search(r'\[([\w]+)\]', log["msg"])
            if match:
                id += f",{match.group(1)})"

            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"], line_number))
            self.last_recorded_time = log["ts"]

        # match {"level":"info", "msg":"Transitioning/Recording event for workflow state transition [STATUS] -\u003e [STATUS]"}
        if "Transitioning/Recording event for workflow state transition" in log["msg"]:
            id = "UpdateWorkflowPhase("

            match = re.search(r'\[([\w]+)\] -> \[([\w]+)\]', log["msg"])
            if match:
                id += f"{match.group(1)},{match.group(2)})"

            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"], line_number))
            self.last_recorded_time = log["ts"]

# define JsonLogParser class
class JsonLogParser:
    def __init__(self, file, workflow_id):
        self.file = file
        self.workflow_id = workflow_id
        self.line_number = 0

    def next(self):
        while True:
            # read next line
            line = self.file.readline()
            if not line:
                return None, -1
            self.line_number += 1

            try:
                # only process if {"json":{"exec_id":"WORKFLOW_ID"}} or json.msg contains WORKFLOW_ID
                log = json.loads(line)
                if ("exec_id" in log["json"] and log["json"]["exec_id"] == self.workflow_id) \
                        or ("msg" in log and self.workflow_id in log["msg"]):
                    return log, self.line_number
            except:
                # TODO - stderr?
                pass

def print_block(block, prefix):
    # print all enqueue messages from prior to this line number
    while len(enqueue_msgs) > 0 and enqueue_msgs[0][0] <= block.line_number:
        enqueue_time = datetime.strptime(enqueue_msgs[0][1], '%Y-%m-%dT%H:%M:%S%z').strftime("%H:%M:%S")

        print(printfmt %(enqueue_time, enqueue_msgs[0][0], "-", "Enqueue Workflow"))
        enqueue_msgs.pop(0)

    # compute block elapsed time
    elapsed_time = 0
    if block.end_time and block.start_time:
        elapsed_time = datetime.strptime(block.end_time, '%Y-%m-%dT%H:%M:%S%z').timestamp() \
            - datetime.strptime(block.start_time, '%Y-%m-%dT%H:%M:%S%z').timestamp()

    # compute formatted id
    start_time = datetime.strptime(block.start_time, '%Y-%m-%dT%H:%M:%S%z').strftime("%H:%M:%S")
    id = prefix + " " + block.get_id()

    print(printfmt %(start_time, block.line_number, str(elapsed_time) + "s", id))

    # process all children
    count = 1
    for child in block.children:
        print_block(child, f"    {prefix}.{count}")
        count += 1

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("args no good")
        sys.exit(1)

    workflow = Workflow()
    with open(sys.argv[1], "r") as file:
        global parser
        parser = JsonLogParser(file, sys.argv[2])

        global enqueue_msgs
        enqueue_msgs = []

        workflow.parse()

    workflow.end_time = workflow.children[len(workflow.children) - 1].end_time

    print(header)
    print_block(workflow, "1")

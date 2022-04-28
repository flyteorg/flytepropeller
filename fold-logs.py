#!/usr/bin/env python

from abc import ABC, abstractmethod
from datetime import datetime
import json
import random
import re
import sys

# define propeller log blocks
class Block(ABC):
    def __init__(self):
        self.children = []
        self.start_time = None
        self.end_time = None

    def get_id(self):
        return self.__class__.__name__

    @abstractmethod
    def handle_log(self, log):
        pass

    def parse(self):
        while True:
            log = parser.next()
            if not log:
                return

            self.handle_log(log)
            if self.end_time:
                return

class Workflow(Block):
    def __init__(self):
        super().__init__()
        self.last_recorded_time = None

    def handle_log(self, log):
        global queued

        # match {msg:"==\u003e Enqueueing workflow [flytesnacks-development/self.id]"}
        if "msg" in log and "Enqueueing workflow" in log["msg"]:
            if not self.start_time:
                self.start_time = log["ts"]

            if not queued:
                # add idle block
                if self.last_recorded_time:
                    self.children.append(IDBlock("Idle", self.last_recorded_time, log["ts"]))

                self.last_recorded_time = log["ts"]
                queued = True

        # match {json: {exec_id: self.id}, msg:"Processing Workflow"}
        if "msg" in log and "Processing Workflow" in log["msg"]:
            # add queued block - TODO figure out the correct way to handle this
            #if queued:
                #self.children.append(IDBlock("Queued", self.last_recorded_time, log["ts"]))

            queued = False

            block = Processing(log["ts"])
            block.parse()
            self.children.append(block)

            # set idle time
            self.last_recorded_time = block.end_time

class IDBlock(Block):
    def __init__(self, id, start_time, end_time):
        super().__init__()
        self.id = id
        self.start_time = start_time
        self.end_time = end_time

    def handle_log(self, log):
        pass

    def get_id(self):
        return self.id

class Processing(Block):
    def __init__(self, start_time):
        super().__init__()
        self.start_time = start_time
        self.last_recorded_time = start_time

    def handle_log(self, log):
        global queued

        # match {json: {exec_id: self.id}, msg:"Completed processing workflow"}
        if "msg" in log and "Completed processing workflow" in log["msg"]:
            self.end_time = log["ts"]

        # match {json: {exec_id: self.id}, msg:"Handling Workflow ..."}
        if "msg" in log and "Handling Workflow" in log["msg"]:
            match = re.search(r'p \[([\w]+)\]', log["msg"])
            if match:
                block = StreakRound(f"{match.group(1)}", log["ts"])
                block.parse()
                self.children.append(block)

        # match {json: {exec_id: self.id}, msg:"Enqueueing workflow ..."}
        if "msg" in log and "Enqueueing workflow" in log["msg"]:
            queued = True

class StreakRound(Block):
    def __init__(self, phase, start_time):
        super().__init__()
        self.phase = phase
        self.start_time = start_time
        self.last_recorded_time = start_time

    def get_id(self):
        return f"{self.__class__.__name__}({self.phase})"

    def handle_log(self, log):
        global queued

        # match {json: {exec_id: self.id}, msg:"Handling Workflow ..."}
        if "msg" in log and "Handling Workflow" in log["msg"]:
            self.end_time = log["ts"]

        # match {json: {exec_id: self.id}, msg:"TODO ..."}
        if "msg" in log and "Change in node state detected" in log["msg"]:
            id = "UpdateNodePhase(" + log["json"]["node"]

            match = re.search(r'\[([\w]+)\] -> \[([\w]+)\]', log["msg"])
            if match:
                id += f",{match.group(1)},{match.group(2)})"

            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"]))
            self.last_recorded_time = log["ts"]

        # match {json: {exec_id: self.id}, msg:"TODO ..."}
        if "msg" in log and "Sending transition event for plugin phase" in log["msg"]:
            id = "UpdatePluginPhase(" + log["json"]["node"]

            match = re.search(r'\[([\w]+)\]', log["msg"])
            if match:
                id += f",{match.group(1)})"

            self.children.append(IDBlock(id, self.last_recorded_time, log["ts"]))
            self.last_recorded_time = log["ts"]

        # match {json: {exec_id: self.id}, msg:"Enqueueing workflow ..."}
        if "msg" in log and "Enqueueing workflow" in log["msg"]:
            queued = True

# define JsonLogParser class
class JsonLogParser:
    def __init__(self, file, id):
        self.file = file
        self.id = id

    def next(self):
        while True:
            line = self.file.readline()
            if not line:
                return None

            try:
                log = json.loads(line)
                if ("exec_id" in log["json"] and log["json"]["exec_id"] == self.id) \
                        or ("msg" in log and self.id in log["msg"]):
                    return log
            except:
                # TODO - stderr?
                pass

def print_block_flame(block, prefix):
    id = ""
    if prefix:
        id = prefix + block.get_id()

        elapsed_time = 0
        if block.end_time and block.start_time:
            elapsed_time = datetime.strptime(block.end_time, '%Y-%m-%dT%H:%M:%S%z').timestamp() \
                - datetime.strptime(block.start_time, '%Y-%m-%dT%H:%M:%S%z').timestamp()
            #elapsed_time = datetime.strptime(block.end_time, '%Y-%m-%dT%H:%M:%S.%f%z').timestamp() \
            #    - datetime.strptime(block.start_time, '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()

        print(f"{id} {elapsed_time}")

    if id:
        id += ";"

    count = 0
    for child in block.children:
        print_block_flame(child, f"{id}{count:04}-")
        count += 1

def print_block(block, indent):
    start_time = datetime.strptime(block.end_time, '%Y-%m-%dT%H:%M:%S%z').strftime("%H:%M:%S")
    id = start_time + indent + block.get_id()

    elapsed_time = 0
    if block.end_time and block.start_time:
        elapsed_time = datetime.strptime(block.end_time, '%Y-%m-%dT%H:%M:%S%z').timestamp() \
            - datetime.strptime(block.start_time, '%Y-%m-%dT%H:%M:%S%z').timestamp()

    print(f"{id} {elapsed_time}s")

    count = 0
    for child in block.children:
        print_block(child, f"{indent}    ")
        count += 1

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("args no good")
        sys.exit(1)

    workflow = Workflow()
    with open(sys.argv[1], "r") as file:
        global parser
        parser = JsonLogParser(file, sys.argv[2])

        global queued
        queued = False

        workflow.parse()

    workflow.end_time = workflow.children[len(workflow.children) - 1].end_time
    print_block(workflow, "    ")
    #print_block_flame(workflow, None)

    # TODO - fix workflow start_time
    # TODO - add info on when workflow is queued
    # TODO - add info on transitioning node (ex. n0) to success

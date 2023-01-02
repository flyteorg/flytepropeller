#!/usr/bin/env python

import click
import grpc
import json
import logging
import numpy as np
import requests
import yaml

from datetime import timezone
from dateutil.parser import parse

from flytekit.remote import FlyteRemote
from flytekit.configuration import Config

from query_pb2 import FindTracesRequest, TraceQueryParameters 
from query_pb2_grpc import QueryServiceStub

from yamlable import YamlObject2

@click.group
@click.option('--domain', default="development", help='workflow execution domain')
@click.option('--project', default="flytesnacks", help='workflow execution project')
@click.pass_context
def cli(ctx, project, domain):
    ctx.ensure_object(dict)

    ctx.obj['DOMAIN'] = domain
    ctx.obj['PROJECT'] = project

@cli.group
@click.pass_context
def runtime(ctx):
    pass

@runtime.command
@click.argument('execution_id')
@click.pass_context
def dump(ctx, execution_id):
    domain = ctx.obj['DOMAIN']
    project = ctx.obj['PROJECT']

    # initialize FlyteRemote with config, project, domain
    remote = FlyteRemote(
        config=Config.auto(),
        default_project=project,
        default_domain=domain,
    )

    execution = remote.fetch_execution(name=execution_id)
    if not execution.is_done:
        print('TODO - execution not yet complete')
        return

    _, workflow_info = parse_workflow(remote, execution)

    yaml.emitter.Emitter.process_tag = lambda self, *args, **kw: None
    print(yaml.dump(workflow_info, indent=2))

@runtime.command
@click.argument('execution_id')
@click.argument('node_id')
@click.pass_context
def explain(ctx, execution_id, node_id):
    # TODO @hamersaw - move this duplication up another layer
    domain = ctx.obj['DOMAIN']
    project = ctx.obj['PROJECT']

    # initialize FlyteRemote with config, project, domain
    remote = FlyteRemote(
        config=Config.auto(),
        default_project=project,
        default_domain=domain,
    )

    execution = remote.fetch_execution(name=execution_id)
    if not execution.is_done:
        print('TODO - execution not yet complete')
        return

    _, workflow_info = parse_workflow(remote, execution)
    node = find_node(workflow_info, node_id)
    if node == None:
        print('TODO - node not found')
        return

    print('{:21s} {:24s} {:24s} {:>9s}    {:s}'.format('category', 'start_timestamp', 'end_timestamp', 'duration', 'description'))
    print('-'*140)

    nodes = node['breakdown'].reports
    for report in sorted(nodes, key = lambda ele: ele[1]):
        print('{:21s} {:24s} {:24s} {:8.2f}s    {:s}'.format(
            report[0],
            report[1].strftime("%m-%d %H:%M:%S.%f"),
            report[2].strftime("%m-%d %H:%M:%S.%f"),
            (report[2] - report[1]).total_seconds(),
            report[3])
        )

def find_node(node, node_id):
    nodes = node.get('nodes')
    if nodes:
        for underlying_node in nodes:
            if underlying_node['node_id'] == node_id:
                return underlying_node

            n = find_node(underlying_node, node_id) 
            if n != None:
                return n

    # TODO @hamersaw - search subworkflows

    return None

EXECUTION_OVERHEAD = 'execution_overhead'
K8S_ORCHESTRATION = 'k8s_orchestration'
K8S_RUNTIME = 'k8s_runtime'
NODE_TRANSITION = 'node_transition'

class Breakdown(YamlObject2):
    yaml_tag = 'foo'

    def __init__(self):
        self.reports = []

    def add(self, breakdown):
        for report in breakdown.reports:
            self.reports.append(report)

    def report(self, category, started_at, ended_at, description):
        if (ended_at - started_at).total_seconds() != 0:
            self.reports.append((category, started_at, ended_at, description))

    def __to_yaml_dict__(self):
        total_runtime = 0
        aggregations = {}
        for report in self.reports:
            category = report[0]
            duration = (report[2] - report[1]).total_seconds()

            total_runtime += duration
            if aggregations.get(category) == None:
                aggregations[category] = duration
            else:
                aggregations[category] += duration

        result = {}
        for category, duration in aggregations.items():
            result[category] = '{:.2f}s ({:.2f}%)'.format(duration, (duration / total_runtime) * 100)

        return result

def parse_launchplan_node(remote, node):
    breakdown = Breakdown()
    workflow_infos = []
    for underlying_execution in node.workflow_executions:
        breakdown.report(EXECUTION_OVERHEAD, node.closure.created_at, underlying_execution.closure.created_at, 'TODO')
        breakdown.report(EXECUTION_OVERHEAD, underlying_execution.closure.updated_at, node.closure.updated_at, 'TODO')

        workflow_breakdown, workflow_info = parse_workflow(remote, underlying_execution)

        breakdown.add(workflow_breakdown)
        workflow_infos.append(workflow_info)

    return breakdown, {
        'node_id': node.id.node_id,
        'workflows': workflow_infos,
    }

def parse_nodes(remote, nodes, upstream_node_ids):
    breakdowns = []
    return_node_infos = []
    for node_id, node in nodes.items():
        is_parent_node = node.metadata.is_parent_node
        closure = node.closure

        if node_id == 'start-node' or node_id == 'end-node':
            # TODO @hamersaw - need to explicitely add last dependency -> end-node.updated_at
            continue

        if not is_parent_node and len(node.task_executions) > 0:
            # task node
            breakdown, node_info = parse_task_node(node)

        elif is_parent_node and len(node.task_executions) > 0:
            # dynamic node
            print("TODO @hamersaw - parse dynamic")
            breakdown = Breakdown()
            node_info = {}

        elif not is_parent_node and closure.workflow_node_metadata:
            # launchplan node
            breakdown, node_info = parse_launchplan_node(remote, node)

        elif is_parent_node and node._underlying_node_executions != None:
            # subworkflow node
            breakdown, node_info = parse_subworkflow_node(remote, node)

        else:
            print("UNKNOWN NODE TYPE") # TODO @hamersaw - implement gate node and branch node
            breakdown = Breakdown()
            node_info = {}

        # compute transition time from previous node
        latest_upstream_node = None
        for upstream_node_id in upstream_node_ids[node_id].ids:
            upstream_node = nodes[upstream_node_id]
            if latest_upstream_node == None or upstream_node.closure.updated_at > latest_upstream_node.closure.updated_at:
                latest_upstream_node = upstream_node

        breakdown.report(NODE_TRANSITION, latest_upstream_node.closure.updated_at, node.closure.created_at,
            f'transition from node "{latest_upstream_node.id.node_id}" to node "{node.id.node_id}"')

        # finalize node_info
        node_info['breakdown'] = breakdown

        breakdowns.append(breakdown)
        return_node_infos.append(node_info)

    return breakdowns, return_node_infos

def parse_subworkflow_node(remote, node):
    subworkflow_ref = node._node.workflow_node.sub_workflow_ref
    subworkflow = remote.fetch_workflow(name=subworkflow_ref.name, version=subworkflow_ref.version)

    nodes = {ele.metadata.spec_node_id : ele for ele in node.subworkflow_node_executions.values()}

    breakdown = Breakdown()

    start_node = None
    end_node = None
    for node_id, underlying_node in nodes.items():
        if node_id == 'start-node':
            start_node = underlying_node
        if node_id == 'end-node':
            end_node = underlying_node

    breakdown.report(EXECUTION_OVERHEAD, node.closure.created_at, start_node.closure.updated_at, 'TODO')
    breakdown.report(EXECUTION_OVERHEAD, end_node.closure.updated_at, node.closure.updated_at, 'TODO')

    breakdowns, node_infos = parse_nodes(remote, nodes, subworkflow._compiled_closure.primary.connections.upstream)
    for node_breakdown in breakdowns:
        breakdown.add(node_breakdown)

    return breakdown, {
        'node_id': node.id.node_id,
        'nodes': node_infos,
    }

def parse_task_node(node):
    tasks = sorted(node.task_executions, key = lambda ele: ele.closure.created_at)

    breakdown = Breakdown()

    first_task = tasks[0]
    breakdown.report(EXECUTION_OVERHEAD, node.closure.created_at, first_task.closure.created_at.replace(tzinfo=timezone.utc), f'setup node execution for "{node.id.node_id}"')

    last_task = tasks[len(tasks)-1]
    breakdown.report(EXECUTION_OVERHEAD, last_task.closure.updated_at.replace(tzinfo=timezone.utc), node.closure.updated_at, f'finalizing node execution for "{node.id.node_id}"')

    task_breakdowns, task_infos = parse_tasks(tasks)
    for task_breakdown in task_breakdowns:
        breakdown.add(task_breakdown)

    for i, task in enumerate(tasks):
        # if this is not the first task we capture flyte overhead between previous attempt completion
        start_time = task.closure.created_at
        if i != 0:
            prev_task_closure = tasks[i-1].closure
            start_time = prev_task_closure.started_at + prev_task_closure.duration

        # if this is the last task we capture overhead between task completion and final update
        end_time = task.closure.started_at + task.closure.duration
        if i == len(node.task_executions)-1:
           end_time = task.closure.updated_at

        breakdown.report(EXECUTION_OVERHEAD, start_time.replace(tzinfo=timezone.utc),
            task.closure.created_at.replace(tzinfo=timezone.utc), f'overhead between task attempts {i-1} and {i}')
        breakdown.report(EXECUTION_OVERHEAD, (task.closure.started_at + task.closure.duration).replace(tzinfo=timezone.utc),
            end_time.replace(tzinfo=timezone.utc), 'completed processing tasks')

    return breakdown, {
        'node_id': node.id.node_id,
        'tasks': task_infos,
    }

def parse_tasks(tasks):
    breakdowns=[]
    task_infos = []

    for i, task in enumerate(tasks):
        # TODO @hamersaw - fix bug where task has not yet started (task.closure.started_at = 1970-01-01)

        breakdown = Breakdown()
        breakdown.report(K8S_ORCHESTRATION, task.closure.created_at.replace(tzinfo=timezone.utc),
            task.closure.started_at.replace(tzinfo=timezone.utc), f'setup for task attempt {task.id.retry_attempt}')
        breakdown.report(K8S_RUNTIME, task.closure.started_at.replace(tzinfo=timezone.utc),
            (task.closure.started_at + task.closure.duration).replace(tzinfo=timezone.utc), f'execution of task attempt {task.id.retry_attempt}')

        task_info = {
            'attempt': task.id.retry_attempt,
            'breakdown': breakdown,
        }

        breakdowns.append(breakdown)
        task_infos.append(task_info)

    return breakdowns, task_infos

def closure_to_timestamps(closure):
    return {
        'created_at': closure.created_at,
        'duration': closure.duration.total_seconds(),
        'started_at': closure.started_at,
        'updated_at': closure.updated_at,
    }

def parse_workflow(remote, execution):
    # TODO - how can we sync node executions from `fetch_execution`
    execution = remote.sync_execution(execution=execution, sync_nodes=True)
    nodes = execution.node_executions

    # compute overhead
    breakdown = Breakdown()
    breakdown.report(EXECUTION_OVERHEAD, execution.closure.created_at, nodes['start-node'].closure.updated_at, 'TODO')
    breakdown.report(EXECUTION_OVERHEAD, nodes['end-node'].closure.updated_at,execution.closure.updated_at, 'TODO')

    # parse nodes
    node_breakdowns, node_infos = parse_nodes(remote, nodes, execution.flyte_workflow._compiled_closure.primary.connections.upstream)
    for node_breakdown in node_breakdowns:
        breakdown.add(node_breakdown)

    return breakdown, {
        'breakdown': breakdown,
        'nodes': node_infos,
    }

@cli.group
@click.pass_context
def orchestration(ctx):
    pass

@orchestration.command
@click.argument('execution_id')
@click.pass_context
def dump(ctx, execution_id):
    #
    # TODO use jaeger grpc API to retrieve traces
    #

    channel = grpc.insecure_channel('localhost:16685')
    stub = QueryServiceStub(channel)

    response = stub.FindTraces(
        FindTracesRequest(
            query = TraceQueryParameters(
                service_name='flytepropeller',
                tags={'exec_id':execution_id},
            )
        )
    )

    # TODO - document
    operation_durations = {} # service:operation -> total_duration
    root_spans=[]            # [ids]
    spans={}                 # id -> span
    span_children={}         # id -> service:operation -> [ids]
    for spans_chunk in response:
        for span in spans_chunk.spans:
            span_type = span.process.service_name + ":" + span.operation_name
            if operation_durations.get(span_type) == None:
                operation_durations[span_type] = 0

            operation_durations[span_type] += span.duration.nanos / 1000000.0

            trace_id = ''.join(map(chr, span.trace_id))
            span_id = ''.join(map(chr, span.span_id))
            id = trace_id + span_id

            spans[id]=span
            if len(span.references) > 0:
                for reference in span.references:
                    parent_trace_id = ''.join(map(chr, reference.trace_id))
                    parent_span_id = ''.join(map(chr, reference.span_id))
                    parent_id = parent_trace_id + parent_span_id

                    if span_children.get(parent_id) == None:
                        span_children[parent_id] = {}

                    if span_children[parent_id].get(span_type) == None:
                        span_children[parent_id][span_type] = []

                    span_children[parent_id][span_type].append(id)
            else:
                root_spans.append(id)

    def compute_span_info(ids, root_duration, parent_duration, indent):
        indent_str = ''
        for i in range(indent):
            indent_str += '  '

        service_name = None
        operation_name = None
        durations = []
        type_children={}
        for id in ids:
            span = spans[id]

            if service_name == None:
                service_name = span.process.service_name
            elif service_name != span.process.service_name:
                print('ERROR')

            if operation_name == None:
                operation_name = span.operation_name
            elif operation_name != span.operation_name:
                print('ERROR')

            durations.append(span.duration.nanos / 1000000.0)

            if span_children.get(id) != None:
                for span_type, children in span_children[id].items():
                    if type_children.get(span_type) == None:
                        type_children[span_type] = []

                    type_children[span_type].extend(children)

        total_duration = np.sum(durations)
        span_type = service_name + ':' + operation_name
        operation_duration = operation_durations[span_type]

        span_info = {
            'count': len(ids),
            'duration': {
                'percentile': {
                    '50': '{:.2f}ms'.format(np.percentile(durations, 50)),
                    '90': '{:.2f}ms'.format(np.percentile(durations, 90)),
                    '99': '{:.2f}ms'.format(np.percentile(durations, 99)),
                },
                'relational_percentage': {
                    'operation': '{:.2f}%'.format(total_duration / operation_duration * 100),
                    'parent': '{:.2f}%'.format(total_duration / parent_duration * 100),
                    'root': '{:.2f}%'.format(total_duration / root_duration * 100),
                },
                'total': '{0:.2f}ms'.format(total_duration),
            },
            'service': service_name,
            'operation': operation_name,
        }

        children_info = []
        for children in type_children.values():
            child_info = compute_span_info(children, root_duration, total_duration, indent+1)
            children_info.append(child_info)

        if len(span_children) > 0:
            span_info['children'] = children_info

        return span_info

    # TODO - group root spans by service:operation
    root_duration = 0
    for id in root_spans:
        root_duration += spans[id].duration.nanos / 1000000.0

    span_info = compute_span_info(root_spans, root_duration, root_duration, 0)
    print(yaml.safe_dump(span_info))

if __name__ == '__main__':
    cli(obj={})

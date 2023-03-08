#!/usr/bin/env python

import click
import grpc
import json
import numpy as np
import yaml

from query_pb2 import FindTracesRequest, TraceQueryParameters 
from query_pb2_grpc import QueryServiceStub

from yamlable import YamlObject2

@click.command
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
    dump()

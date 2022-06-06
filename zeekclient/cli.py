"""This module provides command line parsers and corresponding commands."""
import argparse
import configparser
import ipaddress
import json
import os
import sys
import traceback

import broker

from .config import CONFIG

from .consts import (
    CONFIG_FILE,
    CONTROLLER
)

from .events import (
    GetConfigurationRequest,
    GetConfigurationResponse,
    GetIdValueRequest,
    GetIdValueResponse,
    GetInstancesRequest,
    GetInstancesResponse,
    GetNodesRequest,
    GetNodesResponse,
    SetConfigurationRequest,
    SetConfigurationResponse,
    TestTimeoutRequest,
    TestTimeoutResponse
)

from .logs import LOG

from .types import (
    BrokerEnumType,
    ClusterRole,
    Configuration,
    Instance,
    ManagementRole,
    NodeStatus,
    NodeOutputs,
    Result
)

from .utils import make_uuid


# Broker's basic types aren't JSON-serializable, so patch that up
# in this json.dumps() wrapper for JSON serialization of any object.
# Could go into utils.py, but it easier here to keep free of cyclic
# dependencies.
def json_dumps(obj):
    def default(obj):
        if isinstance(obj, ipaddress.IPv4Address):
            return str(obj)
        if isinstance(obj, ipaddress.IPv6Address):
            return str(obj)
        if isinstance(obj, broker.Port):
            return str(obj)
        if isinstance(obj, BrokerEnumType):
            return obj.to_json_data()
        raise TypeError('cannot serialize {} ({})'.format(type(obj), str(obj)))

    indent = 2 if CONFIG.getboolean('client', 'pretty_json') else None
    return json.dumps(obj, default=default, sort_keys=True, indent=indent)


def create_parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='A Zeek management client',
        epilog='environment variables:\n\n'
        '    ZEEK_CLIENT_CONFIG_FILE:      '
        'Same as `--configfile` argument, but lower precedence.\n'
        '    ZEEK_CLIENT_CONFIG_SETTINGS:  '
        'Same as a space-separated series of `--set` arguments, but lower precedence.\n')

    parser.add_argument('-c', '--configfile', metavar='FILE', default=CONFIG_FILE,
                        help='Path to zeek-client config file. (Default: {})'.format(CONFIG_FILE))
    parser.add_argument('--controller', metavar='HOST:PORT', default=CONTROLLER,
                        help='Address and port of the controller, either of '
                        'which may be omitted (default: {})'.format(CONTROLLER))
    arg = parser.add_argument('--set', metavar='SECTION.KEY=VAL', action='append', default=[],
                              help='Adjust a configuration setting. Can use repeatedly. See show-settings.')

    # This is for argcomplete users and has no effect otherwise.
    arg.completer = CONFIG.completer

    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument('--quiet', '-q', action='store_true',
                                 help='Suppress informational output to stderr.')
    verbosity_group.add_argument('--verbose', '-v', action='count', default=0,
                                 help='Increase informational output to stderr. '
                                 'Repeat for more output (e.g. -vvv).')

    parser.add_argument('--version', action='store_true',
                        help='Show version number and exit.')

    command_parser = parser.add_subparsers(
        title='commands', dest='command',
        help='See `%(prog)s <command> -h` for per-command usage info.')

    sub_parser = command_parser.add_parser(
        'get-config', help='Retrieve deployed cluster configuration.')
    sub_parser.set_defaults(run_cmd=cmd_get_config)
    sub_parser.add_argument('--filename', '-f', metavar='FILE', default='-',
                            help='Output file for the configuration, default stdout')
    sub_parser.add_argument('--as-json', action='store_true',
                            help='Report in JSON instead of INI-style config file')

    sub_parser = command_parser.add_parser(
        'get-id-value', help='Show the value of a given identifier in Zeek cluster nodes.')
    sub_parser.set_defaults(run_cmd=cmd_get_id_value)
    sub_parser.add_argument('id', metavar='IDENTIFIER',
                            help='Name of the Zeek script identifier to retrieve.')
    sub_parser.add_argument('nodes', metavar='NODES', nargs='*', default=[],
                            help='Name(s) of select Zeek cluster nodes to query. '
                            'When omitted, queries all nodes.')

    sub_parser = command_parser.add_parser(
        'get-instances', help='Show instances connected to the controller.')
    sub_parser.set_defaults(run_cmd=cmd_get_instances)

    sub_parser = command_parser.add_parser(
        'get-nodes', help='Show active Zeek nodes at each instance.')
    sub_parser.set_defaults(run_cmd=cmd_get_nodes)

    sub_parser = command_parser.add_parser(
        'monitor', help='For troubleshooting: do nothing, just report events.')
    sub_parser.set_defaults(run_cmd=cmd_monitor)

    sub_parser = command_parser.add_parser(
        'set-config', help='Deploy cluster configuration.')
    sub_parser.set_defaults(run_cmd=cmd_set_config)
    sub_parser.add_argument('config', metavar='FILE',
                            help='Cluster configuration file, "-" for stdin')

    sub_parser = command_parser.add_parser(
        'show-settings', help="Show zeek-client's own configuration.")
    sub_parser.set_defaults(run_cmd=cmd_show_settings)

    sub_parser = command_parser.add_parser(
        'test-timeout', help='Send timeout test event.')
    sub_parser.set_defaults(run_cmd=cmd_test_timeout)
    sub_parser.add_argument('--with-state', action='store_true',
                            help='Make request stateful in the controller.')

    return parser


def cmd_get_config(controller, args):
    controller.publish(GetConfigurationRequest(make_uuid()))
    resp, msg = controller.receive()

    if resp is None:
        LOG.error('no response received: %s', msg)
        return 1

    if not isinstance(resp, GetConfigurationResponse):
        LOG.error('received unexpected event: %s', resp)
        return 1

    res = Result.from_broker(resp.result)

    if not res.success:
        msg = res.error if res.error else 'no reason given'
        LOG.error('response indicates failure: %s', msg)
        return 1

    if not res.data:
        LOG.error('received result did not contain configuration data: %s', resp)
        return 1

    config = Configuration.from_broker(res.data)

    with open(args.filename, 'w') if args.filename and args.filename != '-'  else sys.stdout as hdl:
        if args.as_json:
            hdl.write(json_dumps(config.to_json_data()) + '\n')
        else:
            cfp = config.to_config_parser()
            cfp.write(hdl)

    return 0


def cmd_get_id_value(controller, args):
    controller.publish(GetIdValueRequest(make_uuid(), args.id, set(args.nodes)))
    resp, msg = controller.receive()

    if resp is None:
        LOG.error('no response received: %s', msg)
        return 1

    if not isinstance(resp, GetIdValueResponse):
        LOG.error('received unexpected event: %s', resp)
        return 1

    json_data = {
        'results': {},
        'errors': [],
    }

    # The Result records have both instance and node filled in, so use both for
    # ordering. While for the JSON serialization we can outsource the ordering
    # task to Python, for our error reporting it's up to us, and we want be
    # reproducible.

    results = [Result.from_broker(broker_data) for broker_data in resp.results]

    for res in sorted(results):
        if not res.success:
            json_data['errors'].append({
                'source': res.node,
                'error': res.error,
            })
            continue

        # Upon success, we should always have res.node filled in. But guard anyway.
        if res.node:
            # res.data is a string containing JSON rendered by Zeek's to_json()
            # BiF. Parse it into a data structure to render seamlessly.
            try:
                json_data['results'][res.node] = json.loads(res.data)
            except json.JSONDecodeError as err:
                json_data['errors'].append({
                    'source': res.node,
                    'error': 'JSON decode error: {}'.format(err),
                })
            continue

        json_data['errors'].append({
            'error': 'result lacking node: {}'.format(res.data),
        })

    print(json_dumps(json_data))
    return 0 if len(json_data['errors']) == 0 else 1


def cmd_get_instances(controller, args): # pylint: disable=unused-argument
    controller.publish(GetInstancesRequest(make_uuid()))
    resp, msg = controller.receive()

    if resp is None:
        LOG.error('no response received: %s', msg)
        return 1

    if not isinstance(resp, GetInstancesResponse):
        LOG.error('received unexpected event: %s', resp)
        return 1

    res = Result.from_broker(resp.result)

    if not res.success:
        msg = res.error if res.error else 'no reason given'
        LOG.error('response indicates failure: %s', msg)
        return 1

    if res.data is None:
        LOG.error('received result did not contain instance data: %s', resp)
        return 1

    json_data = {}

    # res.data is a (possibly empty) vector of Instances. Make the list of
    # instances easier to comprehend than raw Broker data: turn it into Instance
    # objects, then render these JSON-friendly.
    try:
        for inst in sorted([Instance.from_broker(inst) for inst in res.data]):
            json_data[inst.name] = inst.to_json_data()
            json_data[inst.name].pop('name')
    except TypeError as err:
        LOG.error('instance data invalid: %s', err)

    print(json_dumps(json_data))
    return 0


def cmd_get_nodes(controller, _args):
    controller.publish(GetNodesRequest(make_uuid()))
    resp, msg = controller.receive()

    if resp is None:
        LOG.error('no response received: %s', msg)
        return 1

    if not isinstance(resp, GetNodesResponse):
        LOG.error('received unexpected event: %s', resp)
        return 1

    json_data = {
        'results': {},
        'errors': [],
    }

    results = [Result.from_broker(broker_data) for broker_data in resp.results]

    for res in sorted(results):
        if not res.success:
            json_data['errors'].append({
                'source': res.instance,
                'error': res.error,
            })
            continue

        if res.data is None:
            json_data['errors'].append({
                'source': res.instance,
                'error': 'result does not contain node status data',
            })
            continue

        json_data['results'][res.instance] = {}

        # res.data is a NodeStatusVec
        try:
            nstats = [NodeStatus.from_broker(nstat_data) for nstat_data in res.data]
            for nstat in sorted(nstats):
                # If either of the two role enums are "NONE", we make them
                # None. That way they stay in the reporting, but are more easily
                # distinguished from "actual" values.
                mgmt_role = nstat.mgmt_role if nstat.mgmt_role != ManagementRole.NONE else None
                cluster_role = nstat.cluster_role if nstat.cluster_role != ClusterRole.NONE else None

                json_data['results'][res.instance][nstat.node] = {
                    'state': nstat.state,
                    'mgmt_role': mgmt_role,
                    'cluster_role': cluster_role,
                }

                if nstat.pid is not None:
                    json_data['results'][res.instance][nstat.node]['pid'] = nstat.pid
                if nstat.port is not None:
                    json_data['results'][res.instance][nstat.node]['port'] = nstat.port
        except TypeError as err:
            LOG.error('NodeStatus data invalid: %s', err)
            LOG.debug(traceback.format_exc())

    print(json_dumps(json_data))
    return 0 if len(json_data['errors']) == 0 else 1


def cmd_monitor(controller, args): # pylint: disable=unused-argument
    while True:
        resp, msg = controller.receive(None)

        if resp is None:
            print('no response received: {}'.format(msg))
        else:
            print('received "{}"'.format(resp))

    return 0


def cmd_set_config(controller, args):
    if not args.config or (args.config != '-' and not os.path.isfile(args.config)):
        LOG.error('please provide a cluster configuration file.')
        return 1

    # We use a config parser to parse the cluster configuration. For instances,
    # we allow names without value to designate agents that connect to the
    # controller, like this:
    #
    # [instances]
    # foobar
    #
    # All other keys must have a value.
    config = Configuration()
    cfp = configparser.ConfigParser(allow_no_value=True)

    if args.config == '-':
        cfp.read_file(sys.stdin)
    else:
        cfp.read(args.config)

    config = Configuration.from_config_parser(cfp)

    if config is None:
        LOG.error('configuration has errors, not deploying')
        return 1

    controller.publish(SetConfigurationRequest(make_uuid(), config.to_broker()))
    resp, msg = controller.receive()

    if resp is None:
        LOG.error('no response received: %s', msg)
        return 1

    if not isinstance(resp, SetConfigurationResponse):
        LOG.error('received unexpected event: %s', resp)
        return 1

    json_data = {}
    retval = 0

    for broker_data in resp.results:
        res = Result.from_broker(broker_data)

        if not res.success:
            retval = 1

        # If the result record doesn't mention a node, it's the response of an
        # agent that had no nodes to launch (but possibly dropped existing ones
        # as part of this request). We adopt its error state, but don't render
        # any output.
        if res.node is None:
            continue

        json_data[res.node] = {
            'success': res.success,
            'instance': res.instance,
        }

        # If launching this node failed, we should have a NodeOutputs record as
        # data member in the result record.
        if res.data:
            node_outputs = NodeOutputs.from_broker(res.data)
            json_data[res.node]['stdout'] = node_outputs.stdout
            json_data[res.node]['stderr'] = node_outputs.stderr


    print(json_dumps(json_data))
    return retval


def cmd_show_settings(_controller, _args):
    CONFIG.write(sys.stdout)
    return 0


def cmd_test_timeout(controller, args):
    controller.publish(TestTimeoutRequest(make_uuid(), args.with_state))
    resp, msg = controller.receive()

    if resp is None:
        LOG.error('no response received: %s', msg)
        return 1

    if not isinstance(resp, TestTimeoutResponse):
        LOG.error('received unexpected event: %s', resp)
        return 1

    res = Result.from_broker(resp.result)
    print(json_dumps({'success': res.success, 'error': res.error}))
    return 0

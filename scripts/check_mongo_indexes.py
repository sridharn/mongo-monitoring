#!/usr/bin/python

import argparse
import logging
import simplejson as json
# import sys

from pymongo import MongoClient, ReadPreference

from sample_index_results import REFERENCE, RESULTS

logging.basicConfig(
    level='INFO',
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
log = logging.getLogger('check_mongo')


def main(mongo_uri, output_file, simulate, output_json):
    cleaned = mongo_uri.strip()
    out_html_file_name = output_file+'.html'
    with open(out_html_file_name, 'w') as out_html_file:
        if not simulate:
            results = {
                'servers': {},
                'errors': {}
            }
            reference = {}
            log.info('Processing URI {0}'.format(cleaned))
            conn = MongoClient(
                cleaned,
                read_preference=ReadPreference.SECONDARY_PREFERRED)
            log.debug('Obtained connection to {0}'.format(cleaned))
            if conn.is_mongos:
                process_mongods(results, conn, cleaned, reference)
            else:
                replica_set_members = get_replica_set_members(cleaned)
                process_replica_set_members(
                    replica_set_members,
                    results, reference)
        else:
            results = RESULTS
            reference = REFERENCE
        if output_json:
            out_raw_file_name = output_file+'.raw'
            with open(out_raw_file_name, 'w') as out_raw_file:
                write_raw_output(out_raw_file, results, reference)
        write_html_output(out_html_file, cleaned, results, reference)


def process_mongods(results, conn, mongos_uri, reference):
    log.info('In processing mongods for mongos {0}'.format(mongos_uri))
    shards = conn['config']['shards'].find({}, {'_id': 0, 'host': 1})
    for replica_set in shards:
        candidates = replica_set['host'].split('/')[-1]
        success = False
        replica_set_members = None
        for mongod in candidates.split(','):
            try:
                replica_set_members = get_replica_set_members(mongod)
                success = True
                break
            except:
                log.exception(
                    'Error getting replica set info for {0}'.format(mongod))
        if success:
            process_replica_set_members(replica_set_members, results, reference)
        else:
            results['errors'][mongos_uri] = 'Cannot get replica set info'


def process_indexes(results, conn, server_uri, reference):
    log.info('In processing indexes for mongod {0}'.format(server_uri))
    collection_indexes = []
    for database in conn.database_names():
        if database in [u'local', u'admin', u'config', u'test']:
            log.debug(
                'Skipping processing index for system database {0}'.format(
                    database))
            continue
        indexes = conn[database]['system.indexes'].find().sort(
            [('ns', 1), ('key', 1)])
        index_dict = {}
        ns = None
        for index in indexes:
            if not ns:
                ns = index['ns']
                log.debug('NS is none. Set to {0}'.format(ns))
            if ns != index['ns']:
                log.debug('{0} != {1}'.format(ns, index['ns']))
                collection_indexes.append({ns: index_dict})
                ns = index['ns']
                index_dict = {}
            index_dict[index['name']] = index['key']
            if ns not in reference:
                ns_stats = {'index_count': 0, 'indexes': {}}
                reference[ns] = ns_stats
            else:
                ns_stats = reference[ns]
            if index['name'] not in ns_stats['indexes']:
                ns_stats['indexes'][index['name']] = index['key']
                ns_stats['index_count'] += 1

        collection_indexes.append({ns: index_dict})
    results['servers'][server_uri] = collection_indexes


def get_replica_set_members(replica_set_member):
    log.info('Get replica set info for member {0}'.format(replica_set_member))
    conn = MongoClient(
        replica_set_member,
        read_preference=ReadPreference.SECONDARY_PREFERRED)
    rsconfig = conn['local']['system.replset'].find_one()
    if rsconfig:
        return [member['host'] for member in rsconfig['members']]
    else:
        return [replica_set_member]


def process_replica_set_members(members, results, reference):
    for member in members:
        try:
            member_conn = MongoClient(
                member,
                # connectTimeoutMS=5000,
                read_preference=ReadPreference.SECONDARY_PREFERRED)
            is_master = member_conn['admin'].command('isMaster')
            if not (is_master['ismaster'] or is_master['secondary']):
                log.warning('{0} is neither primary or secondary', member)
                results['errors'][member] = 'Neither primary nor secondary'
                return
            log.debug('Obtained connection to mongod {0}'.format(member))
            process_indexes(results, member_conn, member, reference)
        except Exception, e:
            log.exception(e)
            results['errors'][member] = 'Cannot connect'


def write_raw_output(out_file, results, reference):
    out_file.write('REFERENCE\n')
    out_file.write('---------\n')
    json.dump(reference, out_file, indent=4 * ' ')
    out_file.write('\nSERVER\n')
    out_file.write('------\n')
    json.dump(results, out_file, indent=4 * ' ')


def write_html_output(out_file, header, results, reference):
    out_file.write('<html>\n')
    out_file.write('\t<head>\n')
    out_file.write('\t\t<title>Indexes for %s</title>\n' % header)
    out_file.write("""
            <style type="text/css">
            .error {
                background-color: white;
                color: red;
            }
        </style>""")
    out_file.write('\t</head>\n')
    out_file.write('\t<body>\n')
    write_output_body(out_file, header, results, reference)
    out_file.write('\t</body>\n')
    out_file.write('</html>\n')
    out_file.flush()


def write_output_body(out_file, header, results, reference):
    out_file.write('\t\t<h1>{0}</h1>\n'.format(header))
    write_indexes(out_file, reference)
    write_server_status(out_file, results['servers'], reference)
    write_errors(out_file, results['errors'])


def write_errors(out_file, errors):
    out_file.write('\t\t<h2>Other errors</h2>\n')
    if len(errors) > 0:
        out_file.write('\t\t<table border="1" class="error">\n')
        out_file.write(
            '\t\t\t<tr><th>Server</th><th>Error</th></tr>\n')
        for server in sorted(errors.iterkeys()):
            out_file.write(
                '\t\t\t<tr><td>{0}</td><td>{0}</td></tr>\n'.format(
                server,
                errors[server]))
        out_file.write('\t\t</table>\n')
    else:
        out_file.write('\t\t<p>No other errors on this run.</p>')


def write_indexes(out_file, reference):
    out_file.write('\t\t<h2>Indexes</h2>\n')
    for namespace in sorted(reference.iterkeys()):
        body = reference[namespace]
        log.debug('Namespace=%s, Body=%s', namespace, body)
        out_file.write(
            '\t\t<h3>%s - %d indexes</h3>\n' %
            (namespace, body['index_count']))
        out_file.write('\t\t<table border="1">\n')
        out_file.write('\t\t\t<tr><th>Index Name</th><th>Index Key</th></tr>\n')
        for index_name in sorted(body['indexes'].iterkeys()):
            index_key = body['indexes'][index_name]
            log.debug('Name=%s, Key=%s', index_name, index_key)
            out_file.write(
                '\t\t\t<tr><td>{0}</td><td>{1}</td></tr>\n'.format(
                index_name,
                index_key))
        out_file.write('\t\t</table>\n')


def write_server_status(out_file, servers, reference):
    out_file.write('\t\t<h2>Servers</h2>\n')
    out_file.write('\t\t<table border="1">\n')
    out_file.write(
        '\t\t\t<tr><th>Server</th><th>Status</th><th>Namespace</th><th>Source</th><th>Index Name</th><th>Source</th><th>Target</th></tr>\n')
    for server_name in sorted(servers.keys()):
        namespaces = servers[server_name]
        valid = True
        for collection in namespaces:
            namespace_name = collection.keys()[0]
            if namespace_name == "null":
                log.warning(
                    'Encountered namespace name null for %s',
                    server_name)
                continue
            if not namespace_name:
                log.warning(
                    'Encountered namespace name None for %s',
                    server_name)
                continue
            namespace_indexes = collection.values()[0]
            log.debug('namespace_name = %s', namespace_name)
            log.debug('namespace_indexes = %s', namespace_indexes)
            reference_indexes = reference[namespace_name]['indexes']
            log.debug('reference_indexes = %s', reference_indexes)

            # Check server indexes against master's
            for index_name, index_key in namespace_indexes.items():
                reference_index_key = reference_indexes.get(index_name)
                if index_key != reference_index_key:
                    valid = False
                    out_file.write(
                        '\t\t\t<tr class="error"><td>%s</td><td>INVALID</td><td>%s</td><td>SERVER</td><td>%s</td><td>%s</td><td>%s</td></tr>\n' % (
                            server_name,
                            namespace_name,
                            index_name,
                            index_key,
                            reference_index_key))

            # Check master indexes against server's
            for index_name, index_key in reference_indexes.items():
                server_index_key = namespace_indexes.get(index_name)
                if index_key != server_index_key:
                    valid = False
                    out_file.write(
                        '\t\t\t<tr class="error"><td>%s</td><td>INVALID</td><td>%s</td><td>MASTER</td><td>%s</td><td>%s</td><td>%s</td></tr>\n' % (
                            server_name,
                            namespace_name,
                            index_name,
                            index_key,
                            server_index_key))

        if valid:
            out_file.write(
                '\t\t\t<tr><td>%s</td><td colspan="6">VALID</td></tr>\n' %
                server_name)
    out_file.write('\t\t</table>\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mongos_uri', help='URL of mongos')
    parser.add_argument('output_file', help='File contain output result')
    parser.add_argument('--simulate', action='store_true')
    parser.add_argument('--output_json', action='store_true')
    args = parser.parse_args()
    main(args.mongos_uri, args.output_file, args.simulate, args.output_json)

#!/usr/bin/python

import argparse
from distutils.version import StrictVersion
import json
import logging
from pymongo import MongoClient
import re

from mongo_setup import MONITORING_DB, MONITORING_HOSTS, CONNECTION_TIMEOUT_MS


logging.basicConfig(level='INFO', format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
log = logging.getLogger('check_mongo_config')


def main(mongo_uri, minimum_version, output_file):
    conn = MongoClient(mongo_uri, connectTimeoutMS=CONNECTION_TIMEOUT_MS)
    final_results = { 
        'minimum_version' : minimum_version,
        'results' : {}
    }
    for seed_host in conn[MONITORING_DB][MONITORING_HOSTS].find().sort([('live', 1), 
        ('_id', 1)]):
        results = {
            'mongod' : [],
            'mongos' : [],
            'config' : [],
            'errors' : []
        }
        label = seed_host['_id']
        live = seed_host.get('live', False)
        log.info('Processing {0}'.format(label))
        if not live:
            log.warning('Skipping {0} since it is not live'.format(label))
            continue
        hosts = seed_host.get('hosts')
        skip_mongos = not seed_host.get('process_mongos', True)
        if not hosts:
            log.warning('Skipping {0} since no hosts specified'.format(label))
            continue
        if isinstance(hosts, basestring):
            process(hosts, minimum_version, results, skip_mongos)
        elif isinstance(hosts, list):
            for host in hosts:
                process(host, minimum_version, results, skip_mongos)
        else:
            log.warning('Skipping {0} as hosts incorrectly specified'.format(label))
        final_results['results'][label] = results
    with open(output_file, 'w') as fp:
        json.dump(final_results, fp)


def process(server_uri, minimum_version, results, skip_mongos=False, process_subs=True, process_override=None):
    log.debug('Processing {0} with subs {1}'.format(server_uri, process_subs))
    try:
        conn = MongoClient(server_uri, connectTimeoutMS=CONNECTION_TIMEOUT_MS, slaveOk=True)
        log.debug('Obtained connection to {0}'.format(server_uri))
        server_status = conn['admin'].command({'serverStatus' : 1, 'recordStats' : 0})
        process = server_status['process']
        version = server_status['version']
        if not process_subs or process == 'mongod':
            if process_override:
                add_server_info(results, server_uri, process_override, version,
                    minimum_version)
            else:
                add_server_info(results, server_uri, process, version,
                    minimum_version)
        else:
            process_sharded_cluster(results, conn, minimum_version, skip_mongos)
    except:
        results['errors'].append({ 'server' : server_uri})


def get_valid_version(version, minimum_version):
    return StrictVersion(version) >= StrictVersion(minimum_version)


def process_sharded_cluster(results, conn, minimum_version, skip_mongos):
    log.info('In processing sharded cluster for {0}'.format(conn))
    process_configs(results, conn, minimum_version)
    if not skip_mongos:
        process_mongos(results, conn, minimum_version)
    else:
        log.info('Skipping processing mongos')
    process_mongods(results, conn, minimum_version)
    log.info('Done processing sharded cluster')


def process_mongos(results, conn, minimum_version):
    log.info('In processing mongos for {0}'.format(conn))
    mongos_list = conn['config']['mongos'].find()
    for mongos in mongos_list:
        process(mongos['_id'], minimum_version, results, process_subs=False)
    log.info('Done processing mongos')


def process_mongods(results, conn, minimum_version):
    log.info('In processing mongods for {0}'.format(conn))
    shards = conn['config']['shards'].find({},{'_id':0, 'host':1})
    for replica_set in shards:
        candidates = replica_set['host'].split('/')[-1]
        success = False
        replica_set_members = None
        for mongod in candidates.split(','):
            try :
                replica_set_members = get_replica_set_members(mongod)
                success = True
                break
            except:
                log.exception('Error getting replica set info for {0}'.format(mongod))
        if success:
            for member in replica_set_members:
                process(member, minimum_version, results)
        else:
            results['errors'].append({ 'server' : server_uri})
    log.info('Done processing mongods')


def process_configs(results, conn, minimum_version):
    log.info('In processing config for {0}'.format(conn))
    cmd_line_opts = conn['admin'].command('getCmdLineOpts')
    config_servers = cmd_line_opts['parsed']['configdb']
    for config_server in config_servers.split(','):
        if ":" not in config_server:
            config_server = config_server + ':27019'
        process(config_server, minimum_version, results, process_override='config')
    log.info('Done processing configs')


def add_server_info(results, server, process, version, minimum_version):
    if 'mongos' in process:
        process = 'mongos'
    results[process].append({
        'server' : server, 
        'version' : version,
        'valid' : get_valid_version(version, minimum_version)
    })


def get_replica_set_members(member):
    log.info('Get replica set info for {0}'.format(member))
    conn = MongoClient(member)
    rsconfig = conn['local']['system.replset'].find_one()
    if rsconfig:
        return [member['host'] for member in rsconfig['members']]
    else:
        return [member]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mongo_uri', help='Mongo(d/s) URI')
    parser.add_argument('--minimum_version',
        help = 'Minimum 3 digit version to check', default='2.0.0')
    parser.add_argument('--output_file',
        help = 'Output file', default='mongo_check.json')
    args = parser.parse_args()
    main(args.mongo_uri, args.minimum_version, args.output_file)
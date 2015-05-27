#!/usr/bin/python

import argparse
import csv
from distutils.version import StrictVersion
import json
import logging
# import re

from pymongo import MongoClient

from mongo_setup import MONITORING_DB, MONITORING_HOSTS, CONNECTION_TIMEOUT_MS
EXCLUDED_DATABASES = { 'admin', 'config', 'test'}


logging.basicConfig(level='INFO', format='%(asctime)s %(levelname)s [%(name)s] %(message)s')
log = logging.getLogger('check_mongo_config')


def main(mongo_uri, output_file):
    conn = MongoClient(mongo_uri, connectTimeoutMS=CONNECTION_TIMEOUT_MS)
    final_results = {}
    for seed_host in conn[MONITORING_DB][MONITORING_HOSTS].find().sort([('live', 1), 
        ('_id', 1)]):
        label = seed_host['_id']
        live = seed_host.get('live', False)
        log.info('Processing server {0}'.format(label))
        if not live:
            log.warning('Skipping {0} since it is not live'.format(label))
            continue
        hosts = seed_host.get('hosts')
        skip_mongos = not seed_host.get('process_mongos', True)
        if not hosts:
            log.warning('Skipping {0} since no hosts specified'.format(label))
            continue
        if isinstance(hosts, basestring):
            final_results[label] = process(hosts)
    json_file = output_file + '.json'
    with open(json_file, 'w') as fp:
        json.dump(final_results, fp)
    output_excel(final_results, output_file+'.xls')


def process(server_uri):
    databases = {}
    # try:
    conn = MongoClient(server_uri, connectTimeoutMS=CONNECTION_TIMEOUT_MS, slaveOk=True)
    log.debug('Obtained connection to {0}'.format(server_uri))
    result = conn['admin'].command('listDatabases')
    for database in result['databases']:
        db_name = database.get('name')
        if database.get('empty'):
            log.debug('Skipping {0} for {1}'.format(db_name, server_uri))
        else:
            if db_name not in EXCLUDED_DATABASES:
                databases[db_name] = process_database(conn, database.get('name'))
    return databases
    # except Exception, e:
    #     print e
    #     return {'error' : server_uri}


def process_database(conn, db_name):
    log.info('Processing database {0}'.format(db_name))
    result = conn['config']['databases'].find_one({'_id' : db_name})
    output = {}
    if result.get('partitioned'):
        sharded_db = True
        output['sharded'] = True
    else:
        sharded_db = False
        output['sharded'] = False
    output['collections'] = []
    collections = conn[db_name].collection_names(include_system_collections=False)
    for coll_name in collections:
        coll_output = { 'name' :  coll_name }
        coll_output['indexes'] = []
        log.debug(coll_output)
        if (sharded_db):
            coll_info = conn['config']['collections'].find_one({'_id' : db_name+'.'+coll_name})
            if coll_info and coll_info.get('dropped') == False:
                coll_output['shard_key'] = coll_info.get('key')
            else:
                coll_output['shard_key'] = 'Unsharded'
        indexes = conn[db_name][coll_name].index_information()
        for index in indexes.values():
            log.debug(index)
            coll_output['indexes'].append(index['key'])
        log.debug(coll_output)
        output['collections'].append(coll_output)
    return output


def output_excel(final_results, output_file):
    with open(output_file, 'w') as fp:
        csvwriter = csv.writer(fp, delimiter=',', quotechar='"')
        csvwriter.writerow(['Cluster', 'Database', 'Sharded', 'Collection name', 'Shard key', 'Index key'])
        for cluster in final_results.keys():
            for database in cluster.keys():
                for collection in cluster[database]['collections']:
                    csvwriter.writerow(cluster, database, 
                        cluster[database]['sharded'], collection['name'],
                        collection['shard_key'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mongo_uri', help='Mongo(d/s) URI')
    parser.add_argument('--output_file',
        help = 'Output file', default='mongo_check')
    args = parser.parse_args()
    main(args.mongo_uri, args.output_file)
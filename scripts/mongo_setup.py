#!/usr/bin/python

import argparse

from pymongo import MongoClient

MONITORING_DB = 'mongo_monitoring'
MONITORING_HOSTS = 'monitoring_hosts'
CONNECTION_TIMEOUT_MS = 5000


def install_monitoring_mongo(mongo_uri):
    conn = MongoClient(mongo_uri, connectTimeoutMS=CONNECTION_TIMEOUT_MS)
    db = conn[MONITORING_DB]
    setup_hosts(db)

def setup_hosts(db):
    # lists of hosts to monitor in the form of
    # { 
    #     '_id' : 'name of cluster',  <-- just a label
    #     'hosts' : ['', ''],     <-- mongo uri of cluster. checked in order. e.g.
    #         in case of RS it is 1 or more members of the cluster
    #         in case of a sharded cluster it is one or more mongos
    #     'live' : True/False     <-- is this a cluster to be monitored
    #     'process_mongos' : True/False <-- if connected to a sharded cluster
    #         get the status of each mongos
    # }
    hosts_collection = db[MONITORING_HOSTS]
    hosts_collection.save( {
        '_id' : 'my_localhost_1',
        'hosts' : ['localhost:27018'],
        'live' : False}
    )
    hosts_collection.save( {
        '_id' : 'my_localhost_2',
        'hosts' : ['localhost:27017'],
        'live' : True,
        'process_mongos' : True}
    )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mongo_uri', help='Mongod URI')
    args = parser.parse_args()
    install_monitoring_mongo(args.mongo_uri)

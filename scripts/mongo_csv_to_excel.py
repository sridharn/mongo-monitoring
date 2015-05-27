import argparse
import csv
import json

def main(input_file):
	json_file = input_file + '.json'
	output_file = input_file + '.xls'
	with open(json_file, 'r') as fp:
		catalog = json.load(fp)
		with open(output_file, 'w') as fp:
			csvwriter = csv.writer(fp, delimiter=',', quotechar='"')
			csvwriter.writerow(['Cluster', 'Database', 'Sharded', 'Collection name', 'Shard key', 'Index key'])
			for cluster in catalog.keys():
				for database in catalog[cluster]:
					for collection in catalog[cluster][database]['collections']:
						indexes = collection.get('indexes', {})
						if len(indexes) > 0:
							for index in indexes:
								csvwriter.writerow([cluster, database,
									catalog[cluster][database]['sharded'], collection['name'],
									collection.get('shard_key'), index])

						else:
							csvwriter.writerow([cluster, database,
								catalog[cluster][database]['sharded'], collection['name'],
								collection.get('shard_key')])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file',
        help = 'Input file prefix', default='mongo_check')
    args = parser.parse_args()
    main(args.input_file)
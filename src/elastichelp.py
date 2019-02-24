import os
import elasticsearch
from elasticsearch import Elasticsearch, Urllib3HttpConnection
from elasticsearch.client import Elasticsearch
from elasticsearch.client import ClusterClient

class CustomConnection(Urllib3HttpConnection):
    def __init__(self, *args, **kwargs):
        extra_headers = kwargs.pop('extra_headers', {})
        super(CustomConnection, self).__init__(*args, **kwargs)
        self.headers.update(extra_headers)

# Creates an Elasticsearch client via options stored in the provided config file
def createElasticsearchClient(conf):
  auth = None

    return  Elasticsearch(
            ["{}:{}".format(os.environ.get('ELASTICSEARCH_HOST'), os.environ.get('ELASTICSEARCH_PORT'))],
        connection_class=CustomConnection,
        extra_headers={'x-api-key':"{}".format(os.environ.get('ELASTICSEARCH_API_KEY'))}
    )
# Queries health of the Elasticsearch cluster
def health(conf, args):
  if args == "help" or args == "-help":
    return """Queries the health of the Elasticsearch cluster.```
Usage: ${prefix}health
```"""

  client = createElasticsearchClient(conf)
  result = client.cluster.health()
  
  response = """Elasticsearch Cluster Health -> %s
```
                    Name: %s
                   Nodes: %d
              Data Nodes: %d
           Active Shards: %d (%d%%)
     Initializing Shards: %d
       Unassigned Shards: %d
           Pending Tasks: %d
          Inflight Tasks: %d
       Max Queue Time MS: %d
```""" % (result["status"], result["cluster_name"], result["number_of_nodes"], result["number_of_data_nodes"],
          result["active_shards"], result["active_shards_percent_as_number"], result["initializing_shards"],
          result["unassigned_shards"], result["number_of_pending_tasks"], result["number_of_in_flight_fetch"],
          result["task_max_waiting_in_queue_millis"])
  return response

# Perform arbitrary query
def search(conf, args):
  if not conf.get('searchEnabled'):
    return "Search has been disabled by the administrator."

  count = 1
  if args == "help" or args == "-help" or args is None or len(args) == 0:
    return """Executes a simple search of the Elasticsearch cluster across all indices. Results are returned in descending time order (most recent first).```
Usage: ${prefix}search <query [|<max>]>
Options:
  query     - Lucene syntax query string
  max       - Maximum number of records to display; defaults to %d
```""" % (count)

  splitArgs = args.split("|", 1)
  name = splitArgs[0].replace(" ", "\\ ").strip()
  if len(splitArgs) > 1:
    count = int(splitArgs[1].strip())

  client = createElasticsearchClient(conf)
  result = client.search(size=count, q=args, sort='@timestamp:desc')
  
  if result.get('hits') and result.get('hits').get('hits'):
    num = result['hits']['total']
    result = result['hits']['hits']
    response = "Found " + str(num) + " matching record(s), showing " + str(count) + ":\n"
    for record in result:
      response = response + "```\n"
      source = record['_source']
      index = record['_index']
      response = response + "index: " + str(index) + "\n"
      for k in source:
        response = response + " " + str(k) + ": " + str(source[k]) + "\n"
      response = response + "```\n"
  else:
    response = "No results found"

  return response

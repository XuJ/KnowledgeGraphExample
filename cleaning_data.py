import numpy as np
import pandas as pd
import json

offline = pd.read_csv('data/offline.csv')
with open('data/data.json') as f:
    for line in f:
        data_orig = json.loads(line)

data = data_orig['data']
rel = data['relationships']
node = data['nodes']

source_node_df = pd.DataFrame(node, columns=['source_bbd_id', 'source_name', 'source_isperson', 'source_degree'])
source_node_df['source_isperson'] = source_node_df['source_isperson'].apply(lambda x: 1 if x == 'BBDPerson' else 0)

destination_node_df = pd.DataFrame(node, columns=['destination_bbd_id', 'destination_name', 'destination_isperson',
                                                  'destination_degree'])
destination_node_df['destination_isperson'] = destination_node_df['destination_isperson'].apply(
    lambda x: 1 if x == 'BBDPerson' else 0)

rel_df = pd.DataFrame(rel, columns=['source_bbd_id', 'destination_bbd_id', 'relation_type', 'position'])
rel_df['position'] = rel_df['position'].apply(lambda x: x.get('position', np.nan))

result = pd.merge(rel_df, source_node_df, how='left', on='source_bbd_id')
result = pd.merge(result, destination_node_df, how='left', on='destination_bbd_id')
result['company_name'] = node[0][1]
result['bbd_qyxx_id'] = node[0][0]
result['dt'] = '20180713'
result2 = result[offline.columns]
result2.to_csv('data/offline2.csv', index=False)

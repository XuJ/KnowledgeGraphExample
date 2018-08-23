import os

import pandas as pd
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth(user="neo4j", password="123456789"))

folder = r'C:/Users/john/Documents/Neo4j/chongqing/import'
vertex_file = 'chongqing_vertex_df_20180821.csv'
edge_file = 'chongqing_edge_df_20180821.csv'

vertex = pd.read_csv(os.path.join(folder, vertex_file), encoding='utf8')
vertex['label'] = vertex['is_person'].apply(lambda x: 'PERSON' if x == 1 else 'COMPANY')
vertex = vertex[['name', 'label']]
vertex.drop_duplicates(inplace=True)

edge = pd.read_csv(os.path.join(folder, edge_file), encoding='utf8')
edge = edge[['start_name', 'end_name', 'relation']]
edge_ba = edge[edge['relation'].isin(['director', 'supervisor', 'executive'])].sort_values(
    by=['end_name', 'start_name', 'relation'], ascending=[True, True, False])
edge_gd = edge[edge['relation'].isin(['invest', 'hist_invest'])].sort_values(by=['end_name', 'start_name', 'relation'],
                                                                             ascending=[True, True, False])
edge_fr = edge[edge['relation'].isin(['legal', 'hist_legal'])].sort_values(by=['end_name', 'start_name', 'relation'],
                                                                           ascending=[True, True, False])
edge_ba.drop_duplicates(keep='first', inplace=True)
edge_gd.drop_duplicates(subset=['end_name', 'start_name'], keep='first', inplace=True)
edge_fr.drop_duplicates(subset=['end_name', 'start_name'], keep='first', inplace=True)
edge = pd.concat([edge_ba, edge_gd, edge_fr])

with driver.session() as session:
    session.run("match (n) detach delete n")

    vertex.to_csv(os.path.join(folder, 'vertex_info.csv'), index=False, encoding='utf8')

    session.run("""
      using periodic commit
      load csv with headers from "file:///vertex_info.csv" as row
      create (x:NODE{name:row.name,label:row.label})
      """)
    session.run("""
      CREATE INDEX ON :NODE(name)
      """)

    for label in vertex.label.unique():
        session.run("""
          match (n:NODE{label:'%s'}) set n:%s;
          """ % (label, label))

    for relation, df in edge.groupby('relation'):
        df.to_csv(os.path.join(folder, 'edge_info_{}.csv'.format(relation.lower())), index=False, encoding='utf8')

        session.run("""
          using periodic commit
          load csv with headers from 'file:///edge_info_%s.csv' as row
          match (a:NODE{name:row.start_name}), (b:NODE{
          name:row.end_name})
          merge (a)-[r:%s]->(b);
          """ % (relation.lower(), relation.upper()))

    session.run("""
      drop index on :NODE(name);
      """)

    session.run("""
      match (n:NODE)
      remove n:NODE;
      """)

import os

import pandas as pd
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth(user="neo4j", password="123456789"))

folder = r'C:/Users/john/Documents/Neo4j/chongqing/import'
# vertex_file = 'chongqing_vertex_df.csv'
# edge_file = 'chongqing_edge_df.csv'
vertex_file = 'chongqing_vertex_df_20180820.csv'
edge_file = 'chongqing_edge_df_20180820.csv'
target_companies = ['重庆红池药业开发有限公司', '重庆绿华电动车有限公司', '重庆友强门窗有限公司']

vertex = pd.read_csv(os.path.join(folder, vertex_file), encoding='utf8')
vertex['leijinrong_label'] = vertex['label']
vertex['label'] = vertex['is_person'].apply(lambda x: 'PERSON' if x == 1 else 'COMPANY')
vertex = vertex[['name', 'label']]
vertex.drop_duplicates(inplace=True)

edge = pd.read_csv(os.path.join(folder, edge_file), encoding='utf8')
edge = edge[['start_name', 'end_name', 'relation']]
edge.drop_duplicates(inplace=True)

history_edge_df = pd.DataFrame({
    'start_name': ['韩曦', '胡蓉', '冯海军', '伍煜仙', '谢会琼', '冯海军', '冯海军', '冯海军', '韩曦', '王传侠', '谢会琼', '杨竣童', '杨国成', '王兴强', '李涛',
                   '冯海军'],
    'end_name': ['重庆红池药业开发有限公司'] * 3 + ['重庆绿华电动车有限公司'] * 3 + ['重庆友强门窗有限公司'] + ['重庆市中驰置业有限公司'] + [
        '重庆啡可咖啡经营管理有限公司'] * 4 + ['重庆亿茂达机动车配件有限责任公司'] + ['西双版纳共语咖啡发展有限公司'] * 3,
    })
history_edge_df['relation'] = 'history'

same_contact_edge_df = pd.DataFrame({
    'start_name': ['西双版纳共语咖啡发展有限公司', '普洱共语咖啡进出口有限公司', '重庆红池药业开发有限公司', '重庆宝元中药材种植有限公司'],
    'end_name': ['普洱共语咖啡进出口有限公司', '西双版纳共语咖啡发展有限公司', '重庆宝元中药材种植有限公司', '重庆红池药业开发有限公司']
    })
same_contact_edge_df['relation'] = 'contact'

history_vertex_df = pd.DataFrame({
    'name': history_edge_df.start_name.unique(),
    'label': ['PERSON'] * len(history_edge_df.start_name.unique())
    })

edge = edge.append(history_edge_df).append(same_contact_edge_df)
edge.drop_duplicates(inplace=True)
vertex = vertex.append(history_vertex_df)
vertex.drop_duplicates(inplace=True)

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

    for company in target_companies:
        session.run("""
        match (n:NODE{name:'%s'}) set n:TARGET
        """ % company)

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

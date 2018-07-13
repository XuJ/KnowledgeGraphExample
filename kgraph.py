#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/21 18:24
# @Author  : Gan
# @File    : kgraph.py

import pandas as pd
import json
import re
import copy


class EventChangeKG(object):
    name_entity = re.compile(
        u'(^[\u4e00-\u9fa5][\u4e00-\u9fa5（）\(\)]*[\u4e00-\u9fa5\)）]$)')
    clean_l = re.compile(r'\(')
    clean_r = re.compile(r'\)')

    def __init__(self, change_df, vertex_dict, subset=None, add_ba=False):
        self._base_df = change_df.copy(deep=True)
        self._add_ba = add_ba
        self._vertex_dict = copy.deepcopy(vertex_dict)
        self._vertex, self._edge = self._create_data(subset=subset)

    @property
    def vertex(self):
        return self._vertex

    @property
    def edge(self):
        return self._edge

    def _add_hist_vertex(self):
        cname_change_event = self._base_df.query(
            "change_key in ('company_name')")
        for _, row in cname_change_event.iterrows():
            if row['content_after_change'] not in self._vertex_dict:
                self._vertex_dict[row['content_after_change']] = row[
                    'bbd_qyxx_id']
            if row['content_before_change'] not in self._vertex_dict:
                self._vertex_dict[row['content_before_change']] = row[
                    'bbd_qyxx_id']

    def _create_data(self, subset=None):

        edge_list = []
        vertex = {}
        date_id = {}
        count_id = {}
        event_2_name = {
            'frname': {
                'base_name': 'LEGAL_CHANGE_{}',
                'name': '法人变更'
            },
            'gdxx': {
                'base_name': 'SHAREHOLDER_CHANGE_{}',
                'name': '股东变更'
            },
            'baxx': {
                'base_name': 'REGISTRATION_{}',
                'name': '备案信息'
            }
        }
        if self._add_ba:
            data_filtered = self._base_df.query(
                " change_key in ('gdxx', 'baxx', 'frname')")
        else:
            data_filtered = self._base_df.query(
                " change_key in ('gdxx', 'frname')")

        all_change_event = {}

        for _, row in data_filtered.iterrows():
            bbd_id = row['bbd_qyxx_id']

            if subset:
                if bbd_id not in subset:
                    continue

            u1 = json.loads(row['content_before_change'])
            u2 = json.loads(row['content_after_change'])

            if row['bbd_qyxx_id'] not in vertex:
                vertex[row['bbd_qyxx_id']] = {
                    'name': row['company_name'],
                    'label': 'COMPANY'
                }
            if row['change_date'] not in date_id:
                date_id[row['change_date']] = 'TIME_{}'.format(
                    len(date_id) + 1)
                vertex[date_id[row['change_date']]] = {
                    'name': row['change_date'],
                    'label': 'TIME'
                }
            node_time = date_id[row['change_date']]
            node_company = row['bbd_qyxx_id']
            atime = row['change_date']
            if node_company not in all_change_event:
                all_change_event[node_company] = {atime: {}}
            if atime not in all_change_event[node_company]:
                all_change_event[node_company][atime] = {}

            change_event = all_change_event[node_company][atime]

            for u in u1:
                event = u.get('change_key', row['change_key'])
                if event not in change_event:
                    count_id[event] = count_id.get(event, 0) + 1
                    event_id = count_id[event]
                    change_event[event] = {
                        'name':
                        event_2_name[event]['base_name'].format(event_id),
                        'sub_count': 0,
                        'position_visited': {}
                    }
                    node_event = change_event[event]['name']
                    vertex[node_event] = {
                        'name': event_2_name[event]['name'],
                        'label': 'EVENT'
                    }

                    edge_list.append((node_company, node_event, 'HAVE_EVENT',
                                      None, None))
                    edge_list.append((node_event, node_time, 'WHEN', None,
                                      None))

                node_event = change_event[event]['name']

                if event == 'frname':
                    name_clean_found = self.name_entity.search(u['name'])
                    if not name_clean_found:
                        print('NOT FOUND', u['name'])
                        continue
                    name_clean = name_clean_found.group(1)
                    name_clean = self.clean_l.sub('（', name_clean)
                    name_clean = self.clean_r.sub('）', name_clean)

                    if len(name_clean) < 2:
                        print(name_clean)
                        continue
                    if name_clean not in self._vertex_dict:
                        self._vertex_dict[name_clean] = name_clean + bbd_id

                    if self._vertex_dict[name_clean] not in vertex:
                        vertex[self._vertex_dict[name_clean]] = {
                            'name': name_clean,
                            'label': 'PERSON'
                            if len(name_clean) < 5 else 'COMPANY'
                        }
                    edge_list.append((self._vertex_dict[name_clean],
                                      node_event, 'LEAVE', None, None))
                elif event == 'baxx':
                    name_clean_found = self.name_entity.search(u['name'])
                    if not name_clean_found:
                        print('NOT FOUND', u['name'])
                        continue
                    name_clean = name_clean_found.group(1)
                    name_clean = self.clean_l.sub('（', name_clean)
                    name_clean = self.clean_r.sub('）', name_clean)

                    if len(name_clean) < 2:
                        print(name_clean)
                        continue

                    position = u.get('position', '其他人员')
                    position_visited = change_event[event]['position_visited']

                    if position not in position_visited:
                        change_event[event]['sub_count'] += 1
                        position_visited[position] = node_event + '_' + str(
                            change_event[event]['sub_count'])
                        vertex[position_visited[position]] = {
                            'name': position,
                            'label': 'POSITION'
                        }
                        node_position = position_visited[position]
                        edge_list.append((node_event, node_position,
                                          'HAVE_SUB_EVENT', None, None))

                    node_position = position_visited[position]

                    if name_clean not in self._vertex_dict:
                        self._vertex_dict[name_clean] = name_clean + bbd_id

                    if self._vertex_dict[name_clean] not in vertex:
                        vertex[self._vertex_dict[name_clean]] = {
                            'name': name_clean,
                            'label': 'PERSON'
                            if len(name_clean) < 5 else 'COMPANY'
                        }
                    node_entity = self._vertex_dict[name_clean]
                    edge_list.append((node_entity, node_position, 'LEAVE',
                                      None, None))

                elif event == 'gdxx':
                    name_clean_found = self.name_entity.search(
                        u['shareholder_name'])
                    if not name_clean_found:
                        print('NOT FOUND', u['shareholder_name'])
                        continue
                    name_clean = name_clean_found.group(1)
                    name_clean = self.clean_l.sub('（', name_clean)
                    name_clean = self.clean_r.sub('）', name_clean)

                    if len(name_clean) < 2:
                        print(name_clean)
                        continue

                    if name_clean not in self._vertex_dict:
                        self._vertex_dict[name_clean] = name_clean + bbd_id

                    if self._vertex_dict[name_clean] not in vertex:
                        vertex[self._vertex_dict[name_clean]] = {
                            'name': name_clean,
                            'label': 'PERSON'
                            if len(name_clean) < 5 else 'COMPANY'
                        }

                    node_entity = self._vertex_dict[name_clean]
                    edge_list.append((node_entity, node_event, 'LEAVE',
                                      u.get('subscribed_capital', 'UNKONWN'),
                                      u.get('invest_ratio', 'UNKNOWN')))

            for u in u2:
                event = u.get('change_key', row['change_key'])
                if event not in change_event:
                    count_id[event] = count_id.get(event, 0) + 1
                    event_id = count_id[event]

                    change_event[event] = {
                        'name':
                        event_2_name[event]['base_name'].format(event_id),
                        'sub_count': 0,
                        'position_visited': {}
                    }
                    node_event = change_event[event]['name']
                    vertex[node_event] = {
                        'name': event_2_name[event]['name'],
                        'label': 'EVENT'
                    }

                    edge_list.append((node_company, node_event, 'HAVE_EVENT',
                                      None, None))
                    edge_list.append((node_event, node_time, 'WHEN', None,
                                      None))

                node_event = change_event[event]['name']

                if event == 'frname':
                    name_clean_found = self.name_entity.search(u['name'])
                    if not name_clean_found:
                        print('NOT FOUND', u['name'])
                        continue
                    name_clean = name_clean_found.group(1)
                    name_clean = self.clean_l.sub('（', name_clean)
                    name_clean = self.clean_r.sub('）', name_clean)
                    if len(name_clean) < 2:
                        print(name_clean)
                        continue

                    if name_clean not in self._vertex_dict:
                        self._vertex_dict[name_clean] = name_clean + bbd_id

                    if self._vertex_dict[name_clean] not in vertex:
                        vertex[self._vertex_dict[name_clean]] = {
                            'name': name_clean,
                            'label': 'PERSON'
                            if len(name_clean) < 5 else 'COMPANY'
                        }
                    edge_list.append(
                        (node_event, self._vertex_dict[name_clean], 'JOIN',
                         None, None))
                elif event == 'baxx':
                    name_clean_found = self.name_entity.search(u['name'])
                    if not name_clean_found:
                        print('NOT FOUND', u['name'])
                        continue
                    name_clean = name_clean_found.group(1)
                    name_clean = self.clean_l.sub('（', name_clean)
                    name_clean = self.clean_r.sub('）', name_clean)
                    if len(name_clean) < 2:
                        print(name_clean)
                        continue

                    position = u.get('position', '其他人员')
                    position_visited = change_event[event]['position_visited']

                    if position not in position_visited:
                        change_event[event]['sub_count'] += 1
                        position_visited[position] = node_event + '_' + str(
                            change_event[event]['sub_count'])
                        vertex[position_visited[position]] = {
                            'name': position,
                            'label': 'POSITION'
                        }
                        node_position = position_visited[position]
                        edge_list.append((node_event, node_position,
                                          'HAVE_SUB_EVENT', None, None))

                    node_position = position_visited[position]

                    if name_clean not in self._vertex_dict:
                        self._vertex_dict[name_clean] = name_clean + bbd_id

                    if self._vertex_dict[name_clean] not in vertex:
                        vertex[self._vertex_dict[name_clean]] = {
                            'name': name_clean,
                            'label': 'PERSON'
                            if len(name_clean) < 5 else 'COMPANY'
                        }
                    node_entity = self._vertex_dict[name_clean]
                    edge_list.append((node_position, node_entity, 'JOIN', None,
                                      None))

                elif event == 'gdxx':
                    name_clean_found = self.name_entity.search(
                        u['shareholder_name'])
                    if not name_clean_found:
                        print('NOT FOUND', u['shareholder_name'])
                        continue
                    name_clean = name_clean_found.group(1)
                    name_clean = self.clean_l.sub('（', name_clean)
                    name_clean = self.clean_r.sub('）', name_clean)
                    if len(name_clean) < 2:
                        print(name_clean)
                        continue

                    if name_clean not in self._vertex_dict:
                        self._vertex_dict[name_clean] = name_clean + bbd_id

                    if self._vertex_dict[name_clean] not in vertex:
                        vertex[self._vertex_dict[name_clean]] = {
                            'name': name_clean,
                            'label': 'PERSON'
                            if len(name_clean) < 5 else 'COMPANY'
                        }

                    node_entity = self._vertex_dict[name_clean]
                    edge_list.append((node_event, node_entity, 'JOIN',
                                      u.get('subscribed_capital', 'UNKONWN'),
                                      u.get('invest_ratio', 'UNKNOWN')))

        vertex_df = pd.DataFrame.from_dict(
            vertex, orient='index').reset_index()
        vertex_df.columns = ['uid', 'name', 'label']
        edge_df = pd.DataFrame(
            edge_list,
            columns=['start_uid', 'end_uid', 'label', 'capital', 'ratio'])
        edge_df.sort_values(
            by=['start_uid', 'end_uid', 'label', 'capital', 'ratio'])
        edge_df = edge_df.drop_duplicates(
            subset=['start_uid', 'end_uid', 'label']).reset_index(drop=True)
        return vertex_df, edge_df

    def to_neo4j(self, folder, driver=None, erase_all=True):
        import os
        if driver is None:
            from neo4j.v1 import GraphDatabase, basic_auth
            driver = GraphDatabase.driver(
                "bolt://localhost",
                auth=basic_auth(user="neo4j", password="123456"))

        with driver.session() as session:

            if erase_all:
                session.run("match (n) detach delete n")

            self.vertex.to_csv(
                os.path.join(folder, 'vertex.csv'),
                index=False,
                encoding='utf8')
            session.run("""
            using periodic commit
            load csv with headers from "file:///vertex.csv" as row
            create (x:NODE{uid:row.uid,name:row.name, label:row.label})
            """)
            session.run("""
            CREATE INDEX ON :NODE(uid)
            """)

            for label in self.vertex.label.unique():
                session.run("""
                match (n:NODE{label:'%s'}) set n:%s;
                """ % (label, label))

            with_info = self.edge[self.edge['capital'].notnull()]
            without_info = self.edge[self.edge['capital'].isnull()]

            for label, df in with_info.groupby('label'):
                df.to_csv(
                    os.path.join(folder,
                                 'edge_info_{}.csv'.format(label.lower())),
                    index=False,
                    encoding='utf8')

                session.run("""
                using periodic commit
                load csv with headers from 'file:///edge_info_%s.csv' as row
                match (a:NODE{uid:row.start_uid}), (b:NODE{uid:row.end_uid})
                merge (a)-[r:%s{capital:row.capital, ratio:row.ratio}]->(b);
                """ % (label.lower(), label.upper()))

            for label, df in without_info.groupby('label'):
                df.to_csv(
                    os.path.join(
                        folder,
                        'edge_without_info_{}.csv'.format(label.lower())),
                    index=False,
                    encoding='utf8')

                session.run("""
                using periodic commit
                load csv with headers from 'file:///edge_without_info_%s.csv' as row
                match (a:NODE{uid:row.start_uid}), (b:NODE{uid:row.end_uid})
                merge (a)-[r:%s]->(b);
                """ % (label.lower(), label.upper()))

            session.run("""
            drop index on :NODE(uid);
            """)

            session.run("""
            match (n:NODE)
            remove n:NODE;
            """)


def create_vertex_from_off_line_relation(df, bbd_id_as_key=True):
    dict_mapping = {}
    if bbd_id_as_key:
        for _, row in df.iterrows():
            if row['source_bbd_id'] not in dict_mapping:
                dict_mapping[row['source_bbd_id']] = {
                    'name': row['source_name'],
                    'is_person': row['source_isperson']
                }

            if row['destination_bbd_id'] not in dict_mapping:
                dict_mapping[row['destination_bbd_id']] = {
                    'name': row['destination_name'],
                    'is_person': row['destination_isperson']
                }
    else:
        for _, row in df.iterrows():
            if row['source_name'] not in dict_mapping:
                dict_mapping[row['source_name']] = {
                    'bbd_qyxx_id': row['source_bbd_id'],
                    'is_person': row['source_isperson']
                }

            if row['destination_name'] not in dict_mapping:
                dict_mapping[row['destination_name']] = {
                    'bbd_qyxx_id': row['destination_bbd_id'],
                    'is_person': row['destination_isperson']
                }
    return dict_mapping

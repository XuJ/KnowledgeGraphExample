#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/26 14:27
# @Author  : Gan
# @File    : to_neo4j.py


import pandas as pd
import sys
from neo4j.v1 import GraphDatabase, basic_auth
from kgraph import EventChangeKG, create_vertex_from_off_line_relation
# from graph_gene.core.kgraph import EventChangeKG


data = pd.read_csv('data/change.csv', encoding='utf8')

id_list = pd.read_csv('data/id_new.csv', encoding='utf8')
vertex_dict = id_list.set_index('company_name').to_dict()['bbd_qyxx_id']
# off_line_relations = pd.read_csv('data/offline_relation.csv')

# vertex = create_vertex_from_off_line_relation(off_line_relations, bbd_id_as_key=False)
# vertex_dict = {key: vertex[key]['bbd_qyxx_id'] for key in vertex}

# vertex_dict = {}

event_change_kg = EventChangeKG(data, vertex_dict, add_ba=False)

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth(user="neo4j", password="123456"))

# folder = r'E:\neo4j\change_event_txs\import'
folder = r'C:/Users/john/Documents/Neo4j/python_test/import'

import os
if not os.path.exists(folder):
    os.mkdir(folder)

event_change_kg.to_neo4j(folder=folder, driver=driver, erase_all=True)

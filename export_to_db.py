#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/11 10:49
# @Author  : Gan
# @File    : export_to_db.py


import os

import numpy as np
import pandas as pd

from tool import KunlunFormat

data_dir = 'data/suining'

edge = pd.DataFrame()
vertex = pd.DataFrame()
for file in os.listdir(data_dir):
    if file.startswith('edge_info'):
        edge = edge.append(pd.read_csv(os.path.join(data_dir, file), encoding='utf8'))
    elif file.startswith('vertex_info'):
        vertex = vertex.append(pd.read_csv(os.path.join(data_dir, file), encoding='utf8'))

id_list = np.arange(len(vertex))
vertex['uid'] = id_list
vertex['uid'] = vertex['uid'].apply(str)


def lookup_label(name):
    row = vertex[vertex['name'] == name].iloc[0]
    return row['label']


def lookup_uid(name):
    row = vertex[vertex['name'] == name].iloc[0]
    return row['uid']


edge['start_type'] = edge['start_name'].apply(lambda x: lookup_label(x))
edge['end_type'] = edge['end_name'].apply(lambda x: lookup_label(x))
edge['start_uid'] = edge['start_name'].apply(lambda x: lookup_uid(x))
edge['end_uid'] = edge['end_name'].apply(lambda x: lookup_uid(x))

edge = edge[['start_type', 'start_uid', 'end_type', 'end_uid', 'relation']]
edge['start_time'] = '2007/01/01'
edge['end_time'] = '2007/01/01'

kl = KunlunFormat()
vertex_kl_dict, vertex_kl_meta = kl.parse_vertex(vertex, col_rel='label')
edge_kl_dict, edge_kl_meta = kl.parse_edge(edge, start_label='start_type', end_label='end_type',
    relation_type='relation')
meta = pd.concat([vertex_kl_meta, edge_kl_meta])

#################################################################################
# 导出 CSV格式
#################################################################################
result_dir = 'result/csv_format'
try:
    os.makedirs(os.path.join(data_dir, result_dir))
except FileExistsError:
    pass

kl.to_csv(os.path.join(data_dir, result_dir), vertex_kl_dict, edge_kl_dict, meta)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/11 10:49
# @Author  : Gan
# @File    : export_to_db.py


import os
import pandas as pd

from tool import KunlunFormat


vertex = pd.read_csv('data/node1.csv', index_col=0, encoding='gbk', dtype={'number': str})
vertex.rename(columns={'number': 'uid'}, inplace=True)
edge = pd.read_csv('data/edge1.csv', index_col=0, encoding='gbk', dtype={'start_uid': str, 'end_uid': str})


kl = KunlunFormat()
vertex_kl_dict, vertex_kl_meta = kl.parse_vertex(vertex, col_rel='label')
edge_kl_dict, edge_kl_meta = kl.parse_edge(
    edge, start_label='start_type', end_label='end_type', relation_type='relation'
)
meta = pd.concat([vertex_kl_meta, edge_kl_meta])


#################################################################################
# 导出 CSV格式
#################################################################################
try:
    os.makedirs('data/result/csv_format/')
except FileExistsError:
    pass

kl.to_csv('data/result/csv_format/', vertex_kl_dict, edge_kl_dict, meta)


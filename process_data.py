#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/26 14:09
# @Author  : Gan
# @File    : process_data.py

import pandas as pd

data = pd.read_csv('./KnowlegeGraphExample/data/offline.csv')

id_list1 = data.loc[data['source_isperson'] == 0, 'source_bbd_id']
id_list2 = data.loc[data['destination_isperson'] == 0, 'destination_bbd_id']

name_list = pd.concat([id_list1, id_list2], ignore_index=True)
name_list.drop_duplicates(inplace=True)
name_list.to_csv('./KnowlegeGraphExample/data/company_bbd_id.csv', index=False, encoding='utf8')



from graph_gene.core import common

common.bfs_degree()
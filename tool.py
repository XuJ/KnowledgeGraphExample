#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/11 10:52
# @Author  : Gan
# @File    : tool.py

import re
import logging
import datetime as dt
import pandas as pd


class KunlunFormat(object):

    @staticmethod
    def data_type(x):
        if 'float' in str(x):
            return 'decimal(10,4)'
        elif 'int64' in str(x) or 'int32' in str(x):
            return 'bigint'
        elif 'int' in str(x):
            return 'int'
        elif 'date' in str(x):
            return 'datetime'
        else:
            # 'object' in str(x):
            return 'varchar(64)'

    def export(
            self, vertex, edge, meta=True, vertex_col_rel='label',
            edge_start_label='start_label', edge_end_label='end_label',
            edge_relation_type='relation_type'
    ):
        res_vertex, res_vertex_meta = self.parse_vertex(
            vertex, vertex_col_rel, meta
        )
        res_edge, res_edge_meta = self.parse_edge(
            edge, edge_start_label, edge_end_label, edge_relation_type, meta
        )
        res_meta = pd.concat([res_vertex_meta, res_edge_meta])

        return res_vertex, res_edge, res_meta

    def parse_vertex(self, vertex, col_rel='label', meta=True, keep_label=False):
        res = {}
        if meta:
            res_meta = []
        for rel, df in vertex.groupby(col_rel):
            table_name = 'vertex_{}'.format(rel)
            if not keep_label:
                del df[col_rel]
            res[table_name] = df.reset_index(drop=True)

            if 'start_time' not in df and 'end_time' not in df:
                now_str = dt.datetime.now().strftime('%Y-%m-%d')
                # now = dt.datetime.now()
                res[table_name]['start_time'] = now_str
                res[table_name]['end_time'] = now_str
            elif 'start_time' not in df:
                res[table_name]['start_time'] = res[table_name]['end_time']
            elif 'end_time' not in df:
                res[table_name]['end_time'] = res[table_name]['start_time']

            if meta:
                for key, col in res[table_name].dtypes.items():
                    if 'time' not in key:
                        res_meta.append([
                            1, table_name, key, key, self.data_type(col)
                        ])
                    else:
                        res_meta.append([
                            1, table_name, key, key, 'datetime'
                        ])

        if meta:
            return res, pd.DataFrame(res_meta, columns=[
                'class_permission', 'table_name', 'column_name', 'column_name_cn', 'data_type'
            ])
        else:
            return res

    def parse_edge(
            self, edge, start_label='start_label', end_label='end_label',
            relation_type='relation_type', meta=True, keep_label=False
    ):
        res = {}
        if meta:
            res_meta = []
        for row, df in edge.groupby([start_label, relation_type, end_label]):
            if not keep_label:
                del df[start_label]
                del df[relation_type]
                del df[end_label]

            table_name = 'edge_{}_{}_{}'.format(*(x for x in row))
            res[table_name] = df.reset_index(drop=True)

            if 'start_time' not in df and 'end_time' not in df:
                now_str = dt.datetime.now().strftime('%Y-%m-%d')
                # now = dt.datetime.now()
                res[table_name]['start_time'] = now_str
                res[table_name]['end_time'] = now_str
            elif 'start_time' not in df:
                res[table_name]['start_time'] = res[table_name]['end_time']
            elif 'end_time' not in df:
                res[table_name]['end_time'] = res[table_name]['start_time']

            if 'weight' not in res[table_name]:
                res[table_name]['weight'] = 1

            if meta:
                for key, col in res[table_name].dtypes.items():
                    if 'time' not in key:
                        res_meta.append([
                            1, table_name, key, key, self.data_type(col)
                        ])
                    else:
                        res_meta.append([
                            1, table_name, key, key, 'datetime'
                        ])

        if meta:
            return res, pd.DataFrame(res_meta, columns=[
                'class_permission', 'table_name', 'column_name', 'column_name_cn', 'data_type'
            ])
        else:
            return res

    def to_csv(self, folder, vertex_dict, edge_dict, meta_df):
        for k, df in vertex_dict.items():
            columns = df.columns
            new_columns = []
            for col in columns:
                col_type = meta_df.loc[(meta_df['table_name']==k) & (meta_df['column_name']==col), 'data_type'].iloc[0]
                if col_type == 'bigint':
                    col_type = 'int'
                new_columns.append(col + ':' + col_type)
            df.columns = new_columns
            df.to_csv(folder + '/' + k + '.csv', index=False, encoding='utf8')

        for k, df in edge_dict.items():
            columns = df.columns
            new_columns = []
            for col in columns:
                col_type = meta_df.loc[(meta_df['table_name']==k) & (meta_df['column_name']==col), 'data_type'].iloc[0]
                if col_type == 'bigint':
                    col_type = 'int'
                new_columns.append(col + ':' + col_type)
            df.columns = new_columns
            df.to_csv(folder + '/' + k + '.csv', index=False, encoding='utf8')

        # meta_df.to_csv(folder + '/graphdb_meta.csv', index=False, encoding='utf8')

    def to_db(self, conn, vertex_dict, edge_dict, meta_df, verbose=True):
        cursor = conn.cursor()

        meta_df_in_effect = meta_df[meta_df['class_permission'] == 1]
        for table_name, df_tmp in meta_df_in_effect.groupby('table_name'):
            if verbose:
                print('creating table {}'.format(table_name))
            create_table_sql = 'create table if not exists {} ('.format(table_name)
            column_order = []
            for _, row in df_tmp.iterrows():
                create_table_sql += '{} {},'.format(row['column_name'], row['data_type'])
                column_order.append(row['column_name'])
            # exclude the last ,
            create_table_sql = create_table_sql[:-1]
            create_table_sql += ")engine = InnoDB, CHARACTER SET=utf8;"
            cursor.execute(create_table_sql)

            insert_table_sql_base = 'insert into {} VALUES'.format(table_name)

            def quote_str(x):
                if type(x) == str:
                    return "\'" + clean_name(x) + "\'"
                else:
                    return str(x)

            def clean_name(x):
                return re.sub(r'[\"\']+', '', x)

            if table_name in vertex_dict:
                df = vertex_dict[table_name]
                count = 1
                insert_table_sql = insert_table_sql_base
                for _, row in df.iterrows():
                    need_to_insert = True
                    insert_table_sql += "({}),".format(",".join([quote_str(row[col]) for col in column_order]))

                    if count % 500 == 0:
                        cursor.execute(insert_table_sql[:-1])
                        insert_table_sql = insert_table_sql_base
                        need_to_insert = False
                        conn.commit()

                        if verbose:
                            print('inserting records {:.3f}'.format(count / len(df)))

                    count += 1

                if need_to_insert:
                    print('inserting records {:.3f}'.format(count / len(df)))
                    # print('last few records')
                    cursor.execute(insert_table_sql[:-1])
                    conn.commit()
            elif table_name in edge_dict:
                df = edge_dict[table_name]
                count = 1
                insert_table_sql = insert_table_sql_base
                for _, row in df.iterrows():
                    need_to_insert = True
                    insert_table_sql += "({}),".format(",".join([quote_str(row[col]) for col in column_order]))

                    if count % 500 == 0:
                        cursor.execute(insert_table_sql[:-1])
                        insert_table_sql = insert_table_sql_base
                        conn.commit()
                        need_to_insert = False

                        if verbose:
                            print('inserting records {:.3f}'.format(count / len(df)))

                    count += 1

                if need_to_insert:
                    print('inserting records {:.3f}'.format(count / len(df)))
                    # print('last few records')
                    cursor.execute(insert_table_sql[:-1])
                    conn.commit()
            else:
                logging.warning('data for table {} is not found'.format(table_name))

        if verbose:
            print('creating table graphdb_metadata')

        cursor.execute("""
                create table graphdb_metadata (
                class_permission INT,
                table_name VARCHAR(64),
                column_name VARCHAR(64),
                column_name_cn VARCHAR(64),
                data_type VARCHAR(32)
                ) engine = InnoDB, CHARACTER SET=utf8;
                """)
        insert_tmp = """
                insert into graphdb_metadata values 
                """
        for _, row in meta_df.iterrows():
            insert_tmp += "({}, '{}', '{}', '{}', '{}'),".format(
                row['class_permission'],
                row['table_name'],
                row['column_name'],
                row['column_name_cn'],
                row['data_type']
            )
        cursor.execute(insert_tmp[:-1])
        conn.commit()

        if verbose:
            print('finished')

import os
import pandas as pd

file_dir = 'data/suining'
company_df = pd.DataFrame()
person_df = pd.DataFrame()
for txt_file in os.listdir(file_dir):
    if not txt_file.startswith('company'):
        if txt_file.endswith('_1.txt'):
            tmp_df = pd.read_table(os.path.join(file_dir, txt_file), encoding='gbk', index_col=0)
            tmp_df['company'] = txt_file.split('.')[0]
            company_df = company_df.append(tmp_df)
        elif txt_file.endswith('_2.txt'):
            tmp_df = pd.read_table(os.path.join(file_dir, txt_file), encoding='gbk', index_col=0)
            tmp_df['company'] = txt_file.split('.')[0]
            person_df = person_df.append(tmp_df)


company_df_test = company_df.drop_duplicates(subset=['重要企业名称'])
person_df_test = person_df.drop_duplicates(subset=['重要人名称'])
print(len(company_df)-len(company_df_test))
print(len(person_df)-len(person_df_test))

vertex_file = 'suining_vertex_df_20180821.csv'
edge_file = 'suining_edge_df_20180821.csv'
vertex = pd.read_csv(os.path.join(file_dir, vertex_file), encoding='utf8')
edge = pd.read_csv(os.path.join(file_dir, edge_file), encoding='utf8')
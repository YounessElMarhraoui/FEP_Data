import os
import glob
import sys

import sqlite3
from scipy.io import loadmat

import pandas as pd


class StructureFepData:

    def __init__(self, data_path):
        self.data_path = data_path
        self.raw_data_path = os.path.join(self.data_path, 'RAW_DATA', 'DATA_FEP')
        self.structured_data_path = os.path.join(self.data_path, 'STRUCTURED_DATA', 'DATA_FEP')

        self.col_names = ["tHolo", "xHolo", "yHolo", "zHolo", "xfHolo", "yfHolo", "zfHolo", "xI", "yI", "zI",
                          "xIf", "yIf", "zIf", "xHeelR", "yHeelR", "zHeelR", "xHeelRf", "yHeelRf", "zHeelRf",
                          "xHeelL", "yHeelL", "zHeelL", "xHeelLf", "yHeelLf", "zHeelLf", "xToeR", "yToeR", "zToeR",
                          "xToeRf", "yToeRf", "zToeRf", "xToeL", "yToeL", "zToeL", "xToeLf", "yToeLf", "zToeLf",
                          "HoloSD", "Zeni"]

    def mat_to_df(self, mat, id_pat, id_trial):
        df = (pd.DataFrame(mat, columns=self.col_names)
              .assign(id_pat=id_pat, id_trial=id_trial)
              .reindex(['id_pat', 'id_trial'] + self.col_names, axis=1)
              )
        return df

    def create_dfs_dict(self, raw_mat):
        nbr_pats = raw_mat[0].shape[0]
        dfs_dict = {}
        for i in range(nbr_pats):
            dfs_dict.update({i: self.mat_to_df(raw_mat[0][i][j][0], i+1, j+1) for j in range(raw_mat[0][i].shape[0])})
        return dfs_dict

    def concat_dfs(self, dfs_dict):
        return pd.concat(dfs_dict.values())

    def store_df(self, df, file_name):
        print("Storing data:")
        conn = sqlite3.connect(os.path.join(self.structured_data_path, '{}.db'.format(file_name)))
        cur = conn.cursor()

        cur.execute('DROP TABLE IF EXISTS cea_data')

        df.to_sql('file_name', conn, if_exists='replace')
        conn.close()
        print('     SQL storage (DONE - 1/3)')
        df.to_csv(os.path.join(self.structured_data_path, '{}.csv'.format(file_name)), index=False)
        print('     CSV storage (DONE - 2/3)')
        df.to_parquet(os.path.join(self.structured_data_path, '{}.parquet.gzip'.format(file_name)), compression='gzip')
        print('     Parquet storage (DONE - 3/3)')

    def pipeline(self, file_path):
        mdata = loadmat(file_path)
        mat_name = list(mdata.keys())[-1]
        print('Working on {} file'.format(mat_name))
        raw_data = mdata[mat_name]
        dfs_dict = self.create_dfs_dict(raw_data)
        global_df = self.concat_dfs(dfs_dict)
        self.store_df(global_df, mat_name)

    def apply_to_files(self):
        list_files = glob.glob(self.raw_data_path + "/**/*.mat", recursive=True)
        print('{} .mat files are found!'.format(len(list_files)))
        for file in list_files:
            try:
                self.pipeline(file)
            except IndexError:
                print('{} file is empty!'.format(file))


if __name__ == '__main__':
    DATA_PATH = sys.argv[1]
    # print(os.listdir(DATA_PATH))
    sd = StructureFepData(DATA_PATH)
    sd.apply_to_files()
    print("Finished!")

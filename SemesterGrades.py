from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import csv
import openpyxl
import pandas as pd

sheet_names = ['21fall', '20spring', '20fall', '19spring', '19fall']
# pd.set_option('display.max_rows', None)


class CassandraSession:

    def __init__(self):
        self.cluster = None
        self.session = None
        self.prepared_queries = None

    def connect(self, keyspace):
        self.cluster = Cluster()
        self.session = self.cluster.connect(keyspace)
        self.session.row_factory = dict_factory
        self.prepared_queries = {
            'insert': self.session.prepare(
                "INSERT INTO gpa_table (student_id, gpa, semester, reg_year, dep_code, student_name) "
                "VALUES (?, ?, ?, ?, ?, ?);"
            ),
            'select': self.session.prepare(
                "SELECT * FROM gpa_table WHERE dep_code=? AND semester=?"
            )
        }
        print("Session is ready.")

    def select_query(self, dep_code, semester):
        result = self.session.execute(self.prepared_queries['select'], (dep_code, semester))

        for row in result.all():
            print('%(student_name)20s %(student_id)11s %(gpa).2f %(semester)8s %(dep_code)4s %(reg_year)4d' % row)

    def insert_query(self, student_df):
        try:
            for student in student_df.loc:
                self.session.execute(self.prepared_queries['insert'], (student.tolist()))
        except KeyError:
            pass
        print(f'Inserted {len(student_df.index)} rows.')


if __name__ == '__main__':

    def read_excel():
        gpa_df = pd.DataFrame(columns=['student_id', 'gpa', 'semester'])
        for sheet_name in sheet_names:
            semester_df = pd.read_excel(open('semester_list.xlsx', 'rb'),
                                        sheet_name=sheet_name, index_col=None)
            semester_df['semester'] = sheet_name
            gpa_df = pd.concat((gpa_df, semester_df), ignore_index=True)

        gpa_df['gpa'] = gpa_df['gpa'].astype('float') / 100
        gpa_df['reg_year'] = None
        gpa_df['dep_code'] = None

        for i in range(len(gpa_df.index)):
            student_id = str(gpa_df.loc[i, 'student_id'])
            gpa_df.loc[i, 'reg_year'] = int(student_id[:4])
            gpa_df.loc[i, 'dep_code'] = student_id[4:8]

        return gpa_df

    # print('Bilgisayar M\\u00fchendisli\\u011fi'.encode('latin1').decode('unicode_escape'))

    cs = CassandraSession()
    cs.connect('test_keyspace')

    student_gpa_df = read_excel()
    student_gpa_df = student_gpa_df.sort_values(by='gpa', ascending=False, ignore_index=True)

    # cs.insert_query(student_gpa_df.loc[0:1000, :])
    cs.insert_query(student_gpa_df)

    for sem in sheet_names:
        cs.select_query('0602', sem)

from cassandra.cluster import Cluster
import json
import numpy as np
import pandas as pd


pd.set_option('display.max_rows', None)


class QueryManager:

    def __init__(self, keyspace):
        self.cluster = Cluster()
        self.session = self.cluster.connect(keyspace)
        self.session.row_factory = QueryManager.pandas_factory
        self.session.default_fetch_size = None

        self.queries = {
            'insert': self.session.prepare(
                "INSERT INTO test_keyspace.gpa_table (student_id, gpa, semester, reg_year, dep_code, student_name) "
                "VALUES (?, ?, ?, ?, ?, ?);"
            ),
            'select': self.session.prepare(
                "SELECT * FROM test_keyspace.gpa_table WHERE dep_code=? AND semester=?;"
            ),
            'truncate': self.session.prepare(
                "TRUNCATE test_keyspace.gpa_table;"),
            'update_std_name': self.session.prepare(
                "UPDATE test_keyspace.gpa_table SET student_name=? WHERE dep_code=? AND semester=? AND student_id=?;")
        }
        self.contents = None
        with open('./data.json', 'r+', encoding='latin1') as io_file:
            self.contents = json.load(io_file)
            self.dep_code_to_name = self.contents['dep_code_to_name']
            self.dep_name_to_code = self.contents['dep_name_to_code']
            self.semesters = self.contents['semesters']

        print('Session is ready.')

    def select(self, dep_code, semester):
        result_set = self.session.execute(self.queries['select'], (dep_code, semester))
        return result_set._current_rows

    def insert(self, student_df=None):
        if student_df is None:
            student_df = self.read_excel()
        try:
            for student in student_df.loc:
                self.session.execute(self.queries['insert'], (student.tolist()))
        except KeyError:
            pass
        print(f'Inserted {len(student_df.index)} rows.')

    def update_student_names(self):
        for student_id, student_info in self.contents['student_infos'].items():
            for semester in self.semesters:
                self.session.execute(self.queries['update_std_name'],
                                     [student_info['name'],
                                      student_info['dep_code'],
                                      semester,
                                      int(student_id)])

    def truncate(self):
        self.session.execute(self.queries['truncate'], [])

    @staticmethod
    def pandas_factory(colnames, rows):
        if len(rows) == 1:
            return rows[0][0]
        return pd.DataFrame(rows, columns=colnames)

    def read_excel(self):
        import openpyxl

        gpa_df = pd.DataFrame(columns=['student_id', 'gpa', 'semester'])
        for sheet_name in self.semesters:
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

    def parse_student_info(self, filename):
        student_id_info = {}
        dep_code_to_name = {}

        for std_id, std_info in list(zip(self.contents['student_ids'], self.contents['raw_infos'])):
            std_info = std_info.encode('latin1').decode('unicode_escape')
            s_list = std_info.split()

            for i in range(len(s_list)):
                if not s_list[i].isupper():
                    std_name = ' '.join(s_list[:i])
                    dep_name = ' '.join(s_list[i:])
                    dep_code = std_id[4:8]

                    student_id_info[std_id] = {'name': std_name,
                                               'dep_code': dep_code}
                    dep_code_to_name[dep_code] = dep_name
                    break

        with open(filename, 'w', encoding='utf8') as json_file:
            self.contents['student_infos'] = student_id_info
            self.contents['dep_code_to_name'] = dep_code_to_name

            dep_name_to_code = {}
            for k, v in dep_code_to_name.items():
                dep_name_to_code[v] = k

            self.contents['dep_name_to_code'] = dep_name_to_code
            json.dump(self.contents, json_file)

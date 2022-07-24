from cassandra.cluster import Cluster, NoHostAvailable
import json
import pandas as pd

pd.set_option('display.max_rows', None)


# noinspection PyProtectedMember
class QueryManager:

    def __init__(self, keyspace):
        try:
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
                    "UPDATE test_keyspace.gpa_table SET student_name=? WHERE dep_code=? AND semester=? AND student_id=?;"),
                'select_new': self.session.prepare(
                    "SELECT student_name, semester_grades FROM test_keyspace.new_gpa_table WHERE dep_code=?;"
                ),
                'insert_new': self.session.prepare(
                    "INSERT INTO test_keyspace.new_gpa_table (student_id, student_name, semester_grades, reg_year, dep_code) "
                    "VALUES (?, ?, ?, ?, ?);"
                )
            }
            print('Session is ready.')

        except NoHostAvailable:
            print('Unable to connect to any servers.')

        self.contents = None
        with open('./data.json', 'r+', encoding='latin1') as io_file:
            self.contents = json.load(io_file)
            self.dep_code_to_name = self.contents['dep_code_to_name']
            self.dep_name_to_code = self.contents['dep_name_to_code']
            self.semesters = self.contents['semesters']

    def select(self, dep_code, semester=None):
        def select_depcode_semester():
            result_set = self.session.execute(self.queries['select'], (dep_code, semester))
            grades_df = result_set._current_rows[['student_name', 'gpa']]
            return grades_df.sort_values(by='gpa', ascending=False, ignore_index=True).dropna()

        def select_depcode():
            result_df = self.session.execute(self.queries['select_new'], [dep_code])._current_rows
            grades_df = pd.DataFrame(columns=['student_name', 'gpa', 'semester'])

            for i in range(len(result_df.index)):
                current_std = result_df.iloc[i]
                for k, v in current_std['semester_grades'].items():
                    grades_df = pd.concat([grades_df,
                                           pd.DataFrame([[current_std['student_name'], v, k]],
                                                        columns=['student_name', 'gpa', 'semester'])
                                           ], ignore_index=True)
            return grades_df.sort_values(by='gpa', ascending=False, ignore_index=True)

        if semester is None:
            return select_depcode()
        else:
            return select_depcode_semester()

    def insert(self, dtype, data=None):

        def insert_df(student_df=None):
            if student_df is None:
                student_df = self.read_excel()
            try:
                for student in student_df.loc:
                    self.session.execute(self.queries['insert'], (student.tolist()))
            except KeyError:
                pass
            print(f'Inserted {len(student_df.index)} rows.')

        def insert_dict(student_dict=None):
            from datetime import datetime

            if student_dict is None:
                student_dict = self.parse_from_excel()
            query_start = datetime.now()
            for v in student_dict.values():
                self.session.execute(self.queries['insert_new'], v)
            print(f'Inserted {len(student_dict.keys())} rows'
                  f' in {(datetime.now() - query_start).microseconds // 1000}ms.')

        if dtype == 'df':
            insert_df(data)
        elif dtype == 'dict':
            insert_dict(data)
        else:
            print('Bruh')

    def truncate(self):
        self.session.execute(self.queries['truncate'], [])

    def update_student_names(self):
        for student_id, student_info in self.contents['student_infos'].items():
            for semester in self.semesters:
                self.session.execute(self.queries['update_std_name'],
                                     [student_info['name'],
                                      student_info['dep_code'],
                                      semester,
                                      int(student_id)])

    @staticmethod
    def pandas_factory(colnames, rows):
        if len(rows) == 1:
            return rows[0][0]
        return pd.DataFrame(rows, columns=colnames)

    def read_excel(self):
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

    def parse_from_excel(self):
        student_info_dict = {}
        for sem in self.semesters:
            sem_df = pd.read_excel(open('semester_list.xlsx', 'rb'),
                                   sheet_name=sem, index_col=None)
            sem_df.index = sem_df['student_id']
            sem_df = sem_df.drop('student_id', axis=1)
            sem_df['gpa'] = sem_df['gpa'].astype('float') / 100

            for k, v in sem_df.to_dict()['gpa'].items():
                std_id = str(k)
                if std_id not in student_info_dict.keys():
                    student_info_dict[std_id] = {
                        'student_id': std_id,
                        'student_name': self.contents['student_infos'][std_id]['name'],
                        'semester_grades': {},
                        'reg_year': int(std_id[:4]),
                        'dep_code': std_id[4:8]}
                student_info_dict[std_id]['semester_grades'][sem] = v
        return student_info_dict

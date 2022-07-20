from implementations import *


if __name__ == '__main__':

    qm = QueryManager('test_keyspace')

    dep_name = 'Bilgisayar Mühendisliği'
    semester = '20fall'
    dep_code = qm.dep_name_to_code[dep_name]

    print('> Both "dep_code" and "semester" columns as partition keys:')
    print(f'>> dep_code={dep_code} semester={semester}')
    print(qm.select(dep_code=dep_code, semester=semester))

    print()

    print('> Only "dep_code" column as partition key:')
    print(f'>> dep_code={dep_code}')
    print(qm.select(dep_code=dep_code))

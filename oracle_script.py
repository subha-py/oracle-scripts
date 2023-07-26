import concurrent.futures
import subprocess
import argparse

parser = argparse.ArgumentParser(description='A program to create multiple dbs in oracle', usage='python3 oracle_script.py --cdb cdb --prefix pdb --db_count 5 --start 1')
parser.add_argument('--cdb', default='CDB', help='cdb name as per $ORACLE_HOME/network/admin/tnsnames.ora (default: CDB)',
                    type=str)
parser.add_argument('--prefix', default='pdb', help='prefix of pdb you want to create (default: pdb)', type=str)
parser.add_argument('--db_username', default='sys', help='username of db (default: sys)', type=str)
parser.add_argument('--db_password', default='cohesity', help='password of cdb (default: cohesity)', type=str)
parser.add_argument('--db_count', default=5, help='number of dbs you want to create (default: 5)', type=int)
parser.add_argument('--start', default=1, help='starting index (default: 1)', type=int)



def create_db(cdb_name,pdb_name, db_username,db_password ):
    print('creating - {}'.format(pdb_name))
    cmd = 'sqlplus -S {db_username}/{db_password}@{cdb_name} as sysdba @create_db.sql {pdb_name}'.format(
                cdb_name=cdb_name, pdb_name=pdb_name, db_username=db_username, db_password=db_password)
    cmd_args = cmd.split()
    exit_code = subprocess.run(cmd_args, stdout=subprocess.DEVNULL)
    if exit_code.returncode == 0:
        print('pdb - {} created'.format(pdb_name))
        return True
    return False

def list_pdbs(cdb_name, db_username,db_password):
    cmd = 'sqlplus -S {db_username}/{db_password}@{cdb_name} as sysdba @list_db.sql'.format(
        cdb_name=cdb_name, db_username=db_username,db_password=db_password)
    cmd_args = cmd.split()
    exit_code = subprocess.run(cmd_args)
    if exit_code.returncode == 0:
        return True
    return False



def create_dbs_in_parallel(cdb_name, db_prefix, db_count, start_index, db_username,db_password):
    list_pdbs(cdb_name, db_username,db_password)
    future_to_db = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=db_count) as executor:
        while db_count > 0:
            pdb_name = f'{db_prefix}{start_index}'
            start_index += 1
            arg = (cdb_name, pdb_name)
            future_to_db[executor.submit(create_db, *arg)] = pdb_name
            db_count -= 1


    result = []
    for future in concurrent.futures.as_completed(future_to_db):
        pdb_name = future_to_db[future]
        try:
            res = future.result()
            if not res:
                result.append(pdb_name)
        except Exception as exc:
            print("%r generated an exception: %s" % (pdb_name, exc))
    list_pdbs(cdb_name, db_username, db_password)
    return result

if __name__ == '__main__':
    result = parser.parse_args()
    create_dbs_in_parallel(result.cdb, result.prefix, result.db_count, result.start,
                           result.db_username, result.db_password)
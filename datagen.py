import concurrent.futures
import subprocess
import argparse

parser = argparse.ArgumentParser(description='A program to populate multiple dbs in oracle',
                                 usage='python3 datagen.py --prefix pdb --db_count 5 --start 1 --oewizard_path /home/oracle/Downloads/swingbench/bin/oewizard --pdb_username pdbadmin --pdb_password pdbadmin --hostname 10.14.70.51 --scale 0.1')
parser.add_argument('--prefix', default='pdb', help='prefix of pdb you want to create (default: pdb)', type=str)
parser.add_argument('--pdb_username', default='pdbadmin', help='username of db (default: pdbadmin)', type=str)
parser.add_argument('--pdb_password', default='pdbadmin', help='password of cdb (default: pdbadmin)', type=str)
parser.add_argument('--db_count', default=5, help='number of dbs you want to create (default: 5)', type=int)
parser.add_argument('--start', default=1, help='starting index (default: 1)', type=int)
parser.add_argument('--oewizard_path', default='/home/oracle/Downloads/swingbench/bin/oewizard',
                    help='path to oewizard of swingbench', type=str)
parser.add_argument('--hostname', default='localhost', help='hostname of oracle host (default: localhost)', type=str)
parser.add_argument('--scale', default=0.1, help='scale of datagen (default: 0.1)', type=float)


def create_table_and_fill(oewizard_path, hostname, pdb_name, pdb_password, pdb_username, scale):

    args = f'-c soe.xml -cs //{hostname}/{pdb_name} -dbap {pdb_password} -dba {pdb_username} -u soe -p soe -async_off -scale {scale} -hashpart -create -cl -v -debug -tc 5'
    print(f'running swingbench with - {oewizard_path} {args}')
    cmd = f'{oewizard_path} {args}'
    cmd_args = cmd.split()
    exit_code = subprocess.run(cmd_args, stdout=subprocess.DEVNULL)
    if exit_code.returncode == 0:
        print(f'swingbench completed with - {args}')
        return True
    return False


def create_table_and_fill_in_parallel(db_prefix, db_count, start_index, oewizard_path, pdb_username, pdb_password, hostname, scale):
    future_to_db = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=db_count) as executor:
        while db_count > 0:
            pdb_name = f'{db_prefix}{start_index}'
            start_index += 1
            arg = (oewizard_path, hostname, pdb_name, pdb_password, pdb_username, scale)
            future_to_db[executor.submit(create_table_and_fill, *arg)] = pdb_name
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
    return result


if __name__ == '__main__':
    result = parser.parse_args()
    create_table_and_fill_in_parallel(db_prefix=result.prefix, db_count=result.db_count, start_index=result.start,oewizard_path=result.oewizard_path,
                                      pdb_username=result.pdb_username, pdb_password=result.pdb_password,
                                      hostname=result.hostname, scale=result.scale)
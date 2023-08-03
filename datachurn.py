import concurrent.futures
import subprocess
import argparse
import random
parser = argparse.ArgumentParser(description='A program to update multiple dbs in oracle',
                                 usage='python3 datachurn.py --prefix pdb --db_count 5 --start 1 --oewizard_path /home/oracle/Downloads/swingbench/bin/oewizard --pdb_username pdbadmin --pdb_password pdbadmin --hostname 10.14.70.51 --scale 0.1')
parser.add_argument('--prefix', default='pdb', help='prefix of pdb you want to churn (default: pdb)', type=str)
parser.add_argument('--db_count', default=5, help='number of dbs you want to churn (default: 5)', type=int)
parser.add_argument('--start', default=1, help='starting index (default: 1)', type=int)
parser.add_argument('--charbench_path', default='/home/oracle/Downloads/swingbench/bin/charbench',
                    help='path to charbench of swingbench', type=str)
parser.add_argument('--hostname', default='localhost', help='hostname of oracle host (default: localhost)', type=str)
parser.add_argument('--users', default=0.1, help='how many users you want to churn', type=float)
parser.add_argument('--runtime', default=False, help='for how long you want to run churn', type=str)

def create_table_and_fill(oewizard_path, hostname, pdb_name, pdb_password, pdb_username, scale, is_random, random_low, random_high):
    if is_random:
        scale = float("{:.2f}".format(random.uniform(random_low, random_high)))
    args = f'-c soe.xml -cs //{hostname}/{pdb_name} -dbap {pdb_password} -dba {pdb_username} -u soe -p soe -async_off -scale {scale} -hashpart -create -cl -v -debug -tc 15'
    print(f'running swingbench with - {oewizard_path} {args}')
    cmd = f'{oewizard_path} {args}'
    cmd_args = cmd.split()
    exit_code = subprocess.run(cmd_args, stdout=subprocess.DEVNULL)
    if exit_code.returncode == 0:
        print(f'swingbench completed with - {args}')
        return True
    return False

def churn_data(charbench_path, hostname, pdb_name, users, runtime):
    args = f'-c ../configs/SOE_Server_Side_V2.xml -cs //{hostname}/{pdb_name} -u soe -p soe -v users,tpm,tps,vresp -intermin 0 -intermax 0 -min 0 -max 0 -uc {users} -di SQ,WQ,WA -rt {runtime}'
    print(f'running charbench with - {charbench_path} {args}')
    cmd = f'{charbench_path} {args}'
    cmd_args = cmd.split()
    exit_code = subprocess.run(cmd_args, stdout=subprocess.DEVNULL)
    if exit_code.returncode == 0:
        print(f'charbench completed with - {args}')
        return True
    print(f'charbench ran into some issue - {args}')
    return False

def churn_db_data_in_parallel(db_prefix, db_count, start_index, charbench_path, users, runtime, hostname):


    future_to_db = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=db_count) as executor:
        while db_count > 0:
            pdb_name = f'{db_prefix}{start_index}'
            start_index += 1
            arg = (charbench_path, hostname, pdb_name, users, runtime)
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
    churn_db_data_in_parallel(db_prefix=result.prefix, db_count=result.db_count, start_index=result.start,
                                charbench_path=result.charbench_path,
                                hostname=result.hostname, users=result.users,runtime=result.runtime)
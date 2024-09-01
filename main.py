from concurrent.futures import ThreadPoolExecutor
from indexed import IndexedOrderedDict
from traceback import format_exc
from datetime import datetime
import requests
import time
import orjson, json
import os


sfdp_list_api_url = 'https://api.solana.org/api/validators/list?offset={}&limit={}&order_by=name&order=asc'
sfdp_validator_api_url = 'https://api.solana.org/api/validators/{}'
rejected_validators = IndexedOrderedDict()
LIST_PATH: str = 'list-all.txt'
SECOND_LIST_PATH: str = 'list-1000.txt'
RETRY_SLEEP: float = 5


def parse_page(offset=0, limit=100) -> bool:
    r = requests.get(sfdp_list_api_url.format(offset, limit))
    if r.status_code == 429:
        print('err while parsing page', r)
        time.sleep(RETRY_SLEEP)
        return parse_page(offset, limit)
    if r.status_code != 200:
        print('req err', r)
        return
    js = orjson.loads(r.text)
    for i in js['data']:
        validator_pubkey: str = i['mainnetBetaPubkey']
        if len(validator_pubkey) < 20: continue
        if validator_pubkey in rejected_validators: continue
        state: str = i['state']
        if state == 'Rejected':
            rejected_validators[validator_pubkey] = -1
    
    if len(js['data']) < 10: return False
    return True


def parse_validator(pubkey: str):
    if rejected_validators[pubkey] != -1: return
    
    r = requests.get(sfdp_validator_api_url.format(pubkey))
    if r.status_code == 429:
        print('err while parsing validator', r)
        time.sleep(RETRY_SLEEP)
        return parse_validator(pubkey)
    if r.status_code != 200:
        print('req err', r)
        return
    js = orjson.loads(r.text)
    # print(js)
    try: rejected_in = int(list(js['mnStats']['epochs'])[-1])
    except: rejected_in = 0
    rejected_validators[pubkey] = rejected_in
    
    
def load_list(filename: str):
    with open(filename, 'r') as f:
        for line in f.readlines()[1:]:
            identity, when_rejected = line.split()
            rejected_validators[identity] = int(when_rejected)
            
            
def write_list(filename: str, how_many=None):
    sorted_list = sorted(list(rejected_validators), key=lambda x: rejected_validators[x], reverse=True)
    if type(how_many) is int: sorted_list = sorted_list[:how_many]
    with open(filename, 'w') as f:
        f.write('INDETITY\t WHEN REJECTED (EPOCH)\n')
        for v in sorted_list:
            f.write('%s %d\n' % (v, rejected_validators[v]))


def iteration():
    t = time.time()
    load_list(LIST_PATH)
    
    limit = 100
    page_num = 0
    while 1:
        print('parsing page: %d' % page_num)
        if not parse_page(page_num*limit, limit): break
        page_num += 1

    with ThreadPoolExecutor(max_workers=1) as pool:
        for _ in pool.map(parse_validator, rejected_validators): pass

    write_list(LIST_PATH)
    write_list(SECOND_LIST_PATH, 1000)
    print('done in %ds' % (time.time() - t))
    push_to_github()


def push_to_github():
    dt = str(datetime.utcnow()).split('.')[0] + ' UTC'
    upd_badge = 'last ' + dt
    print(upd_badge)
    os.system('rm -f last*')
    os.system('touch "%s"' % upd_badge)
    os.system('git add .')
    os.system('git commit -m "upd"')
    os.system('git push')


def main():
    while 1:
        try: iteration()
        except KeyboardInterrupt: return
        except: print(format_exc())
        finally: time.sleep(3600 * 12)
        

if __name__ == '__main__':
    main()
    # iteration()
    # parse_validator('JCmdhNCyzypryaQUGLGxbCM366dScTD4tVy5ooSWyaBZ')
    # load_list()
    # push_to_github()

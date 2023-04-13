import requests
import click
import logging
import urllib3
from datetime import datetime, timedelta
import pytz
import json


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
log = logging.getLogger('prism-logs')


def save_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)


class PrismClient(requests.Session):
    def __init__(self):
        self.host = None
        self.port = 9440
        self.user = None
        self.password = None
        self.auth = None
        self.timezone = "UTC"
        self.base_url = None
        super().__init__()

    def init(self, host, port, user, password, verify=False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.auth = (self.user, self.password)
        self.verify = verify
        self.timezone = timezone
        self.base_url = "https://{}:{}/api/nutanix/v3/".format(self.host, self.port)

    def authenticate(self):
        r = self.get(self.base_url + "users/me")
        if r.status_code == 200:
            return True
        else:
            return False

    def get_logs(self, endpoint, attribute, start, end,):
        url = f'{self.base_url}{endpoint}s/list'
        start = int(datetime.timestamp(start) * 1000000)
        end = int(datetime.timestamp(end) * 1000000)
        payload = {
            'kind': endpoint,
            'length': 10,
            'offset': 0,
            'sort_order': 'DESCENDING',
            'sort_attribute': attribute,
            'filter': f'({attribute}=ge={start};{attribute}=lt={end})'
        }
        r = self.post(url, json=payload)
        offset = int(r.json()['metadata']['offset'])
        length = 100
        total = int(r.json()['metadata']['total_matches'])
        log.info(f'Got total of {total} of {endpoint}s')

        if total == 0:
            return []
        logs = []
        for x in range(offset, total, length):
            payload['offset'] = x
            payload['length'] = length
            r = self.post(url, json=payload)
            data = r.json().get('entities', [])
            log.info(f'Got {len(data)} {endpoint}s')
            logs.extend(data)

        return logs

    def get_logs_by_group(self, attribute, start, end):
        url = f'{self.base_url}groups'
        start = int(datetime.timestamp(start) * 1000000)
        end = int(datetime.timestamp(end) * 1000000)
        payload = {
            'entity_type': 'event',
            'query_name': f'eb:data-{str(datetime.timestamp(datetime.now())).split(".")[0]}',
            'group_count': 1,
            'group_offset': 0,
            'filter_criteria': f'({attribute}=ge={start};{attribute}=lt={end})',
            'group_attributes': [],
            'group_member_sort_attribute': attribute,
            'group_member_sort_order': 'DESCENDING',
            'group_member_count': 999,
            'group_member_offset': 0,
            'group_member_attributes': [
                {'attribute': 'title'},
                {'attribute': 'source_entity_name'},
                {'attribute': 'classification'},
                {'attribute': 'cluster'},
                {'attribute': '_created_timestamp_usecs_'},
                {'attribute': 'default_message'},
                {'attribute': 'param_name_list'},
                {'attribute': 'param_value_list'},
                {'attribute': 'source_entity_uuid'},
                {'attribute': 'source_entity_type'},
                {'attribute': 'operation_type'},
                {'attribute': 'info'}
            ],
        }

        r = self.post(url, json=payload)
        offset = 0
        length = 999
        total = int(r.json()['filtered_entity_count'])
        log.info(f'Got total of {total} events')
        if total == 0:
            return []
        logs = []
        for x in range(offset, total, length):
            payload['group_offset'] = x
            payload['group_member_offset'] = x
            r = self.post(url, json=payload)
            data = r.json().get('group_results', [])[0].get('entity_results', [])
            log.info(f'Got {len(data)} events')
            logs.extend(data)

        return logs


prism_client = PrismClient()
timezone = pytz.utc
start_time = None
end_time = None
output_file = None


@click.group()
@click.option('--username', '-u', prompt=True, help='Username for Prism Central')
@click.option('--password', '-p', prompt=True, hide_input=True, help='Password for Prism Central')
@click.option('--prism', '-pc', prompt=True, help='Prism Central IP or FQDN')
@click.option('--verify', '-v', default=False, help='Verify SSL certificate (default: False)')
@click.option('--port', '-port', default=9440, help='Prism Central Port (default: 9440)')
@click.option('--debug', '-d', default=False, help='Debug mode (default: False)')
@click.option('--logs_tz', '-tz', default='UTC', help='Logs timezone (default: UTC)')
@click.option('--start', '-s', default=None, help='Start time of logs in format YYYY-MM-DDTHH:MM:SS (default: 1 hour ago)')
@click.option('--end', '-e', default=None, help='End time of logs in format YYYY-MM-DDTHH:MM:SS (default: now)')
@click.option('--output', '-o', default=None, help='Output filename')
def main(username, password, prism, verify, port, debug, logs_tz, start, end, output):

    # configure logging
    global log
    if debug:
        log.setLevel(logging.DEBUG)
        log_level = logging.DEBUG
    else:
        log.setLevel(logging.INFO)
        log_level = logging.INFO

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)

    # configure prism client and test authentication
    prism_client.init(prism, port, username, password, verify)
    if prism_client.authenticate():
        log.info('Authentication successful')
    else:
        log.error('Authentication failed')
        exit(1)

    # configure timezone
    global timezone
    timezone = pytz.timezone(logs_tz)
    log.info('Timezone set to {}'.format(timezone))

    # configure filename
    global output_file
    if output:
        output_file = output
    else:
        output_file = f'logs-{int(datetime.now(timezone).timestamp()*1000000)}.json'

    # configure start and end time
    global start_time
    global end_time
    if start:
        try:
            start_time = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S')
            log.info('Start time: {}'.format(start_time))
        except ValueError:
            log.error('Invalid start time format. Use YYYY-MM-DDTHH:MM:SS')
            exit(1)
    else:
        start_time = datetime.now(timezone) - timedelta(hours=1)
        log.info('No start time defined, taking default 1 hour back: {}'.format(start_time))

    if end:
        try:
            end_time = datetime.strptime(end, '%Y-%m-%dT%H:%M:%S')
            log.info('End time: {}'.format(end_time))
        except ValueError:
            log.error('Invalid end time format. Use YYYY-MM-DDTHH:MM:SS')
            exit(1)
    else:
        end_time = datetime.now(timezone)
        log.info('No end time defined, taking default now: {}'.format(end_time))


@main.command()
def audits():
    log.info('Getting audits')
    logs = prism_client.get_logs('audit', 'op_start_timestamp_usecs', start_time, end_time)
    log.info(f'total log entries: {len(logs)}')
    save_json(logs, output_file)


@main.command()
def tasks():
    log.info('Getting tasks')
    logs = prism_client.get_logs('task', 'creation_time_usecs', start_time, end_time)
    log.info(f'total log entries: {len(logs)}')
    save_json(logs, output_file)


@main.command()
def alerts():
    log.info('Getting alerts')
    logs = prism_client.get_logs('alert', '_created_timestamp_usecs_', start_time, end_time)
    log.info(f'total log entries: {len(logs)}')
    save_json(logs, output_file)


@main.command()
def events():
    log.info('Getting alerts')
    logs = prism_client.get_logs_by_group('_created_timestamp_usecs_', start_time, end_time)
    log.info(f'total log entries: {len(logs)}')
    save_json(logs, output_file)

if __name__ == '__main__':
    main()

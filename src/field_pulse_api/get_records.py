import json
import requests
from dotenv import dotenv_values

config = dotenv_values(".env")

FIELD_PULSE_FULL_PATH = config['FIELD_PULSE_BASE_URL'] + config['FIELD_PULSE_BASE_PATH']

class GetRecords():

    def __init__(self) -> json:

        self.headers = {
            'x-api-key': config['FIELD_PULSE_API_KEY'],
            'Accept': 'application/json'
        }

    def api_request(self, record_type: str, limit: int, max_pages: int, **kwargs):
        """
        Query's FieldPulse API and returns a JSON file

        :param (string) record_type: Allowed values: customers, invoices, jobs, payments, purchase-orders, vendors
        :param (integer) limit: Maximum 100 items allowed
        :param (integer) max_pages: Maximum number of pages to fetch
        :param (string) sort_by: Allowed values: created_at, updated_at
        :param (string) sort_dir: Allowed values: asc, desc
        """

        self.print: bool = kwargs['print']
        self.record_type: str = record_type
        self.limit: int = limit
        self.max_pages: int = max_pages + 1

        if kwargs['sort_by']:
            self.sort_by: str = f"&sort_by={kwargs['sort_by']}"
        else:
            self.sort_by: str = ''
        
        if kwargs['sort_dir']:
            self.sort_dir: str = f"&sort_dir={kwargs['sort_dir']}"
        else:
            self.sort_dir: str = ''

        more_records: bool = True
        response: list = []
        api_page: int = 1

        while more_records:

            if api_page == self.max_pages:
                break

            if self.record_type != 'vendors':
                api_page_str: str = '&page=' + str(api_page)
                api_page += 1
            else:
                api_page_str: str = None
                api_page = self.max_pages

            api_request = requests.get(f'{FIELD_PULSE_FULL_PATH}/{self.record_type}?limit={self.limit}{self.sort_by}{self.sort_dir}{api_page_str}', headers=self.headers).json()
            
            if api_request["error"] == True:
                print('Error fetching data from API')
            else:
                response.extend(api_request['response'])

        if kwargs['print'] == True:
            print(response)
        else:
            print(response)
            return response

if __name__ == '__main__':
    GetRecords().api_request(record_type='customers', limit=2, sort_by='updated_at', sort_dir='asc', print=True) 
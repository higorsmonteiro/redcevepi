'''
    Define main API functionalities for a REDCap project.

    Author: Higor S. Monteiro
    Email: higormonteiros@gmail.com
    Date: 07 Fev 2023
'''
import os
import json
import requests

import pandas as pd
from collections import defaultdict

from redccievs.exceptions import *

class RedCaller:
    '''
    
    '''
    def __init__(self, api_token=None, api_host=None) -> None:
        
        self._api_token = api_token
        self.api_host = api_host
        self._schema = defaultdict(lambda: [])
        # self.api_host = 'http://172.16.220.94/redcap/redcap_v10.1.2/API/'

        if self._api_token is None:
            raise AccessError("API Token not provided.")
        if self.api_host is None:
            raise AccessError("API host not provided.")

        data = {
            'token': self._api_token,
            'content': 'metadata',
            'format': 'json',
        }
        req = requests.post(self.api_host, data=data)
        if req.status_code!=200:
            raise RequestError(f"Status code {req.status_code}")
        self._schema = [ (cur_dict['form_name'], cur_dict['field_name'], cur_dict['field_type'], cur_dict['field_label']) for cur_dict in req.json() ]
        self._schema = pd.DataFrame(self._schema, columns=["Nome Formulário", "Nome Variável", "Tipo Variável", "Nome do Campo"])
    
    '''
        --->
        -------> PROPERTIES
        --->
    '''
    @property
    def api_token(self):
        raise AccessError()

    @api_token.setter
    def api_token(self, x):
        raise AttributeError("This attribute is not mutable from outside.") 

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, x):
        raise AttributeError("This attribute is not mutable from outside.") 

    '''
        --->
        -------> EXPORT METHODS
        --->
    '''
    def export_forms(self, return_=True):
        '''
        
        '''
        data = {
            'token': self._api_token,
            'content': 'instrument',
            'format': 'json',
            'returnFormat': 'json'
        }
        req = requests.post(self.api_host, data=data)
        return req 

    def export_forms_pdf(self, output_folder, pdfname, record_id=None, 
                         instrument=None, all_records=None):
        '''

            Args:
            ----
                output_folder:
                    String. Absolute path to the output folder of the PDF file.
                pdfname:
                    String. Name of the generated PDF file with extension.
                record_id:
                    Integer.
                instrument:
                    String.
                all_records:
                    String or Integer.
        '''
        data = {
            'token': self._api_token,
            'content': 'pdf',
            'returnFormat': 'json',
            'allRecords': all_records,
            'record': record_id,
            'instrument': instrument
        }
        req = requests.post(self.api_host, data=data)
        if req.status_code!=200:
            raise RequestError()
        pdf_content = req.content

        f = open(os.path.join(output_folder, pdfname), 'wb')
        f.write(pdf_content)
        f.close()

    def export_metadata(self):
        '''
        
        '''
        data = {
            'token': self._api_token,
            'content': 'metadata',
            'format': 'json',
        }
        req = requests.post(self.api_host, data=data)
        return req

    def export_project_info(self):
        '''
        
        '''
        data = {
            'token': self._api_token,
            'content': 'project',
            'format': 'json',
        }
        req = requests.post(self.api_host, data=data)
        return req

    def export_records(self, records_arr=None, fields_arr=None, forms_arr=None, fmt_type='flat'):
        '''
        
        '''
        data = {
            'token': self._api_token,
            'content': 'record',
            'format': 'json',
            'type': fmt_type,
            'records': records_arr,
            'fields': fields_arr,
            'forms': forms_arr,
        }
        req = requests.post(self.api_host, data=data)
        return req

    def export_report(self, report_id=''):
        '''
            ### ---> TO UNDERSTAND AND TO FIX
        '''
        data = {
            'token': self._api_token,
            'content': 'report',
            'format': 'json',
            'report_id': report_id,
        }
        req = requests.post(self.api_host, data=data)
        return req

    def export_user(self):
        '''
        
        '''
        data = {
            'token': self._api_token,
            'content': 'user',
            'format': 'json',
        }
        req = requests.post(self.api_host, data=data)
        return req

    '''
        --->
        -------> IMPORT METHODS
        --->
    '''
    def import_record(self, records_data, update=False):
        '''
            Args:
            -----
            records_data:
                List of dictionaries. Each dictionary corresponds to a record to
                be imported containing a key-value association according to the
                variable names of the forms. 
            update:
                Boolean. If True, instead of adding a new record, updates an 
                existing record (all fields or only a subset of fields). 
        '''
        fmt_json = json.dumps(records_data)
        data = {
            'token': self._api_token,
            'content': 'record',
            'format': 'json',
            'type': 'flat',
            'overwriteBehavior': 'normal',
            'forceAutoNumber': 'false',
            'returnContent': 'ids',
            'data': fmt_json
        }
        if update:
            data['forceAutoNumber'] = 'false' # --> update
        
        req = requests.post(self.api_host, data=data)
        return req
    
    def delete_record(self, list_of_records):
        '''
            Remove records based on a provided list of record IDs.

            Args:
            -----
                list_of_records:
                    List. List containing the record IDs to be deleted from the project.

            Return:
            -------
                req:
                    requests.Response. Response triggered by the API call. 

        '''
        data = {
            'token': self._api_token,
            'content': 'record',
            'action': 'delete',
        }
        for n in range(len(list_of_records)):
            data.update({f'records[{n}]': f'{list_of_records[n]}'})

        req = requests.post(self.api_host, data=data)
        return req




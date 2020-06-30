#
# Basic interface with RESTfull APIs
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

import requests

class TdmBase:
    def __init__(self):
        self.url_prefix = "https://api.crossref.org"
        self.hdr = {
            'CR-Clickthrough-Client-Token': '',
             'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        }

    def get(self, api_url, params=None):
        if params == None:
            response = requests.get(api_url, headers=self.hdr)
        else:
            response = requests.get(api_url, headers=self.hdr, params=params)

        print("Response Status Code: " + str(response.status_code))
        return response

    def put(self, api_url, properties=None):
        if properties == None:
            response = requests.put(api_url, headers=self.hdr)
        else:
            response = requests.put(api_url, headers=self.hdr, json=properties)

        print("Response Status Code: " + str(response.status_code))
        return response

    def post(self, api_url, properties=None):
        if properties == None:
            response = requests.post(api_url, headers=self.hdr)
        else:
            response = requests.post(api_url, headers=self.hdr, json=properties)

        print("Response Status Code: " + str(response.status_code))
        return response

    def delete(self, api_url, properties=None):
        if properties == None:
            response = requests.delete(api_url, headers=self.hdr)
        else:
            response = requests.delete(api_url, headers=self.hdr, json=properties)

        print(response.status_code)
        return response




#
# Payload for turning a JSON into a python object
# @version 1.0
# @author Sergiu Buhatel <sergiu.buhatel@carleton.ca>
#

import json

class TdmPayload(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)
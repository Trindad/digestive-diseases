from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import optparse
import subprocess
import random

def filteringData():

    # LABELSS: user_id,age,sex,country,checkin_date,trackable_id,trackable_type,trackable_name,trackable_value
    pessoa = open("pessoa.csv","w")
    
    with open("fd-export.csv", "r") as f:
        for line in f:
            print(line,"\n\n")
    sys.stdout.flush()

if __name__ == "__main__":
    filteringData()

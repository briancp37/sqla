import json
import math

import pandas as pd

def tab(input, indent=4, color=None):
    color_dict = {'red':'\033[91m', 'green':'\033[92m', 'yellow':'\033[93m', 'blue':'\033[94m', 'pink':'\033[95m', 'teal':'\033[96m', 'grey':'\033[97m'}
    ret = ''
    if type(input)==str:
        ret = ' '*indent +input.replace('\n','\n'+' '*indent)
    elif (type(input) == pd.core.frame.DataFrame) or (type(input) == pd.core.frame.Series):
        ret = " "*indent + input.to_string().replace("\n", "\n"+" "*indent)
    elif type(input) in [str, float, list, dict, int]:
        ret = ' '*indent +str(input).replace('\n','\n'+' '*indent)
        # ret = ' '*indent + str(input)
    if (color != None):
        ret = color_dict[color]+ret
    return ret+"\033[0m"

def round_sig(x, sig=6):
    """ Rounds a float to a number of significant digits """
    return round(x, sig-int(math.floor(math.log10(abs(x))))-1)

def is_list_of_dicts(obj):
    if not isinstance(obj, list):
        return False
    return all(isinstance(item, dict) for item in obj)

def jsonify_dict(d_in):
    d_out = {}
    for key, value in d_in.items():
        try:
            json.dumps(value)
            d_out[key] = value
        except:
            d_out[key] = str(value)
    return d_out

def jsonify_records(records_arr):
    for i, d_in in enumerate(records_arr):
        for key, value in d_in.items():
            try:
                json.dumps(value)
                records_arr[i][key] = value
            except:
                records_arr[i][key] = str(value)
    return records_arr

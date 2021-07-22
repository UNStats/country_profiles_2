import re
import hashlib
import urllib3
import json
import csv
import pandas as pd
import sys
import numpy as np
import math


def round_KFM(x, n):
    '''Commercial Rounding'''
    posneg = math.copysign(1, x)
    z = abs(x)*10**n
    z = z + 0.5
    z = math.trunc(z)
    z = z/10**n
    result = z * posneg
    return result


def is_quasiConstant(x, cv_threshold):
    '''Is quasi constant?'''
    if len(x) == 1:
        return True
    elif all(x_i == x[0] for x_i in x):
        return True
    elif all(x_i > 0 for x_i in x) and cv(x) < cv_threshold:
        return True
    else:
        return False


def cv(x):
    '''Coefficient of variation'''
    return np.std(x) / np.mean(x)


def camel_case(st):
    """ Convert a string to camelCase. From:
    https://stackoverflow.com/questions/8347048/camelcase-every-string-any-standard-library

    """
    output = ''.join(x for x in st.title() if x.isalnum())
    return output[0].lower() + output[1:]


def camel_case_split(identifier):
    matches = re.finditer(
        '.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    x = ' '.join([m.group(0) for m in matches])
    return x


def numeric_part(value):
    ''' Extract numberic part of a data value.

    The regular expression to capture numeric values (including those in scientific notation) is:
    -?      # an optional -
    \d+     # a series of digits
    (?:     # start non capturing group
    \.    # a dot
    \d+   # a series of digits
    )?
    (?:     # start non capturing group
    e     # "e"
    -?    # an optional -
    \d+   # digits
    )?
    '''
    numeric_part_f = re.compile(r'-?\d+(?:\.\d+)?(?:e-?\d+)?')
    x = numeric_part_f.findall(value)
    if len(x) > 0:
        return float(x[0])
    else:
        return None


def tsv2dictlist(file, newline='', encoding='utf-8', errors='ignore'):
    '''Read a tab-delimited file and convert each record to a dictionary
    '''
    dictList = []

    with open(file, newline=newline, encoding=encoding) as f:
        x = csv.DictReader(f, delimiter='\t')
        for row in x:
            dictList.append(dict(row))
    return(dictList)


def dictLists2str(dictionary):
    '''Convert dictionary items that have lists as a value to comma-delimited strings
    '''
    for k, v in dictionary.items():
        if isinstance(v, list):
            dictionary[k] = re.sub('[\[\]\']', '', str(v))
    return dictionary


def dict2cols(dictionary):
    '''Separate dictionary items that have a dict as a value to individual items
    '''
    new_dict = {}
    for k, v in dictionary.items():
        if isinstance(v, dict):
            for k1, v1 in v.items():
                new_dict[k1] = v1
        else:
            new_dict[k] = v
    return new_dict


def dictList2tsv(dictList, outputfile):
    '''Write a dictList as a tab-spearated file.
       This is the opposite of tsv2dictList().
    '''
    keys = dictList[0].keys()
    with open(outputfile, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f,  keys, delimiter='\t',)
        dict_writer.writeheader()
        dict_writer.writerows(dictList)


def dictList2csv(dictList, outputfile):
    '''Write a dictList as a comma-spearated file.
    '''
    keys = dictList[0].keys()
    with open(outputfile, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f,  keys, delimiter=',',)
        dict_writer.writeheader()
        dict_writer.writerows(dictList)


def clean_str(v):
    return str(v).replace(u'\xa0', u' ').replace(u'\u0151', u'o').replace(u'\u2011', u'-').encode("utf-8").decode("utf-8").replace('\n', ' ').replace('\r', ' ').replace('  ', ' ').strip()


def xlsx2dict(file, sheet_name):
    '''
    Read a sheet form an excel file and convert each record to a dictionary
    '''
    x = pd.read_excel(file, sheet_name).to_dict('records')
    for r in x:
        for k, v in r.items():
            r[k] = clean_str(v)
    return x


def open_json(file):
    '''Open a local json file
    '''
    with open(file) as json_file:
        return json.load(json_file)


def dict_hash(d):
    '''Compute the hash of a dictionary. This is used to identify unique dictionaries.
    '''
    out = hashlib.md5()
    for key, value in d.items():
        out.update(repr(key).encode('utf-8'))
        out.update(repr(value).encode('utf-8'))
    return out.hexdigest()


def unique_dicts(dictionary_list):
    '''Get unique dictionaries in a list
    '''
    uniques_map = {}

    for d in dictionary_list:
        uniques_map[dict_hash(d)] = d

    return list(uniques_map.values())


def subdict_list(dict_list, keys_list, exclude=False):
    '''Extract subset of key-value pairs from each dictionary in a list
       Parameters:
         dict_list --> list of dictionaries
         keys_list --> list of keys to extract (or exclude, if 'exclude' is False)
    '''
    sub_d_list = []

    if exclude:
        for d in dict_list:
            sub_d = {k: d[k] for k in d.keys() if k not in keys_list}
            sub_d_list.append(sub_d)
    else:
        for d in dict_list:
            sub_d = {k: d[k] for k in keys_list}
            sub_d_list.append(sub_d)

    return sub_d_list


def select_dict(dict_list, d_kv, keep=True):
    '''Get all the dictionaries in a list that have a specific set of key-value pairs.
       d_kv is a dictionary of key-value pairs
    '''
    selected = []
    for d in dict_list:
        keys_list = list(d_kv.keys())
        value_list = list(d_kv.values())

        subdict = {k: d[k] for k in keys_list}
        if keep:
            if list(subdict.values()) == value_list:
                selected.append(d)
        else:
            if list(subdict.values()) != value_list:
                selected.append(d)
    return selected


def get_json_from_web(url, method='GET'):
    '''Call the endpoint of an API that produces a json message
    '''
    http = urllib3.PoolManager()
    response = http.request(method, url)
    responseData = json.loads(response.data.decode('UTF-8'))

    return responseData


def traverse_tree(tree, parentCode=None, parentName=None, itemCode='code', itemName='name', itemChildren='children', hierarchy=[], traverse_level=1):
    '''Traverse a hierarchical tree and convert it to a parent-child relationship/
       Tree has to have the following structure:
       tree = {
           'code' : <itemCode>,
           'name' : <itemName>,
           'children' : [
               {
                   'code' : <itemCode>,
                   'name' : <itemName>,
                   'children' : ...
               },
               {
                   'code' : <itemCode>,
                   'name' : <itemName>,
                   'children' : ...
               },
               ...
           ]
       }
    '''
    tree_keys = tree.keys()
    additional_keys = [x for x in tree_keys if x not in [
        itemChildren, itemCode, itemName]]

    d = {}
    d['level'] = traverse_level
    d['parentCode'] = parentCode
    d['parentName'] = parentName
    d[itemCode] = str(tree[itemCode])
    d[itemName] = tree[itemName]
    for ak in additional_keys:
        d[ak] = tree[ak]

    hierarchy.append(d)

    if tree[itemChildren]:
        for child in tree[itemChildren]:
            traverse_level += 1
            traverse_tree(child,
                          str(tree[itemCode]),
                          tree[itemName],
                          itemCode,
                          itemName,
                          itemChildren,
                          hierarchy,
                          traverse_level)
            traverse_level -= 1

    return hierarchy


def merge_dict_lists(dl1, dl2, left_on, right_on, how='inner'):
    '''Merge two lists of flat dictionaries as an inner or left-join query
       - left_on is a list with the keys of dl1 to be matched
       - right_on is a list with the values of dl2 to be matched
       - left_on and right_on must be the same length
       - how can be either 'inner' or 'left'
    '''

    merged_dict_list = []

    for d in dl1:
        sub_d = {k: d[k] for k in left_on}
        match = dict(zip(right_on, list(sub_d.values())))
        e_list = select_dict(dl2, match)

        for e in e_list:
            e_diff = {k: e[k] for k in e.keys() if k not in left_on}
            merged_dict_list.append({**d, **e_diff})

        if len(e_list) == 0 and how == 'left':
            e_diff = {k: None for k in dl2[0].keys() if k not in left_on}
            merged_dict_list.append({**d, **e_diff})

    return(merged_dict_list)


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".

    Source: https://stackoverflow.com/questions/3041986/apt-command-line-interface-like-yes-no-input
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def year_intervals(years_list):
    """ Find the coverage of an ordered list of years"""

    years_list = list(map(int, years_list))

    years_list.sort()

    n = len(years_list)

    start_y = list()
    end_y = list()

    start_y.append(years_list[0])

    if n > 1:
        for i in range(n-1):
            if(years_list[i+1] - years_list[i] > 1):
                start_y.append(years_list[i+1])
                end_y.append(years_list[i])

    end_y.append(years_list[n-1])

    interval_yy = list()

    for i in range(len(start_y)):

        if end_y[i] - start_y[i] > 0:
            interval_yy.append(str(start_y[i]) + '-' + str(end_y[i]))
        else:
            interval_yy.append(str(start_y[i]))

    x = ",".join(interval_yy)
    return(x)

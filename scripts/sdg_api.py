import utils
import math
import json


def geoAreaTree(rootCode=None):
    ''' Call the endpoint of the SDG API that provides the list of hierarchical groupings of geographic Areas.
        Note: The geoAreaTree object has various "trees" in it. We usually use the "World" tree; however, some 
        economic and geographic groupings are only in other trees.
    '''

    http = utils.urllib3.PoolManager()
    response = http.request(
        'GET', "https://unstats.un.org/SDGAPI/v1/sdg/GeoArea/Tree")
    responseData = utils.json.loads(response.data.decode('UTF-8'))

    if rootCode:
        return utils.select_dict(responseData, {'geoAreaCode': rootCode})
    else:
        return responseData


def sdg_tree():
    '''Call the endpoint of the SDG API that provides the list of goals with all their children
    '''
    http = utils.urllib3.PoolManager()
    response = http.request(
        'GET', "https://unstats.un.org/SDGAPI/v1/sdg/Goal/List?includechildren=true")
    responseData = utils.json.loads(response.data.decode('UTF-8'))

    return responseData


def series_request_details(seriesCode, release):
    '''Verify how many pages need to be requested to get all the data for a specific series from the SDG API.
    '''

    seriesRequest = 'https://unstats.un.org/SDGAPI/v1/sdg/Series/Data?seriesCode=' + \
        seriesCode + '&releaseCode=' + release + "&pageSize=2"

    print(seriesRequest)

    responseData = utils.get_json_from_web(seriesRequest, method='GET')

    pageSize = 850
    nPages = math.floor(responseData['totalElements'] / pageSize) + 1
    totalElements = responseData['totalElements']

    return {'series': seriesCode,
            'totalElements': totalElements,
            'nPages': nPages,
            'pageSize': pageSize
            }


def series_code_lists(seriesCode, release):
    '''Explore the code lists of the attributes and dimensions of a series:
       Describe each attribute or dimension as a simple dictionary made of a set of code-description pairs. 
       For the code, use the SDMX code, and not the internal code of the database. 
       Keep all labels in camelCase.
    '''
    seriesRequest = 'https://unstats.un.org/SDGAPI/v1/sdg/Series/Data?seriesCode=' + \
        seriesCode + '&releaseCode=' + release + "&pageSize=2"

    responseData = utils.get_json_from_web(seriesRequest, method='GET')

    series_attributes = responseData['attributes']
    series_dimensions = responseData['dimensions']

    concepts = []

    for d in series_dimensions:

        new_dict = {}
        new_dict['concept'] = utils.camel_case(d['id'])
        new_dict['role'] = 'dimension'
        new_dict['codes'] = []
        for c in d['codes']:
            new_dict2 = {}
            new_dict2['code'] = c['code']
            new_dict2['sdmx'] = c['sdmx']
            new_dict2['description'] = c['description']
            new_dict['codes'].append(new_dict2)
        concepts.append(new_dict)

    for a in series_attributes:

        new_dict = {}
        new_dict['concept'] = utils.camel_case(a['id'])
        new_dict['role'] = 'attribute'
        new_dict['codes'] = []
        for c in a['codes']:
            new_dict2 = {}
            new_dict2['code'] = c['code']
            new_dict2['sdmx'] = c['sdmx']
            new_dict2['description'] = c['description']
            new_dict['codes'].append(new_dict2)
        concepts.append(new_dict)

    return concepts


# ----------------------------------------

def series_data(seriesCode, release):
    '''collect data for a specific series from the global SDG API
    '''
    x = series_request_details(seriesCode, release)
    series_data = []
    if x['totalElements'] > 0:
        for p in range(x['nPages']):
            print("---Series " + seriesCode + ": Processing page " +
                  str(p+1) + " of " + str(x['nPages']))
            queryString = r'https://unstats.un.org/SDGAPI/v1/sdg/Series/Data?seriesCode=' + \
                seriesCode + '&releaseCode=' + release + '&page=' + \
                str(p+1) + '&pageSize=' + str(x['pageSize'])
            responseData = utils.get_json_from_web(queryString, method='GET')
            if len(responseData['data']) > 0:
                series_data = series_data + responseData['data']
    return series_data

# -------------------------------------------


def seriesData2tsv(seriesCode, release):
    '''Extract data from the SDG_API pertaining to a specific series code / release and
       save it as a set of tab-delimited files, one for each reference area.
       Notes:
       - Since the SDG API call returns duplicate records in case of "multipurpose" indicators,
         this script first removes the goal/target/indicator fields and then extract unique
         records. 
       - In the original response, each record has a pair of items called "dimensions" and "attributes".
         Each of these items contains a sub-dictionary with a set of name-code pairs. The script
         also "flattens" the structure, converting the items of each nested subdictionary to new
         "fields" of the output table.
    '''
    # Call the API to obtain the data for <seriesCode> and <release>, remove the
    # goal/target/indicator/seriesCount fields, and remove duplicates present in the case of
    # multi-purpose indicators:

    x_data = utils.unique_dicts(utils.subdict_list(series_data(seriesCode, release),
                                                   ['goal', 'target',
                                                       'indicator', 'seriesCount'],
                                                   exclude=True))

    # print(x_data)

    x_geoAreas = utils.unique_dicts(utils.subdict_list(
        x_data, ['geoAreaCode'], exclude=False))

    x_data2 = []

    # Flatten the dictionary, creating a column for each item in each nested dictionary:
    for r in x_data:
        x_data2.append(utils.dict2cols(r))

    # Split the dataset by geographic reference area and save as tab-delimited file:
    for g in x_geoAreas:

        file_name = 'data/raw/' + release + '/Series_' + \
            seriesCode + '_RefArea_' + g['geoAreaCode'] + '.txt'

        utils.dictList2tsv(utils.select_dict(
            x_data2, {'geoAreaCode': g['geoAreaCode']}), file_name)

        #print('  created file ' + file_name)
    print(f'------finished processing series {seriesCode}')


def flat_series_data(seriesCode, release):
    ''' DEPRECATED
    '''

    codeLists = series_code_lists(seriesCode, release)
    new_x = []
    for d in series_data(seriesCode, release):
        new_d = {}
        for key, value in d.items():
            if type(value) is list:
                new_d[key] = ', '.join(value)
            elif type(value) is dict:
                for k, v in value.items():
                    new_d[utils.camel_case(k+' Code')] = v
                    for cl in codeLists:
                        if cl['concept'] == utils.camel_case(k):
                            for c in cl['codes']:
                                if c['code'] == v:
                                    new_d[utils.camel_case(k+' Desc')
                                          ] = c['description']
                                    new_d[utils.camel_case(
                                        k+' Code')] = c['sdmx']
                                    break
                            break
            elif key == 'time_detail':
                new_d[utils.camel_case(key)] = value
            elif key == 'timePeriodStart':
                new_d['timePeriod'] = int(value)
            elif key == 'series':
                new_d['seriesCode'] = value
            elif key == 'seriesDescription':
                new_d['seriesDesc'] = value
            elif key == 'geoAreaCode':
                new_d['geoAreaCode'] = str(value).zfill(3)
            else:
                new_d[key] = value

        new_d['value_numeric_part'] = utils.numeric_part(new_d['value'])
        new_d['value_is_censored'] = (new_d['valueType'] != 'Float')
        new_d['value_detail'] = new_d['value']

        del new_d['value']
        del new_d['valueType']
        del new_d['seriesCount']

        new_x.append(new_d)

    return new_x


# -----------------------------

def series_data_to_json(seriesCode, release):
    '''Produce 'long' files for each indicator/series combination
    (Notice that multi-purpose indicators are split in multiple files)
    DEPRECATED
    '''

    x = flat_series_data(seriesCode, release)

    indicator_series = utils.unique_dicts(utils.subdict_list(x, ['goal', 'target', 'indicator', 'seriesCode', 'seriesDesc'])
                                          )
    geoAreaTree_flat = utils.traverse_tree(geoAreaTree(1)[0],
                                           parentCode=None,
                                           parentName=None,
                                           itemCode='geoAreaCode',
                                           itemName='geoAreaName')

    for s in indicator_series:
        d = s.copy()
        d['release'] = release
        indicator = d['indicator']
        data = utils.select_dict(x, {'indicator': indicator})
        d['refAreas'] = geoAreaTree_flat
        for g in d['refAreas']:
            g_data = utils.subdict_list(utils.select_dict(data, {'geoAreaCode': g['geoAreaCode']}),
                                        ['goal', 'target', 'indicator', 'seriesCode',
                                         'seriesDesc', 'geoAreaCode', 'geoAreaName'],
                                        exclude=True)
            g['data'] = g_data

        file_name = release + '/Indicator_' + indicator + \
            '_Series_' + d['seriesCode'] + '.json'

        with open(r'data/raw/' + file_name, 'w') as f:
            json.dump(d, f, indent=4)

        print('created file ' + file_name)

# ------------------------------

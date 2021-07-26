# --------------------------------------------------------
# This script will download basic data from the SDG API
#---------------------------------------------------------

# Import libraries and modules

import utils
import json
import math
import pandas as pd

http = utils.urllib3.PoolManager()

# Release:
release = '2021.Q2.G.03'

remove_keys = ['goal','target','indicator',
                'series','seriesDescription','seriesCount',
                'units', 'reporting_type']


remove_keys2 = ['Management Level','Observation Status','Geo Info Type']


#================================================================================
# Get list of all available series in the latest release
#================================================================================

response = http.request(
    'GET', "https://unstats.un.org/SDGAPI/v1/sdg/Series/List?allreleases=false")
responseData = utils.json.loads(response.data.decode('UTF-8'))

#================================================================================
# Get flat list of SDG indicators
#================================================================================

sdgList = []
for s in responseData:
    for idx, g in enumerate(s['goal']):
        d = dict()
        d['release'] = s['release']
        d['goal'] = g
        d['target'] = s['target'][idx]
        d['indicator'] = s['indicator'][idx]
        d['series'] = s['code']
        d['series_desc'] = s['description']
        sdgList.append(d)

# print(sdgSeries[0:3]) 
with open('data/tests/sdgSeries.json', 'w') as fout:
    json.dump(sdgList , fout, indent=4)

#================================================================================
# Get hierarchical tree of Goal, Target, Indicator, Series
#================================================================================

response = http.request(
    'GET', "https://unstats.un.org/SDGAPI/v1/sdg/Goal/List?includechildren=true")
sdgTreeData = utils.json.loads(response.data.decode('UTF-8'))

# Note: All releases are returned, need to filter only to the latest one


# Get rid of old releases, and get rid of uneccesary fields.

sdgTree = []

for g in sdgTreeData:

    d_g = dict()
    d_g['code'] = g['code']
    d_g['title'] = g['title']
    d_g['description'] = g['description']
    
    d_g['targets'] = []

    for t in g['targets']:
        d_t = dict()
        d_t['code'] = t['code']
        d_t['description'] = t['description']
        d_t['indicators'] = []

        for i in t['indicators']:

            d_i = dict()
            d_i['code'] = i['code']
            d_i['description'] = i['description']
            d_i['tier'] = i['tier']
            d_i['series'] = []

            for s in i['series']:
                if s['release'] == release:
                    d_s = dict()
                    d_s['code'] = s['code']
                    d_s['description'] = s['description']
                    d_i['series'].append(d_s)
            
            d_t['indicators'].append(d_i)

        d_g['targets'].append(d_t)
    
    sdgTree.append(d_g)



with open('data/tests/sdgTree.json', 'w') as fout:
    json.dump(sdgTree , fout, indent=4)



#================================================================================
# Get series data (pivoted), one series at a time
#================================================================================

for g_idx, g in enumerate(sdgTree):

    if g_idx !=0:
        continue

    for t_idx, t in enumerate(g['targets']):

        if t_idx !=0:
            continue

        for i_idx, i in enumerate(t['indicators']):
    
            if i_idx !=0:
                continue

            for s_idx, s in enumerate(i['series']):

                # Get number of 'time series' for this series:
                 
                response = http.request(
                    'GET', "https://unstats.un.org/SDGAPI/v1/sdg/Series/PivotData?seriesCode="+s['code']+"&releaseCode="+release+"&page=1&pageSize=1")
                responseData = utils.json.loads(response.data.decode('UTF-8'))

                numberOfTimeSeries = responseData['totalElements']

                # Now that we now how many time series there are in a series, we can calculate how many
                # API requests we need to make in order to get all the data in multiple pages

                pageSize = 500
                numberOfPages = math.ceil(numberOfTimeSeries / pageSize)

                # print(f"{numberOfTimeSeries=}")
                # print(f"{pageSize=}")
                # print(f"{numberOfPages=}")

                # Get data by page, and put them all together

                data = []

                for p in range(numberOfPages):
                    
                    response = http.request(
                        'GET', "https://unstats.un.org/SDGAPI/v1/sdg/Series/PivotData?seriesCode="+s['code']+"&releaseCode="+release+"&page="+str(p+1)+"&pageSize="+str(pageSize))
                    responseData = utils.json.loads(response.data.decode('UTF-8'))
                    
                    if p == 0:
                        attributes = responseData['attributes']
                        dimensions = responseData['dimensions']

                    data.extend(responseData['data'])
                      
                # with open('data/tests/response_attributes.json', 'w') as fout:
                #     json.dump(attributes , fout, indent=4)
                # with open('data/tests/response_dimensions.json', 'w') as fout:
                #     json.dump(dimensions , fout, indent=4)
                # with open('data/tests/response_data.json', 'w') as fout:
                #     json.dump(data , fout, indent=4)

                
                series_data = []



                for sd in data:
                    # Remove empty fields, as well as unnecessary metadata:

                    timeSeries = {i:sd[i] for i in sd if i not in remove_keys }   
                    timeSeries = {i:timeSeries[i] for i in timeSeries if timeSeries[i] } 

                    # Re-code dimensions in data using SDMX codes:

                    new_ts = dict()
                    for k,v in timeSeries.items():

                        new_ts[k] = v

                        for d in dimensions:
                            if d['id'].lower()!= k:
                                continue  
                           #print(d['id'].lower())
                            
                            for code in  d['codes']:
                                if code['code'] != v:
                                    continue
                                #print(f"{v=}")
                                new_ts[k+'_code'] = code['sdmx']
                                del(new_ts[k])
                                new_ts[k+'_desc'] = code['description']

                                #print(f"{v=}")
                                #print('--------')
                        
                    timeSeries = new_ts

                    # Parse "years" into a list of dictionaries:

                    years = json.loads( timeSeries['years']) 
                    
                    timeSeries['years'] = [i for i in years if i['value']]  
                    
                    y2 = []
                    for y in timeSeries['years']:
                        y['year'] = int(y['year'].replace('[','').replace(']',''))
                        y2.append({i:y[i] for i in y if i not in remove_keys2})

                    
                    
                    timeSeries['years'] = y2

                    # Recode attributes usind SDMX codes:
                    new_years = []
                    for y in timeSeries['years']:
                        y_new = dict()
                        for k,v in y.items():
                            y_new[k] = v
                            for a in attributes:
                                if a['id']!= k:
                                    continue  
                                
                                for code in  a['codes']:
                                    if code['code'] != v:
                                        continue
                                    #print(f"{v=}")
                                    y_new[k+'_code'] = code['sdmx']
                                    del y_new[k]
                                    y_new[k+'_desc'] = code['description']
                                    #print(f"{v=}")
                                    #print('--------')
                        new_years.append(y_new)
                    timeSeries['years'] = new_years
                    #To do: define time series keys and id's

                    # timeSeries_keys = [i.replace('_code','') for i in list(timeSeries.keys()) and i.endswith('_code')]
                    print(f"{list(timeSeries.keys())=}")
                    
                    timeSeries_keys = []
                    for tsk in list(timeSeries.keys()):
                        if tsk.endswith('_code'):
                            timeSeries_keys.append(tsk.replace('_code',''))

                    timeSeries_id =   '__'.join([s['code']] + [timeSeries[k+'_code'] for k in timeSeries_keys])
                    timeSeries_keys = '__'.join(timeSeries_keys)
                    
                    timeSeries['timeSeries_id']=timeSeries_id
                    timeSeries['timeSeries_keys']=timeSeries_keys

                    series_data.append(timeSeries)

                with open('data/tests/'+s['code']+'_attributes.json', 'w') as fout:
                    json.dump(attributes , fout, indent=4)
                with open('data/tests/'+s['code']+'_dimensions.json', 'w') as fout:
                    json.dump(dimensions , fout, indent=4)
                with open('data/tests/'+s['code']+'_data.json', 'w') as fout:
                    json.dump(series_data , fout, indent=4)


                # TO DO:
                #
                # - Add descriptions
                # - Save to flat csv file





                    

                    









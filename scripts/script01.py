# --------------------------------------------------------
# This script will download basic data from the SDG API
#---------------------------------------------------------

# Import libraries and modules

import utils
import json
import math

http = utils.urllib3.PoolManager()

# Release:
release = '2021.Q2.G.03'

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

                remove_keys = ['goal','target','indicator','series','seriesDescription','seriesCount' ]

                for sd in data:
                    # Remove empty fields, as well as unnecessary metadata:

                    timeSeries = {i:sd[i] for i in sd if i not in remove_keys }   
                    timeSeries = {i:timeSeries[i] for i in timeSeries if timeSeries[i] } 

                    # Parse "years" into a list of dictionaries:

                    years = json.loads( timeSeries['years']) 
                    
                    timeSeries['years'] = [i for i in years if i['value']]  

                    for y in timeSeries['years']:
                        y['year'] = int(y['year'].replace('[','').replace(']',''))

                    series_data.append(timeSeries)

                with open('data/tests/series_data.json', 'w') as fout:
                    json.dump(series_data , fout, indent=4)





                    

                    









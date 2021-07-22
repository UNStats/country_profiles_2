# --------------------------------------------------------
# This script will download basic data from the SDG API
#---------------------------------------------------------

# Import libraries and modules

import utils
import json

http = utils.urllib3.PoolManager()

# Get list of all available series in the latest release
response = http.request(
    'GET', "https://unstats.un.org/SDGAPI/v1/sdg/Series/List?allreleases=false")
responseData = utils.json.loads(response.data.decode('UTF-8'))

#-------------------------------------------------------------------------------
# Get the SDG indicator framework as a tree (with repeated indicators/series)
#
# - Release
#   - Goal
#     - Target
#       - Indicator
#         - Series (series code, series description)
#-------------------------------------------------------------------------------

sdgSeries = []
for s in responseData:
    for idx, g in enumerate(s['goal']):
        d = dict()
        d['release'] = s['release']
        d['goal'] = g
        d['target'] = s['target'][idx]
        d['indicator'] = s['indicator'][idx]
        d['series'] = s['code']
        d['series_desc'] = s['description']
        sdgSeries.append(d)

print(sdgSeries[0:3]) 

with open('data/tests/sdgSeries.json', 'w') as fout:
    json.dump(sdgSeries , fout, indent=4)

#--------------------------------------------------------------------------------
# Create an ordered tree:  Goal, Target, Indicator, Series
#--------------------------------------------------------------------------------

# Get list of unique goals (ordered)
# For each goal, get list of unique targets (ordered)
# For each target, get list of unique indicators (ordered)
# For each indicator, get list of unique series (ordered)

# [ 
#     "release": "2021.Q2.G.03",
#     "goals": [
#         {
#             "code": '1'
#             "targets" : [
#                 {
#                     "code" : '1.1'
#                     "indicators" : [
#                         {
#                             "code": '1.1.1',
#                             "series": [
#                                 {
#                                     "code": 'SI_POV_DAY1',
#                                     "description": 'Proportion of population below international poverty line (%)'
#                                 },                     
#                                 {
#                                     "code": 'SI_POV_EMP1',
#                                     "description": 'Employed population below international poverty line, by sex and age (%)'
#                                 }
#                             ]
#                         },
#                         {
#                             "code": '1.1.2',
#                             "series": [..]
#                         }
#                     ]
#                 }
#             ]
#         }
#     ]
# ]
        





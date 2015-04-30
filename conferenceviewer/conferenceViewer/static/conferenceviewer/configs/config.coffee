angular.module('ConferenceViewerAPIRoutes', [])
    .value('conferencesAPI', '/api/conferences/')
    .value('errorMsg', "API not found")

angular.module('StatsAPIRoutes', [])
    .value('statsAPI', '/api/stats/')
    .value('errorMsg', "API not found")

angular.module('ConferenceViewerURL', [])
    .value('ConfDetailURL', '/conference/detail/')

angular.module('GLOBAL_CONSTANTS', [])
    .value('audioQOSThresholds', {'rtt_warn': 200, 'rtt_error': 500, 'jitter_warn': 20, 'jitter_error': 40, 'pl_warn': 3, 'pl_error': 6})
    .value('fn_loc_latlng', {'SIN': {lat: +1.36670, lng: +103.75000}, 'AMS': {lat: +52.35000, lng: +4.86660}, 'NJE': {lat: +40.73050, lng: -74.06560}, 'SJO': {lat: +37.33780, lng: -121.89000}, 'SOF': {lat: +42.71670, lng: +23.33330}, 'SFO': {lat: +37.77750, lng: -122.41100}, 'SYD': {lat: -33.8600, lng: +151.2094}})

angular.module('Debug', [])
    .value('testMode', false)

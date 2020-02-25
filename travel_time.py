import os
import json
import statistics
import googlemaps

import util
import rmv_constants

gmaps_key = os.environ['GMAPS_KEY']
gmaps = googlemaps.Client(key=gmaps_key)


def get_commute_times(properties: [dict], destinations: [str]):
    distance_matrix = {}
    modes = ['transit', 'walking', 'bicycling']
    origins = [[x[rmv_constants.RmvPropDetails.geo_lat.name], x[rmv_constants.RmvPropDetails.geo_long.name]]
               for x in properties]

    for mode in modes:
        chunk_sizes = util.chunks(origins, rmv_constants.GMAPS_DISTANCE_MATRIX_MAX_ORIGINS)
        # distance_matrix[mode] = {}
        for chunk in chunk_sizes:
            if mode not in distance_matrix:
                distance_matrix[mode] = gmaps.distance_matrix(origins[chunk[0]:chunk[1]],
                                                              destinations, units='metric', mode=mode, region='uk')
            else:
                temp = gmaps.distance_matrix(origins[chunk[0]:chunk[1]],
                                             destinations, units='metric', mode=mode, region='uk')
                distance_matrix[mode]['origin_addresses'].extend(temp['origin_addresses'])
                distance_matrix[mode]['rows'].extend(temp['rows'])

    for k, v in distance_matrix.items():
        for idx, val in enumerate(v['rows']):
            if 'augment' not in properties[idx]:
                properties[idx]['augment'] = {}
            travel_times = [{dest: {k: (val['elements'][i]['duration']['value']) / 60}}  # in minutes
<<<<<<< HEAD
=======
                            if 'duration' in val['elements'][i] else None
>>>>>>> 2ba4382... Fix temporary patch by moving logic to handle more than 25 origins for GMAPS API inside the function to make transparent to user
                            for i, dest in enumerate(destinations)]
            if 'travel_time' not in properties[idx]['augment']:
                properties[idx]['augment']['travel_time'] = travel_times
            else:
                for i, data in enumerate(properties[idx]['augment']['travel_time']):
                    [properties[idx]['augment']['travel_time'][i][k].update(travel_times[i][k]) for k in data.keys()]

            properties[idx]['avg_travel_time_{}'.format(k)] = (statistics.mean([v[k] for x in travel_times
                                                                                for v in x.values()]))


def get_property_zone(property_dict: dict):
    location = (property_dict[rmv_constants.RmvPropDetails.geo_lat.name],
                property_dict[rmv_constants.RmvPropDetails.geo_long.name])
    r = gmaps.places_nearby(location=location, radius=1200, type='subway_station')

    with open('tube_stations_zone_mapping.json') as f:
        station_zones = json.load(f)

    # find zone of all stations nearby as returned by Google Maps
    nearby_stations_zones = [{k['name']: k['zone']} for each in r['results']
                             for i, k in enumerate(station_zones)
                             if each['name'].replace("Station", '').replace("Underground", '').strip() == k['name']]

    if 'augment' not in property_dict:
        property_dict['augment'] = {}

    property_dict['augment']['nearby_station_zones'] = nearby_stations_zones

    # best guess based on simple average of zones
    if nearby_stations_zones:
        property_dict['zone_best_guess'] = round(statistics.mean([int(v.split(",")[-1]) for x in nearby_stations_zones
                                                                  for v in x.values()]))
    else:
        property_dict['zone_best_guess'] = None

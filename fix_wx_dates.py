import time
import os
import json
import math
import mbd_util as u

file_dir = '/home/s1638696/flight_data/wx_data/'
out_file_dir = '/home/s1638696/flight_data/wx_data_fixed/'

for file in os.listdir(file_dir):
	date_of_file = file[0:8]
	sigmets =json.load(open(file_dir+file))
	for s in sigmets['features']:
		try:
			v_from, v_to = s['properties']['rawSigmet'].split(' VALID ')[1][0:14].split('/')
		except ValueError:
			print('Unable to parse sigmet: \n')
			print(s['properties']['rawSigmet'])
			del s
			continue
		try:
			if v_from[0:2] == date_of_file[-2:]:
				date_from = date_of_file # Valid on day
			elif v_from[2:4] > '12':
				date_from = u.add_1_day(date_of_file) # not valid and is in afternoon (+1day)
			else:
				date_from = u.sub_1_day(date_of_file) # not valid and is in morning (-1day)
	
			if v_to[0:2] == date_of_file[-2:]:
				date_to = date_of_file # Valid on day
			elif v_to[2:4] > '12':
				date_to = u.add_1_day(date_of_file) # not valid and is in afternoon (+1day)
			else:
				date_to = u.sub_1_day(date_of_file) # not valid and is in morning (-1day)

			v_from = date_from[0:4]+'-'+date_from[4:6]+'-'+date_from[6:8]+'T'+v_from[2:4]+':'+v_from[4:6]+':00Z'
			v_to = date_to[0:4]+'-'+date_to[4:6]+'-'+date_to[6:8]+'T'+v_to[2:4]+':'+v_to[4:6]+':00Z'
			s['properties']['v_from'] = u.wx_json_2_timestamp(v_from)
			s['properties']['v_to'] = u.wx_json_2_timestamp(v_to)
		except ValueError:
			print('Error with time ' + v_from + ' ' + v_to + ' in sigmet: ')
			print(s['properties']['rawSigmet'])
			del s
			continue
	with open(out_file_dir + file, 'w') as outfile:
		json.dump(sigmets, outfile)
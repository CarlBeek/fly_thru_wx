import math
import datetime

block_size = 0.5

def block_name(lat, lon):
    discretized_lat = (math.floor(lat/block_size)+0.5)*block_size
    discretized_lon = (math.floor(lon/block_size)+0.5)*block_size
    return (discretized_lat, discretized_lon)

def inside_polygon(x, y, points):
    """
    Return True if a coordinate (x, y) is inside a polygon defined by
    a list of verticies [(x1, y1), (x2, x2), ... , (xN, yN)].

    Reference: http://www.ariel.com.au/a/python-point-int-poly.html
    """
    n = len(points)
    inside = False
    p1x, p1y = points[0]
    for i in range(1, n + 1):
        p2x, p2y = points[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside

def get_covered_blocks(polygon):
	if polygon[0] != polygon[-1]:
		polygon.append(polygon[0])
	lats = [pos[0] for pos in polygon]
	max_lat = max(lats)
	min_lat = min(lats)
	longs = [pos[1] for pos in polygon]
	max_long = max(longs)
	min_long = min(longs)

	max_block = block_name(max_lat, max_long)
	min_block = block_name(min_lat, min_long)

	covered_blocks = []
	for lat_i in range(int((max_block[0] - min_block[0])/block_size)):
		for long_i in range(int((max_block[1] - min_block[1])/block_size)):
			la, lo = min_block[0] + lat_i * block_size, min_block[1] + long_i * block_size
			if inside_polygon(la, lo, polygon):
				covered_blocks.append((la, lo))
	return covered_blocks


def add_1_day(string):
	new_date = datetime.datetime.strptime(string, "%Y%m%d") + datetime.timedelta(days = 1)
	return datetime.datetime.strftime(new_date, '%Y%m%d')

def sub_1_day(string):
	new_date = datetime.datetime.strptime(string, "%Y%m%d") - datetime.timedelta(days = 1)
	return datetime.datetime.strftime(new_date, '%Y%m%d')

def wx_json_2_timestamp(string):
	return int(datetime.datetime.strftime(datetime.datetime.strptime(string, "%Y-%m-%dT%H:%M:%SZ"),'%s'))* 1000

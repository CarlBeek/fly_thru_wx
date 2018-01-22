from mpl_toolkits.basemap import Basemap, cm
import numpy as np
import matplotlib.pyplot as plt
import json

with open("ryan_prob.json", "r") as f:
    j = json.load(f)

l = map(lambda r: [r['_1']['_1'], r['_1']['_2'], r['_2']], j)
#l = filter(lambda e: e[0] > 34 and e[0] < 71 and e[1] > -26 and e[1] < 56, l)
nl = np.array(list(l))

print(nl[:, 2].min())

nl[:, 2] = np.log(nl[:, 2])
mn = np.min(nl, axis=0)
mx = np.max(nl, axis=0)
lats = np.arange(mn[0], mx[0]+0.5, 0.5)
longs = np.arange(mn[1], mx[1]+0.5, 0.5)
mat = np.ones((lats.shape[0], longs.shape[0]), np.float32)*mn[2]
indices_lat = ((nl[:, 0] - mn[0])/0.5).astype(np.int32)
indices_long = ((nl[:, 1] - mn[1])/0.5).astype(np.int32)
xx, yy = np.meshgrid(longs, lats)
mat[indices_lat, indices_long] = nl[:, 2]
fig = plt.figure()

#m = Basemap(projection='merc', llcrnrlat=35, llcrnrlon=-25, urcrnrlat=70, urcrnrlon=55, lat_ts=1)
m = Basemap(projection='merc', llcrnrlat=-89, llcrnrlon=-180, urcrnrlat=89, urcrnrlon=180, lat_ts=1)
m.drawcoastlines()
m.drawstates()
m.drawcountries()
#m.drawparallels(np.arange(36.,70.,.5))
#m.drawmeridians(np.arange(-16.,45.,.5))

print(mat.max())
xx, yy = m(xx, yy)
m.pcolormesh(xx, yy, mat)

plt.savefig("/home/gerben/plot.png")
plt.title("Stuff")
plt.show()


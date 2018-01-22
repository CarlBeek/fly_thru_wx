import numpy as np
import matplotlib.pyplot as plt


def deg2rad(deg):
    return deg*math.pi/180

def rad2deg(rad):
    return rad/math/pi/180

def to_gnomic(lat, long, lat0 = 0, long0 = 0):
    lam = deg2rad(lat)
    phi = deg2rad(long)
    lam0 = deg2rad(lat0)
    phi0 = deg2rad(long0)

    denom = math.sin(phi)*math.sin(phi0)+math.cos(phi)*math.cos(phi0)*math.cos(lam-lam0)
    
    x = (math.cos(phi)*math.sin(lam-lam0))/denom
    y = (math.cos(phi0)*math.sin(phi)-math.sin(phi0)*math.cos(phi)*math.cos(lam-lam0))/denom
    
    R = 6371 #radius of earth in km
    x = R*math.atan2(x, R)
    y = R*math.atan2(y, R)
    return x, y

def forier_transform(np_arr_of_probs):
    fft = np.fft.fft2(np_arr_of_probs)
    return np.absolute(fft)

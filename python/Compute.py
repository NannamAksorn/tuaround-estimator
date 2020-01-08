import numpy as np

EARTH_RADIUS = 6377170.0


def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.    

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371000 * c
    return km


def euclidean_dist(lon1, lat1, lon2, lat2):
    dx = lon2 - lon1
    dy = lat2 - lat1
    return 108550 * np.sqrt(dx * dx + dy * dy)


def bearing_compute(lon1, lat1, lon2, lat2):
    """compute bearing between lon1,lat1 and lon2,lat2
    Returns:
    float: degree bearning (north = 0, east = 90, south = 180, west = 270)
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    brng = np.arctan2(x, y)
    brng = np.degrees(brng)
    brng = (brng + 360) % 360
    return brng


def angle_compute(lon1, lat1, lon2, lat2):
    """compute estimate bearing"""
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    brng = np.arctan2(dlon, dlat)
    brng = np.degrees(brng)
    brng = (brng + 360) % 360
    return brng


def bearing(p1, p2):
    """Bearing between point 1 and point 2"""
    return bearing_compute(p1.lon, p1.lat, p2.lon, p2.lat)


def bearing_np(lon1, lat1, lon2, lat2):
    """Bearing np"""
    return bearing_compute(lon1, lat1, lon2, lat2)
    # return angle_compute(lon1, lat1, lon2, lat2)


def inverse_bearing(brng):
    """Inverse bearing"""
    return (brng + 180) % 360


def bad(brng1, brng2):
    """Distance between brng1 and brng2 (degree)
    Returns:
    float: absolute degree different
    """
    dBrng = abs(brng1 - brng2)
    if dBrng > 180:
        dBrng = 360 - dBrng
    return dBrng


def turn_dist(brng1, brng2):
    """Compute Turn Distance"""
    return bad(brng1, brng2) * (15 / 180)


def turn_dist_np(brng1, brng2):
    """Compute Turn Distance NP"""
    return bad_np(brng1, brng2) * (15 / 180)


def bad_np(brng1, brng2):
    """bearing absolue distance np"""
    dBrng = abs(brng1 - brng2)
    temp = dBrng > 180
    dBrng[temp] = 360 - dBrng[temp]
    return dBrng


def h_dist(p1, p2):
    """ haversine distance between point 1 and point 2"""
    return haversine_np(p1.lon, p1.lat, p2.lon, p2.lat)


def cross_track_dist(dist_SP, brng_SP, brng_SE):
    """ cross track distance
    Parameters:
    float: dist_SP
    float: brng_SP (deg)
    float: brng_SE (deg)
    """
    brng_SP, brng_SE = map(np.radians, [brng_SP, brng_SE])
    dist = np.arcsin(np.sin(dist_SP / EARTH_RADIUS) * np.sin(brng_SP - brng_SE)) * EARTH_RADIUS
    return abs(dist)


def along_track_dist(dist_SP, XTD):
    ATD = np.arccos(np.cos(dist_SP / EARTH_RADIUS) / np.cos(XTD / EARTH_RADIUS)) * EARTH_RADIUS
    return abs(ATD)


def remain_dist(ps, pe, p, dist):
    brng_SE = bearing(ps, pe)
    brng_SP = bearing(ps, p)
    brng_ES = inverse_bearing(brng_SE)
    brng_EP = bearing(pe, p)
    #     obtuse case
    if bad(brng_SE, brng_SP) >= 90:
        return dist
    #     acute
    elif bad(brng_ES, brng_EP) >= 90:
        return 0
    else:
        d_SP = h_dist(ps, p)
        XTD = cross_track_dist(d_SP, brng_SP, brng_SE)
        ATD = along_track_dist(d_SP, XTD)
        return dist - ATD


def cross_track_point(ps, pe, p):
    brng_SE = bearing(ps, pe)
    brng_SP = bearing(ps, p)
    brng_ES = inverse_bearing(brng_SE)
    brng_EP = bearing(pe, p)
    #     obtuse case
    if bad(brng_SE, brng_SP) >= 90:
        return [ps.lon, ps.lat]
    #     acute
    elif bad(brng_ES, brng_EP) >= 90:
        return [pe.lon, pe.lat]
    else:
        d_SP = h_dist(ps, p)
        XTD = cross_track_dist(d_SP, brng_SP, brng_SE)
        ATD = along_track_dist(d_SP, XTD)
        per = abs(ATD) / h_dist(ps, pe)

        lat_n = ps.lat + (pe.lat - ps.lat) * per
        lon_n = ps.lon + (pe.lon - ps.lon) * per
        return [lon_n, lat_n]


def closest_dist(p, s, e):
    """Closest distance between point p and line SE based on cross_track distance
    """
    brng_SE = bearing(s, e)
    brng_SP = bearing(s, p)
    brng_ES = inverse_bearing(brng_SE)
    brng_EP = bearing(e, p)

    dist_SP = h_dist(s, p)
    #     obtuse case
    if bad(brng_SE, brng_SP) >= 90:
        return dist_SP
    #     acute
    elif bad(brng_ES, brng_EP) >= 90:
        dist_EP = h_dist(e, p)
        return dist_EP
    else:
        #         return abs(np.sin(np.radians(bad(brng_SE, brng_SP))) * dist_SP)
        return cross_track_dist(dist_SP, brng_SP, brng_SE)


def get_closest_segment_np(segment_df, lon, lat, direction, thres=6):
    ps_lon = segment_df['ps_lon'].values
    ps_lat = segment_df['ps_lat'].values
    pe_lon = segment_df['pe_lon'].values
    pe_lat = segment_df['pe_lat'].values

    brng_SE = segment_df['bearing'].values
    brng_SP = bearing_np(ps_lon, ps_lat, lon, lat)
    brng_ES = segment_df['bearing_backward'].values
    brng_EP = bearing_np(pe_lon, pe_lat, lon, lat)
    dist_SP = haversine_np(ps_lon, ps_lat, lon, lat)
    dist_EP = haversine_np(pe_lon, pe_lat, lon, lat)
    # cross_track dist
    dist = cross_track_dist(dist_SP, brng_SP, brng_SE)
    # obtuse case
    dist_obtuse = bad_np(brng_SE, brng_SP) >= 90
    dist[dist_obtuse] = dist_SP[dist_obtuse]
    # acute case
    dist_acute = bad_np(brng_ES, brng_EP) >= 90
    dist[dist_acute] = dist_EP[dist_acute]

    td = turn_dist_np(direction, brng_SE)
    dist += td
    min_dist = np.argmin(dist)

    if np.min(dist) > thres:
        return -1
    return segment_df.iloc[np.argmin(dist)].sid

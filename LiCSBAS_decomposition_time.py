#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import numpy as np
import datetime as dt
import h5py as h5
import sys

# ======================
# INPUT
# ======================
#track1 = '/work/scratch-pw5/licsar/alejobea/batchdir/Erta_Ale/014A'
#track2 = '/work/scratch-pw5/licsar/alejobea/batchdir/Erta_Ale/079D'

#folder="TS_GEOCml2clipmask"
#folderGEOC="GEOCml2clipmask"

if len(sys.argv) != 5:
    print(f"Uso: {sys.argv[0]} VOLCANO TRACK_ASC TRACK_DESC TS_FOLDER")
    print(f"Ejemplo: {sys.argv[0]} Socompa 149A 156D TS_GEOCml2clipmask")
    sys.exit(1)

volcano = sys.argv[1]
asc_track = sys.argv[2]
desc_track = sys.argv[3]
folder = sys.argv[4]

base_dir = os.getcwd()

track1 = os.path.join(base_dir, asc_track)
track2 = os.path.join(base_dir, desc_track)

folderGEOC = folder.replace("TS_", "")

# ======================
# UTILS
# ======================
def get_files(track):
    base = os.path.join(track, folder)
    out = {}
    for f in ['cum.h5', 'cum_filt.h5']:
        p = os.path.join(base, f)
        if os.path.exists(p):
            out[f] = p
    return out


def read_bin(f, ny, nx):
    if not os.path.exists(f):
        return None
    return np.fromfile(f, dtype=np.float32).reshape(ny, nx)


# ======================
# MAIN
# ======================
files1 = get_files(track1)
files2 = get_files(track2)

common = sorted(set(files1.keys()) & set(files2.keys()))
print("Procesando:", common)

for key in common:

    print("\n====", key, "====")

    h1 = h5.File(files1[key], 'r')
    h2 = h5.File(files2[key], 'r')

    cum1 = h1['cum'][:]
    cum2 = h2['cum'][:]

    im1 = h1['imdates'][()].astype(str)
    im2 = h2['imdates'][()].astype(str)

    lat1 = float(h1['corner_lat'][()])
    lon1 = float(h1['corner_lon'][()])
    dlat = float(h1['post_lat'][()])
    dlon = float(h1['post_lon'][()])

    nT1, ny, nx = cum1.shape


    # GEODESC

    LOSe1 = np.fromfile(os.path.join(track1, folderGEOC, 'E.geo'),dtype=np.float32).reshape(ny,nx)
    LOSu1 = np.fromfile(os.path.join(track1, folderGEOC, 'U.geo'),dtype=np.float32).reshape(ny,nx)
    LOSe2 = np.fromfile(os.path.join(track2, folderGEOC, 'E.geo'),dtype=np.float32).reshape(ny,nx)
    LOSu2 = np.fromfile(os.path.join(track2, folderGEOC, 'U.geo'),dtype=np.float32).reshape(ny,nx)




    def interp(cum, tin, tout):
        out = np.full((len(tout), ny, nx), np.nan, dtype=np.float32)

        for iy in range(ny):
            for ix in range(nx):
                ts = cum[:,iy,ix]
                ok = ~np.isnan(ts)
                if np.sum(ok) < 3:
                    continue

                out[:,iy,ix] = np.interp(tout, tin[ok], ts[ok])

        return out
    
    # TIME ALIGNMENT (track dominante)

    t1 = np.array([dt.datetime.strptime(d,'%Y%m%d').toordinal() for d in im1])
    t2 = np.array([dt.datetime.strptime(d,'%Y%m%d').toordinal() for d in im2])

    tmin = max(t1.min(), t2.min())
    tmax = min(t1.max(), t2.max())

    # recorte al periodo común
    mask1 = (t1 >= tmin) & (t1 <= tmax)
    mask2 = (t2 >= tmin) & (t2 <= tmax)

    t1c = t1[mask1]
    t2c = t2[mask2]

    cum1c = cum1[mask1]
    cum2c = cum2[mask2]

    # elegir track más denso
    if len(t1c) >= len(t2c):
        t_common = t1c
        cum1_i = cum1c
        cum2_i = interp(cum2c, t2c, t_common)
    else:
        t_common = t2c
        cum2_i = cum2c
        cum1_i = interp(cum1c, t1c, t_common)

    im_common = [dt.datetime.fromordinal(int(t)).strftime('%Y%m%d') for t in t_common]

    print("epochs:", len(t_common))

    
    # INTERPOLATION


    #cum1_i = interp(cum1, t1, t_common)
    #cum2_i = interp(cum2, t2, t_common)


    # MASK ROBUSTA

    mask = np.nanmean(~np.isnan(cum1_i) & ~np.isnan(cum2_i), axis=0) > 0.2

    print("valid pixels:", np.sum(mask))


    # DECOMPOSITION

    ew = np.full_like(cum1_i, np.nan)
    ud = np.full_like(cum1_i, np.nan)

    for it in range(len(t_common)):

        los1 = cum1_i[it]
        los2 = cum2_i[it]

        valid = (~np.isnan(los1)) & (~np.isnan(los2)) & mask

        if np.sum(valid) < 5:
            continue

        d1 = los1[valid]
        d2 = los2[valid]

        e1 = LOSe1[valid]
        e2 = LOSe2[valid]
        u1 = LOSu1[valid]
        u2 = LOSu2[valid]

        a11 = e1**2 + e2**2
        a12 = e1*u1 + e2*u2
        a22 = u1**2 + u2**2

        be = e1*d1 + e2*d2
        bu = u1*d1 + u2*d2

        det = a11*a22 - a12**2
        det[det==0] = np.nan

        ew[it][valid] = (a22*be - a12*bu)/det
        ud[it][valid] = (-a12*be + a11*bu)/det

    print("EW mean:", np.nanmean(ew))
    print("UD mean:", np.nanmean(ud))


    # REF AREA (GARANTIZADO)

    #ys, xs = np.where(mask)

    #refy1 = int(np.percentile(ys,40))
    #refy2 = int(np.percentile(ys,60))
    #refx1 = int(np.percentile(xs,40))
    #refx2 = int(np.percentile(xs,60))

    #ref_str = f"{refx1}:{refx2}/{refy1}:{refy2}"



#    mask_ref = np.all(~np.isnan(ew), axis=0) & np.all(~np.isnan(ud), axis=0)

    #ys, xs = np.where(mask_ref)

    #refy1 = int(np.percentile(ys,45))
    #refy2 = int(np.percentile(ys,55))
    #refx1 = int(np.percentile(xs,45))
    #refx2 = int(np.percentile(xs,55))

    #ref_str = f"{refx1}:{refx2}/{refy1}:{refy2}"

    mask_ref = np.all(~np.isnan(ew), axis=0) & np.all(~np.isnan(ud), axis=0)

    winsize = 20

    best_count = 0

    for y in range(ny-winsize):
        for x in range(nx-winsize):

            count = np.sum(mask_ref[y:y+winsize, x:x+winsize])

            if count > best_count:
                best_count = count
                refy1 = y
                refy2 = y+winsize
                refx1 = x
                refx2 = x+winsize

    ref_str = f"{refx1}:{refx2}/{refy1}:{refy2}"


    # SAVE H5

    def save(outdir, fname, data):

        os.makedirs(outdir, exist_ok=True)

        with h5.File(os.path.join(outdir,fname),'w') as f:

            f.create_dataset('cum', data=data, compression='gzip')

            vel = np.gradient(data,axis=0).mean(axis=0)
            f.create_dataset('vel', data=vel)

            f.create_dataset('imdates', data=np.array(im_common,dtype='S8'))

            f.create_dataset('corner_lat', data=lat1)
            f.create_dataset('corner_lon', data=lon1)
            f.create_dataset('post_lat', data=dlat)
            f.create_dataset('post_lon', data=dlon)

            f.create_dataset('refarea', data=ref_str.encode())


    EAST="TS_GEOCml2mask_EAST"
    UP="TS_GEOCml2mask_UP"

    save(EAST, key, ew)
    save(UP, key, ud)

    # ======================
    # RESULTS REAL
    # ======================
    def build_results(outdir):

        os.makedirs(os.path.join(outdir,'results'), exist_ok=True)

        names = [
            'vel','coh_avg','n_unw','vstd','maxTlen','n_gap','stc',
            'n_ifg_noloop','n_loop_err','resid_rms'
        ]

        for n in names:
            f1 = os.path.join(track1,folder+'/results',n)
            f2 = os.path.join(track2,folder+'/results',n)

            d1 = read_bin(f1,ny,nx)
            d2 = read_bin(f2,ny,nx)

            if d1 is None and d2 is None:
                out = np.zeros((ny,nx),np.float32)
            elif d1 is None:
                out = d2
            elif d2 is None:
                out = d1
            else:
                out = np.nanmean(np.stack([d1,d2]),axis=0)

            out[~mask] = np.nan
            out.tofile(os.path.join(outdir,'results',n))

        # mask
        m = np.ones((ny,nx),np.float32)
        m[~mask] = np.nan
        m.tofile(os.path.join(outdir,'results/mask'))

        # --------------------------------------------------
        # Copy auxiliary files needed by LiCSBAS_plot_ts
        # --------------------------------------------------

        for aux in ['slc.mli', 'hgt']:

            f1 = os.path.join(track1, folder, 'results', aux)
            f2 = os.path.join(track2, folder, 'results', aux)

            src = None

            if os.path.exists(f1):
                src = f1
            elif os.path.exists(f2):
                src = f2

            if src is not None:
                arr = np.fromfile(src, dtype=np.float32)

                if arr.size == ny * nx:
                    arr = arr.reshape(ny, nx)
                    arr[~mask] = np.nan
                    arr.astype(np.float32).tofile(
                        os.path.join(outdir, 'results', aux)
                    )

    build_results(EAST)
    build_results(UP)

    import shutil

    def copy_info(outdir):

        src1 = os.path.join(track1, folder, 'info')
        src2 = os.path.join(track2, folder, 'info')

        if os.path.exists(src1):
            shutil.copytree(src1,
                            os.path.join(outdir, 'info'),
                            dirs_exist_ok=True)

        elif os.path.exists(src2):
            shutil.copytree(src2,
                            os.path.join(outdir, 'info'),
                            dirs_exist_ok=True)

    copy_info(EAST)
    copy_info(UP)

print("\n TODO OK — listo para LiCSBAS_plot_ts")

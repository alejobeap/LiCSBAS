#!/usr/bin/env python3
"""
v1 20241001 Pedro Espin B

This script applies a tropospheric correction to unw data using ERA5 data. ERA5 data may be automatically downloaded from COMET-LiCS web at step01 (if available), or could be externally obtained by requesting on a ICAMS web (https://github.com/ymcmrs/ICAMS).

===============
Input & output files
===============
Inputs in GEOCml*/ :
 - yyyymmdd_yyyymmdd/
   - yyyymmdd_yyyymmdd.unw
   - yyyymmdd_yyyymmdd.cc
 - U.geo
 - EQA.dem_par
 - slc.mli.par

Inputs in ERA5/ :
 - yyyymmdd.icams.sltd.geo.tif  and/or 


Outputs in GEOCml*ERA5/
 - yyyymmdd_yyyymmdd/
   - yyyymmdd_yyyymmdd.unw[.png] : Corrected unw
   - yyyymmdd_yyyymmdd.ERA5.png : Comparison image
   - yyyymmdd_yyyymmdd.cc        : Coherence (symbolic link)
 - ERA5_info.txt : List of noise reduction rates
 - ERA5_info.png : Correlation diagram of STD between before and after
 - no_ERA5_ifg.txt : List of removed ifg because no ERA5 data available
 - no_ERA5_im.txt  : List of images with no available ERA5 data
 - sltd/
   - yyyymmdd.icams.sltd.geo : Slantrange tropospheric delay in rad
 - other files needed for following time series analysis

=====
Usage
=====
LiCSBAS03op_ERA5.py -i in_dir -o out_dir [-g era5dir] [--fillhole] [--n_para int]

 -i  Path to the GEOCml* dir containing stack of unw data
 -o  Path to the output dir
 -g  Path to the dir containing ERA5 data (Default: ERA5)
 --fillhole  Fill holes of ERA5 data at hgt=0 in SRTM3 by averaging surrounding pixels
 --n_para  Number of parallel processing (Default: # of usable CPU)

"""
#%% Change log
'''

v1.0 20241001 P. Espin B. based in GACOS from Yu Morishita, Uni of Leeds and GSI
 - Original implementation
'''


#%% Import
import getopt
import os
import sys
import time
import shutil
import glob
import numpy as np
from osgeo import gdal
import multiprocessing as multi
import SCM
import LiCSBAS_io_lib as io_lib
import LiCSBAS_tools_lib as tools_lib
import LiCSBAS_plot_lib as plot_lib
from matplotlib import pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

class Usage(Exception):
    """Usage context manager"""
    def __init__(self, msg):
        self.msg = msg

#%% 
def plot_gacos_info2(gacos_infofile, pngfile):

    import seaborn as sns
    import pandas as pd
    import warnings
    warnings.simplefilter(action="ignore", category=FutureWarning)
### Read data
    with open(gacos_infofile, "r") as f:
        info = f.readlines()[1:]

    std_bf, std_af, rate = [], [], []; 
    std_bfok, std_afok, std_bfbad, std_afbad= [], [], [], []; 
  

    conp=0
    conn=0
    conc=0
    contador=0
    for line in info:
     date, std_bf1, std_af1, rate1 = line.split()
     #print(std_bf1, std_af1)
     if float(std_bf1)>float(std_af1):
      conp=conp+1
     elif float(std_bf1)==float(std_af1):
      conc=conc+1
     else:
      conn=conn+1
     contador=contador+1

    total=contador #conp+conn
    print("TOTAL",total,len(std_bf1))
    porpos=(conp*100)/total
    porneg=(conn*100)/total
    porcero=(conc*100)/total

    for line in info:
        date, std_bf1, std_af1, rate1 = line.split()
        if std_bf1=='0.0' or std_bf1=='nan' or std_af1=='0.0' or std_af1=='nan':
            continue
        std_bf.append(float(std_bf1))
        std_af.append(float(std_af1))
        rate.append(float(rate1[:-1]))

    std_bf1 = np.array(std_bf)
    std_af1 = np.array(std_af)
   
    yy=np.max(std_bf1)
    xx=np.max(std_af1)
    xylim1 = np.max(np.concatenate((std_bf1, std_af1)))+1 
    #print(std_bf)
#    std_bf.insert(0, str('x'))
 #   std_af.insert(0, str('y'))
    data = np.column_stack((std_bf,std_af))
    df1 = pd.DataFrame(data, columns=['std_before', 'std_after'])
#    print(df1)
    xylim1 = np.max(np.concatenate((std_bf, std_af)))+1

    #joint_kws=dict(gridsize=40)
    #graph = sns.jointplot(x=df1.std_before, y=df1.std_after, kind ="hex",color="lightcoral",joint_kws= joint_kws)
    #sns.lineplot([0, xylim1], [0, xylim1], ax=graph.ax_joint,linewidth=2, color='grey', alpha=0.5, zorder=2)
    #cbar_ax = graph.fig.add_axes([1, .25, .05, .4])  # x, y, width, height
    #plt.colorbar(cax=cbar_ax, label="Pixel count")
    #graph.ax_joint.set_xlabel('STD Before (rad)', fontweight='bold')
    #graph.ax_joint.set_ylabel('STD After (rad)', fontweight='bold')
    #graph.savefig(pngfile)
    graph = sns.jointplot(x=df1.std_before, y=df1.std_after, kind ="hex",color="lightcoral")
    #sns.lineplot([0, xylim1], [0, xylim1], ax=graph.ax_joint,linewidth=2, color='grey', alpha=0.5, zorder=2)
    plt.plot([0, xylim1], [0, xylim1], linewidth=2, color='grey', alpha=0.5, zorder=2)
    plt.text(xx-2, 0.5 , "Good: {:.2f}%\nNo Change: {:.2f}%\n".format(porpos,porcero),fontweight='bold')
    plt.text(0.5, yy , "Bad: {:.2f}%".format(porneg),fontweight='bold' )
    cbar_ax = graph.fig.add_axes([1, .25, .05, .4])  # x, y, width, height
    plt.colorbar(cax=cbar_ax, label="Pixel count")
    graph.ax_joint.set_xlabel('STD Before (rad)', fontweight='bold')
    graph.ax_joint.set_ylabel('STD After (rad)', fontweight='bold')
    graph.savefig(pngfile)



#####################################


#%% fill hole function
def fillhole(ztd):
    """
    Fill holes (no data) surrounded by valid data by averaging surrounding pixels.
    0 in ztd means no data.
    """
    length, width = ztd.shape
    
    ### Add 1 pixel margin to ztd data filled with 0
    ztd1 = np.zeros((length+2, width+2), dtype=np.float32)
    ztd1[1:length+1, 1:width+1] = ztd
    n_ztd1 = np.int16(ztd1!=0) # 1 if exist, 0 if no data

    ### Average 8 srrounding pixels. [1, 1] is center
    pixels = [[0, 0], [0, 1], [0, 2], [1, 0], [1, 2], [2, 0], [2, 1], [2, 2]]
    _ztd = np.zeros_like(ztd)
    _n_ztd = np.zeros_like(ztd)

    for pixel in pixels:
        ### Adding data and number of data
        _ztd = _ztd + ztd1[pixel[0]:length+pixel[0],pixel[1]:width+pixel[1]]
        _n_ztd = _n_ztd + n_ztd1[pixel[0]:length+pixel[0],pixel[1]:width+pixel[1]]

    _n_ztd[_n_ztd==0] = 1 # avoid 0 division
    _ztd = _ztd/_n_ztd

    ### Fill hole 
    ztd[ztd==0] = _ztd[ztd==0]
    
    return ztd


#%% make_hdr
def make_hdr(ztdpar, hdrfile):
    ### Get ztd info. Grid registration
    width_ztd = int(io_lib.get_param_par(ztdpar, 'WIDTH'))
    length_ztd = int(io_lib.get_param_par(ztdpar, 'FILE_LENGTH'))
    dlat_ztd = float(io_lib.get_param_par(ztdpar, 'Y_STEP')) #minus
    dlon_ztd = float(io_lib.get_param_par(ztdpar, 'X_STEP'))
    latn_ztd = float(io_lib.get_param_par(ztdpar, 'Y_FIRST'))
    lonw_ztd = float(io_lib.get_param_par(ztdpar, 'X_FIRST'))

    ### Make hdr file of ztd
    strings = ["NROWS          {}".format(length_ztd),
               "NCOLS          {}".format(width_ztd),
               "NBITS          32",
               "PIXELTYPE      FLOAT",
               "BYTEORDER      I",
               "LAYOUT         BIL",
               "ULXMAP         {}".format(lonw_ztd),
               "ULYMAP         {}".format(latn_ztd),
               "XDIM           {}".format(dlon_ztd),
               "YDIM           {}".format(np.abs(dlat_ztd))]
    with open(hdrfile, "w") as f:
        f.write("\n".join(strings))


#%% Main
def main(argv=None):
    
    #%% Check argv
    if argv == None:
        argv = sys.argv
        
    start = time.time()
    ver="1"; date=20241001; author="P. Espin B."
    print("\n{} ver{} {} {}".format(os.path.basename(argv[0]), ver, date, author), flush=True)
    print("{} {}".format(os.path.basename(argv[0]), ' '.join(argv[1:])), flush=True)

    ### For parallel processing
    global imdates2, era5dir, outputBounds, width_geo, length_geo, resampleAlg,\
        sltddir, LOSu, m2r_coef, fillholeflag, ifgdates2,\
        in_dir, out_dir, length_unw, width_unw, cycle, cmap_wrap


    #%% Set default
    in_dir = []
    out_dir = []
    era5dir = 'ERA5'
    resampleAlg = 'cubicspline'# None # 'cubic' 
    fillholeflag = False
    try:
        n_para = len(os.sched_getaffinity(0))
    except:
        n_para = multi.cpu_count()

    q = multi.get_context('fork')
    cmap_wrap = SCM.romaO

    #%% Read options
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hi:o:g:z:", ["fillhole", "help", "n_para="])
        except getopt.error as msg:
            raise Usage(msg)
        for o, a in opts:
            if o == '-h' or o == '--help':
                print(__doc__)
                return 0
            elif o == '-i':
                in_dir = a
            elif o == '-o':
                out_dir = a
            elif o == '-z': ## for backward-compatible
                era5dir = a
            elif o == '-g':
                era5dir = a
            elif o == "--fillhole":
                fillholeflag = True
            elif o == '--n_para':
                n_para = int(a)

        if not in_dir:
            raise Usage('No input directory given, -i is not optional!')
        elif not os.path.isdir(in_dir):
            raise Usage('No {} dir exists!'.format(in_dir))
        elif not os.path.exists(os.path.join(in_dir, 'slc.mli.par')):
            raise Usage('No slc.mli.par file exists in {}!'.format(in_dir))
        if not out_dir:
            #raise Usage('No output directory given, -o is not optional!')
            out_dir = in_dir+"_ERA5"
        if not os.path.isdir(era5dir):
            raise Usage('No {} dir exists!'.format(era5dir))

    except Usage as err:
        print("\nERROR:", file=sys.stderr, end='')
        print("  "+str(err.msg), file=sys.stderr)
        print("\nFor help, use -h or --help.\n", file=sys.stderr)
        return 2
    
    
    #%% Read data information
    ### Directory

    in_dir = os.path.abspath(in_dir)
    era5dir = os.path.abspath(era5dir)

    out_dir = os.path.abspath(out_dir)
    if not os.path.exists(out_dir): os.mkdir(out_dir)

    sltddir = os.path.join(os.path.join(out_dir),'sltd')
    if not os.path.exists(sltddir): os.mkdir(sltddir)

    ### Get general info
    mlipar = os.path.join(in_dir, 'slc.mli.par')
    width_unw = int(io_lib.get_param_par(mlipar, 'range_samples'))
    length_unw = int(io_lib.get_param_par(mlipar, 'azimuth_lines'))
    speed_of_light = 299792458 #m/s
    radar_frequency = float(io_lib.get_param_par(mlipar, 'radar_frequency')) #Hz
    wavelength = speed_of_light/radar_frequency #meter
    m2r_coef = 4*np.pi/wavelength
    
    if wavelength > 0.2: ## L-band
        cycle = 1.5  # 2pi/cycle for png
    else: ## C-band
        cycle = 3  # 2pi*3/cycle for png

    ### Get geo info. Grid registration
    dempar = os.path.join(in_dir, 'EQA.dem_par')
    width_geo = int(io_lib.get_param_par(dempar, 'width'))
    length_geo = int(io_lib.get_param_par(dempar, 'nlines'))
    dlat_geo = float(io_lib.get_param_par(dempar, 'post_lat')) #minus
    dlon_geo = float(io_lib.get_param_par(dempar, 'post_lon'))
    latn_geo = float(io_lib.get_param_par(dempar, 'corner_lat'))
    lonw_geo = float(io_lib.get_param_par(dempar, 'corner_lon'))
    lats_geo = latn_geo+dlat_geo*(length_geo-1)
    lone_geo = lonw_geo+dlon_geo*(width_geo-1)
    outputBounds = (lonw_geo, lats_geo, lone_geo, latn_geo)
    
    ### Check coordinate
    if width_unw!=width_geo or length_unw!=length_geo:
        print('\n{} seems to contain files in radar coordinate!!\n'.format(in_dir), file=sys.stderr)
        print('Not supported.\n', file=sys.stderr)
        return 1

    ### Calc incidence angle from U.geo
    ufile = os.path.join(in_dir, 'U.geo')
    LOSu = io_lib.read_img(ufile, length_geo, width_geo)
    LOSu[LOSu==0] = np.nan

    ### Get ifgdates and imdates
    ifgdates = tools_lib.get_ifgdates(in_dir)
    imdates = tools_lib.ifgdates2imdates(ifgdates)
    n_ifg = len(ifgdates)
    n_im = len(imdates)


    #%% Process ztd files 
    print('\nConvert ztd/sltd.geo.tif files to icams.sltd.geo files...', flush=True)

    no_ERA5_imfile = os.path.join(out_dir, 'no_era5_im.txt')
    if os.path.exists(no_ERA5_imfile): os.remove(no_ERA5_imfile)

    ### First check if sltd already exist
    imdates2 = []
    for imd in imdates:
        sltd_geofile = os.path.join(sltddir, imd+'.icams.sltd.geo')
        if not os.path.exists(sltd_geofile):
            imdates2.append(imd)

    n_im2 = len(imdates2)
    if n_im-n_im2 > 0:
        print("  {0:3}/{1:3} sltd already exist. Skip".format(n_im-n_im2, n_im), flush=True)

    if n_im2 > 0:
        ### Convert with parallel processing
        if n_para > n_im2:
            _n_para = n_im2
        else:
            _n_para = n_para
            
        print('  {} parallel processing...'.format(_n_para), flush=True)
        p = q.Pool(_n_para)
        no_ERA5_imds = p.map(convert_wrapper, range(n_im2))
        p.close()
    
        for imd in no_ERA5_imds:
            if imd is not None:
                with open(no_ERA5_imfile, mode='a') as fnoERA5:
                    print('{}'.format(imd), file=fnoERA5)
    
    
    #%% Correct unw files
    print('\nCorrect unw data...', flush=True)
    ### Information files    
    gacinfofile = os.path.join(out_dir, 'ERA5_info.txt')
    if not os.path.exists(gacinfofile):
        ### Add header
        with open(gacinfofile, "w") as f:
            print(' Phase STD (rad) Before After  ReductionRate', file=f)
    
    no_ERA5_ifgfile = os.path.join(out_dir, 'no_era5_ifg.txt')
    if os.path.exists(no_ERA5_ifgfile): os.remove(no_ERA5_ifgfile)

    ### First check if already corrected unw exist
    ifgdates2 = []
    for i, ifgd in enumerate(ifgdates): 
        out_dir1 = os.path.join(out_dir, ifgd)
        unw_corfile = os.path.join(out_dir1, ifgd+'.unw')
        if not os.path.exists(unw_corfile):
            ifgdates2.append(ifgd)

    n_ifg2 = len(ifgdates2)
    if n_ifg-n_ifg2 > 0:
        print("  {0:3}/{1:3} corrected unw already exist. Skip".format(n_ifg-n_ifg2, n_ifg), flush=True)

    if n_ifg2 > 0:
        ### Correct with parallel processing
        if n_para > n_ifg2:
            _n_para = n_ifg2
        else:
            _n_para = n_para
            
        print('  {} parallel processing...'.format(_n_para), flush=True)
        p = q.Pool(_n_para)
        _return = p.map(correct_wrapper, range(n_ifg2))
        p.close()
    
        for i in range(n_ifg2):
            if _return[i][0] == 1:
                with open(no_ERA5_ifgfile, mode='a') as fnoERA5:
                    print('{}'.format(_return[i][1]), file=fnoERA5)
            elif _return[i][0] == 2:
                with open(gacinfofile, "a") as f:
                    print('{0}  {1:4.1f}  {2:4.1f} {3:5.1f}%'.format(*_return[i][1]), file=f)
    
    print("", flush=True)
    
    
    #%% Create correlation png
    pngfile = os.path.join(out_dir, 'ERA5_info.png')
    plot_lib.plot_gacos_info(gacinfofile, pngfile)

 #%% Create correlation png
    pngfile = os.path.join(out_dir, 'ERA5_info2.png')
#    plot_lib.plot_gacos_info2(gacinfofile, pngfile)    
    plot_gacos_info2(gacinfofile, pngfile)    
    
    #%% Copy other files
    files = glob.glob(os.path.join(in_dir, '*'))
    for file in files:
        if not os.path.isdir(file): #not copy directory, only file
            print('Copy {}'.format(os.path.basename(file)), flush=True)
            shutil.copy(file, out_dir)
    
    
    #%% Finish
    elapsed_time = time.time()-start
    hour = int(elapsed_time/3600)
    minite = int(np.mod((elapsed_time/60),60))
    sec = int(np.mod(elapsed_time,60))
    print("\nElapsed time: {0:02}h {1:02}m {2:02}s".format(hour,minite,sec))

    print('\n{} Successfully finished!!\n'.format(os.path.basename(argv[0])))
    print('Output directory: {}\n'.format(os.path.relpath(out_dir)))

    if os.path.exists(no_ERA5_ifgfile):
        print('Caution: Some ifgs below are excluded due to ERA5 unavailable')
        with open(no_ERA5_ifgfile) as f:
            for line in f:
                print(line, end='')
        print('')

    if os.path.exists(no_ERA5_imfile):
        print('ERA5 data for the following dates are missing:')
        with open(no_ERA5_imfile) as f:
            for line in f:
                print(line, end='')
        print('')

#%%
def convert_wrapper(ix_im):
    imd = imdates2[ix_im]
    if np.mod(ix_im, 10)==0:
        print('  Finished {0:4}/{1:4}th sltd...'.format(ix_im, len(imdates2)), flush=True)

    ztdfile = os.path.join(era5dir, imd+'.ztd')
    ztdtiffile = os.path.join(era5dir, imd+'.ztd.tif')
    sltdtiffile = os.path.join(era5dir, imd+'.icams.sltd.geo.tif')

    if os.path.exists(sltdtiffile):
        print('    Use {}.icams.sltd.geo.tif'.format(imd), flush=True)
        infile = os.path.basename(sltdtiffile)
        try: ### Cut and resapmle. Already in rad.
            sltd_geo = gdal.Warp("", sltdtiffile, format='MEM', outputBounds=outputBounds, width=width_geo, height=length_geo, resampleAlg=resampleAlg, srcNodata=0).ReadAsArray()
        except: ## if broken
            print ('  {} cannot open. Skip'.format(infile), flush=True)
            return imd

    elif os.path.exists(ztdtiffile):
        print('    Use {}.ztd.tif'.format(imd), flush=True)
        infile = os.path.basename(ztdtiffile)
        try: ### Cut and resapmle ztd to geo
            ztd_geo = gdal.Warp("", ztdtiffile, format='MEM', outputBounds=outputBounds, width=width_geo, height=length_geo, resampleAlg=resampleAlg, srcNodata=0).ReadAsArray()
        except: ## if broken
            print ('  {} cannot open. Skip'.format(infile), flush=True)
            return imd

        ### Meter to rad, slantrange
        sltd_geo = ztd_geo/LOSu*m2r_coef ## LOSu=cos(inc)

    elif os.path.exists(ztdfile):
        print('    Use {}.ztd[.rsc]'.format(imd), flush=True)
        infile = os.path.basename(ztdfile)
        hdrfile = os.path.join(sltddir, imd+'.hdr')
        bilfile = os.path.join(sltddir, imd+'.bil')
        if os.path.exists(hdrfile): os.remove(hdrfile)
        if os.path.exists(bilfile): os.remove(bilfile)
        make_hdr(ztdfile+'.rsc', hdrfile)
        os.symlink(os.path.relpath(ztdfile, sltddir), bilfile)
        
        ## Check read error with unkown cause
        if gdal.Info(bilfile) is None: 
            ### Create new ztd by adding 0.0001m
            print('{} cannot open, but trying minor update. You can ignore this error unless this script stops.'.format(ztdfile))
            shutil.copy2(ztdfile, ztdfile+'.org') ## Backup
            _ztd = np.fromfile(ztdfile, dtype=np.float32)
            _ztd[_ztd!=0] = _ztd[_ztd!=0]+0.001
            _ztd.tofile(ztdfile)

        ### Cut and resapmle ztd to geo
        ztd_geo = gdal.Warp("", bilfile, format='MEM', outputBounds=outputBounds,\
            width=width_geo, height=length_geo, \
            resampleAlg=resampleAlg, srcNodata=0).ReadAsArray()
        os.remove(hdrfile)
        os.remove(bilfile)

        ### Meter to rad, slantrange
        sltd_geo = ztd_geo/LOSu*m2r_coef ## LOSu=cos(inc)

    else:
        print('    No {}.ztd|ztd.tif|sltd.geo.tif! Skip.'.format(imd), flush=True)
        return imd ## Next imd

    ### Skip if no data in the area
    if np.all((sltd_geo==0)|np.isnan(sltd_geo)):
        print('    No valid data in {}! Skip.'.format(infile), flush=True)
        return imd ## Next imd

    ### Fill hole is specified
    if fillholeflag:
        sltd_geo = fillhole(sltd_geo)
    
    ### Output as sltd.geo
    sltd_geofile = os.path.join(sltddir, imd+'.icams.sltd.geo')
    sltd_geo.tofile(sltd_geofile)

    return


#%%
def correct_wrapper(i):
    ifgd = ifgdates2[i]
    if np.mod(i, 10)==0:
        print('  Finished {0:4}/{1:4}th unw...'.format(i, len(ifgdates2)), flush=True)

    md = ifgd[:8]
    sd = ifgd[-8:]
    msltdfile = os.path.join(sltddir, md+'.icams.sltd.geo')
    ssltdfile = os.path.join(sltddir, sd+'.icams.sltd.geo')
    
    in_dir1 = os.path.join(in_dir, ifgd)
    out_dir1 = os.path.join(out_dir, ifgd)
    
    ### Check if sltd available for both primary and secondary. If not continue
    ## Not use in tsa because loop cannot be closed
    if not (os.path.exists(msltdfile) and os.path.exists(ssltdfile)):
        print('  ztd file not available for {}'.format(ifgd), flush=True)
        return 1, ifgd

    ### Prepare directory and file
    if not os.path.exists(out_dir1): os.mkdir(out_dir1)
    unwfile = os.path.join(in_dir1, ifgd+'.unw')
    unw_corfile = os.path.join(out_dir1, ifgd+'.unw')
    
    ### Calculate dsltd
    msltd = io_lib.read_img(msltdfile, length_unw, width_unw)
    ssltd = io_lib.read_img(ssltdfile, length_unw, width_unw)

    msltd[msltd==0] = np.nan
    ssltd[ssltd==0] = np.nan
    
    dsltd = ssltd-msltd
    
    ### Correct unw
    unw = io_lib.read_img(unwfile, length_unw, width_unw)
    
    unw[unw==0] = np.nan
    unw_cor = unw-dsltd
    unw_cor.tofile(unw_corfile)
    
    ### Calc std
    std_unw = np.nanstd(unw)
    std_unwcor = np.nanstd(unw_cor)
    rate = (std_unw-std_unwcor)/std_unw*100

    ### Link cc
#    if not os.path.exists(os.path.join(out_dir1, ifgd+'.cc')):
#        os.symlink(os.path.relpath(os.path.join(in_dir1, ifgd+'.cc'), out_dir1), os.path.join(out_dir1, ifgd+'.cc'))

#####MIOOOOOOOOOOOOOOO
    if not os.path.exists(os.path.join(out_dir1, ifgd+'.cc')):
      try:  
       os.symlink(os.path.relpath(os.path.join(in_dir1, ifgd+'.cc'), out_dir1), os.path.join(out_dir1, ifgd+'.cc'))
      except FileExistsError:
        # The file might have been created by another process
        print(f"Symlink {out_file} already exists. Skipping creation.") 
        
    ### Output png for comparison
    data3 = [np.angle(np.exp(1j*(data/cycle))*cycle) for data in [unw, unw_cor, dsltd]]
    title3 = ['ERA5 unw_org (STD: {:.1f} rad)'.format(std_unw), 'unw_cor (STD: {:.1f} rad)'.format(std_unwcor), 'dsltd ({:.1f}% reduced)'.format(rate)]
    pngfile = os.path.join(out_dir1, ifgd+'.era5.png')
    plot_lib.make_3im_png(data3, pngfile, cmap_wrap, title3, vmin=-np.pi, vmax=np.pi, cbar=True)
    
    ## Output png for corrected unw
    pngfile = os.path.join(out_dir1, ifgd+'.unw.png')
    title = '{} ({}pi/cycle)'.format(ifgd, cycle*2)
    plot_lib.make_im_png(np.angle(np.exp(1j*unw_cor/cycle)*cycle), pngfile, cmap_wrap, title, -np.pi, np.pi, cbar=True)

    if not os.path.exists(os.path.join(out_dir1, ifgd+'.unw.png')):
       print("Prueba")
       pngfile = os.path.join(out_dir1, ifgd+'.unw.png')
       title = '{} ({}pi/cycle)'.format(ifgd, cycle*2)
       plot_lib.make_im_png(np.angle(np.exp(1j*unw_cor/cycle)*cycle), pngfile, cmap_wrap, title, -np.pi, np.pi, cbar=True)

    return 2, [ifgd, std_unw, std_unwcor, rate]



#%% main
if __name__ == "__main__":
    sys.exit(main())

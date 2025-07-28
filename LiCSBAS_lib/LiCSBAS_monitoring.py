#!/usr/bin/env python3
"""
========
Overview
========
Python3 library of monitoring functions for LiCSBAS.

=========
Changelog
=========
20250723 Pedro Espin-Bedon, Uni of Leeds
 - Original implementation

"""

import os
import h5py as h5


def update_ifgdir(ifgdir):
    ifgdir = os.path.abspath(ifgdir)
    ts_dir = os.path.join(os.path.dirname(ifgdir), 'TS_' + os.path.basename(ifgdir))
    cumfile = os.path.join(ts_dir, 'cum.h5')

    if not os.path.exists(cumfile):
        print("No hay archivo cum.h5 antiguo")
        return os.path.basename(ifgdir)

    print(f'\nLeyendo {os.path.relpath(cumfile)}')
    with h5.File(cumfile, 'r') as cumh5:
        imdates = cumh5['imdates'][()].astype(str).tolist()

    if not imdates:
        print("No hay fechas en cum.h5")
        return os.path.basename(ifgdir)

    last_dates = imdates[-4:]  # Last 4 dates
    lastimdate = imdates[-1]
    print("Last date in cum.h5:", lastimdate)

    # Buscar carpetas con fecha inicial mayor que lastimdate3
    newer_folders = []
    for name in os.listdir(ifgdir):
        folder_path = os.path.join(ifgdir, name)
        if os.path.isdir(folder_path):
            try:
                start_date, _ = name.split('_')
                if start_date > last_dates[0]:
                    newer_folders.append(name)
            except ValueError:
                continue

    if not newer_folders:
        print(f"There are no folders with a start date greater than {last_dates[0]}. Using original folder.")
        return os.path.basename(ifgdir)

    print(f"There are folders with an initial date greater than {lastimdate}:")
    for folder in sorted(newer_folders):
        print(" -", folder)

    update_dir = ifgdir + "_update"
    os.makedirs(update_dir, exist_ok=True)

    # Enlazar carpetas con fechas relevantes
    link_ifg_folders(ifgdir, update_dir, last_dates)

    # Enlazar archivos sueltos
    link_loose_files(ifgdir, update_dir)

    print(f"\nUsing update folder: {os.path.basename(update_dir)}")
    return os.path.basename(update_dir)


def link_ifg_folders(src_dir, dst_dir, dates):
    for name in os.listdir(src_dir):
        src_path = os.path.join(src_dir, name)
        if os.path.isdir(src_path):
            for date in dates:
                if date in name:
                    dst_path = os.path.join(dst_dir, name)
                    if not os.path.exists(dst_path):
                        os.symlink(src_path, dst_path)
                        print(f"Enlazado carpeta con fecha {date}: {name}")
                    break  # Don't match the same folder multiple times


def link_loose_files(src_dir, dst_dir):
    for name in os.listdir(src_dir):
        src_path = os.path.join(src_dir, name)
        dst_path = os.path.join(dst_dir, name)
        if os.path.isfile(src_path) and not os.path.exists(dst_path):
            os.symlink(src_path, dst_path)
            print(f"Enlazado archivo suelto: {name}")


#### For steps 12-16 Only to check 


def update_ifgdir12_16(ifgdir):
    ifgdir = os.path.abspath(ifgdir)
    cumfile = os.path.join(os.path.dirname(ifgdir), 'TS_' + os.path.basename(ifgdir), 'cum.h5')

    if not os.path.exists(cumfile):
        print("There aren't old cum.h5 file")
        return os.path.basename(ifgdir)  # sin cambios

    with h5.File(cumfile, 'r') as cumh5:
        imdates = cumh5['imdates'][()].astype(str).tolist()

    lastimdate = imdates[-1] if imdates else None

    if lastimdate is None:
        return os.path.basename(ifgdir)  # sin cambios

    # Verificar si hay carpetas más recientes
    carpetas_mayores = []
    for name in os.listdir(ifgdir):
        full_path = os.path.join(ifgdir, name)
        if os.path.isdir(full_path):
            try:
                start, end = name.split('_')
                if start > lastimdate:
                    carpetas_mayores.append(name)
            except ValueError:
                continue

    if carpetas_mayores:
        update_dir = ifgdir + "_update"
        print(f"More recent folders were detected. We suggest using: {os.path.basename(update_dir)}")
        return os.path.basename(update_dir)
    else:
        print(f"There are no folders with a start date greater than {lastimdate}. Using original folder.")
        return os.path.basename(ifgdir)


# Uso:
#ifgdir = "GEOCml1clipmask"
#ifgdir = update_ifgdir(ifgdir)
#print("Carpeta final a usar:", ifgdir)

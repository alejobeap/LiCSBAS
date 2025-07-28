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
    print("Última fecha en cum.h5:", lastimdate)

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
        print(f"No hay carpetas con fecha inicial mayor que {last_dates[0]}. Usando carpeta original.")
        return os.path.basename(ifgdir)

    print(f"Hay carpetas con fecha inicial mayor que {lastimdate}:")
    for folder in sorted(newer_folders):
        print(" -", folder)

    update_dir = ifgdir + "_update"
    os.makedirs(update_dir, exist_ok=True)

    # Enlazar carpetas con fechas relevantes
    link_ifg_folders(ifgdir, update_dir, last_dates)

    # Enlazar archivos sueltos
    link_loose_files(ifgdir, update_dir)

    print(f"\nUsando carpeta actualizada: {os.path.basename(update_dir)}")
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


import os
import h5py as h5

def update_ifgdir12_16(ifgdir):
    ifgdir = os.path.abspath(ifgdir)
    cumfile = os.path.join(os.path.dirname(ifgdir), 'TS_' + os.path.basename(ifgdir), 'cum.h5')

    if not os.path.exists(cumfile):
        print("No hay archivo cum.h5 antiguo")
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
        print(f"Se detectaron carpetas más recientes. Se sugiere usar: {os.path.basename(update_dir)}")
        return os.path.basename(update_dir)
    else:
        print(f"No hay carpetas con fecha inicial mayor que {lastimdate}. Se usa la original.")
        return os.path.basename(ifgdir)


# Uso:
#ifgdir = "GEOCml1clipmask"
#ifgdir = update_ifgdir(ifgdir)
#print("Carpeta final a usar:", ifgdir)

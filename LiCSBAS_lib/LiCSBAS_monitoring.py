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
    cumfile = os.path.join(os.path.dirname(ifgdir), 'TS_' + os.path.basename(ifgdir), 'cum.h5')

    if not os.path.exists(cumfile):
        print("No hay archivo cum.h5 antiguo")
        return os.path.basename(ifgdir)  # sin cambios

    print('\nLeyendo {}'.format(os.path.relpath(cumfile)))
    with h5.File(cumfile, 'r') as cumh5:
        imdates = cumh5['imdates'][()].astype(str).tolist()
    
    lastimdate = imdates[-1] if imdates else None
    print("Última fecha en cum.h5:", lastimdate)

    if lastimdate is None:
        print("No hay fechas en cum.h5")
        return os.path.basename(ifgdir)  # sin cambios

    # For copy the last 3 epcoh interferograms 
    lastimdate3 = imdates[-3]
    # Buscar carpetas con fecha inicial mayor que lastimdate
    
    carpetas_mayores = []
    for name in os.listdir(ifgdir):
        full_path = os.path.join(ifgdir, name)
        if os.path.isdir(full_path):
            try:
                start, end = name.split('_')
                if start > lastimdate3:
                    carpetas_mayores.append(name)
            except ValueError:
                continue

    if carpetas_mayores:
        print(f"Hay carpetas con fecha inicial mayor que {lastimdate}:")
        for c in sorted(carpetas_mayores):
            print(" -", c)

        update_dir = ifgdir + "_update"
        if not os.path.exists(update_dir):
            os.makedirs(update_dir)

        # Enlazar carpetas con lastimdate en su nombre
        for name in os.listdir(ifgdir):
            full_path = os.path.join(ifgdir, name)
            if os.path.isdir(full_path) and lastimdate in name:
                dst = os.path.join(update_dir, name)
                if not os.path.exists(dst):
                    os.symlink(full_path, dst)
                    print(f"Enlazado carpeta con fecha {lastimdate}: {name}")

        # Enlazar archivos sueltos
        for name in os.listdir(ifgdir):
            full_path = os.path.join(ifgdir, name)
            if os.path.isfile(full_path):
                dst = os.path.join(update_dir, name)
                if not os.path.exists(dst):
                    os.symlink(full_path, dst)
                    print(f"Enlazado archivo suelto: {name}")

        print(f"\nUsando carpeta actualizada: {os.path.basename(update_dir)}")
        return os.path.basename(update_dir)

    else:
        print(f"No hay carpetas con fecha inicial mayor que {lastimdate}. Usando carpeta original.")
        return os.path.basename(ifgdir)



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

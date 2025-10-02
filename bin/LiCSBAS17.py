

'''
Qué significa “usar la varianza” aquí?

Tú tienes:

Serie vieja (old cum.h5): acumulada hasta 2024.

Serie nueva (new cum.h5): empieza en 0 y acumula desde ahí.

El problema es:
¿Dónde enganchar los incrementos de la serie nueva sobre la vieja?

Como la serie nueva siempre empieza en 0, no podemos comparar directamente valores absolutos. Lo que hacemos es mirar los incrementos (pendientes):

De la vieja: diferencias entre sus últimos 5 puntos (old[-5:]).

De la nueva: el primer incremento (new[1] - new[0]).

La varianza en este contexto mide qué tan diferentes son esas pendientes.

Si la pendiente de la nueva se parece a la de cierto tramo de la vieja, entonces es buen lugar para unir.

La fecha con menor varianza → mejor ajuste.
'''

'''
si existe la posibilidad de sismos u otros eventos abruptos, la mejor práctica es:

Detectar saltos grandes en la serie vieja.

Ignorar esas fechas como candidatas de unión.

Forzar a unir en la parte estable posterior al salto.

si usas --varianza, ignore incrementos anómalos (outliers > 3σ),

y si no hay buena coincidencia, caiga de vuelta a -1 (última fecha)?

'''

import os
import sys
import h5py
import numpy as np
import argparse
import LiCSBAS_monitoring as monitoring_lib


def list_original_folders(path):
    return [
        name for name in os.listdir(path)
        if os.path.isdir(os.path.join(path, name)) and not name.endswith('_update')
    ]


def choose_union_index(ds_old, vals_new_valid, method="last", user_idx=None):
    """Decide dónde unir la nueva serie a la vieja."""
    n_old = len(ds_old)

    if method == "manual" and user_idx is not None:
        return user_idx  # ejemplo: -1 = última, -2 = penúltima

    if method == "varianza":
        # pendientes de la vieja (últimos 5 puntos)
        tail_old = ds_old[-5:].astype(np.float64)
        slopes_old = np.diff(tail_old, axis=0)

        # primer incremento de la nueva
        slope_new = vals_new_valid[1] - vals_new_valid[0] if len(vals_new_valid) > 1 else 0

        # detectar outliers en las pendientes viejas
        mean_s, std_s = np.mean(slopes_old), np.std(slopes_old)
        mask_valid = np.abs(slopes_old - mean_s) < 3 * std_s

        if not np.any(mask_valid):
            print(" Todas las pendientes fueron descartadas como outliers, usando última fecha")
            return -1

        valid_errors = (slopes_old[mask_valid] - slope_new) ** 2
        best_local_idx = np.argmin(valid_errors)

        # mapear índice local al global (dentro de los últimos 5)
        valid_indices = np.where(mask_valid)[0]
        best_idx_in_tail = valid_indices[best_local_idx]
        global_idx = n_old - 5 + best_idx_in_tail

        print(f" Unión elegida por varianza en índice global {global_idx} (fecha {global_idx - n_old})")
        return global_idx

    # por defecto: última fecha
    return -1


def merge_cum_files_continuous_accumulate(file_old, file_new, file_out, method="last", user_idx=None):
    with h5py.File(file_old, 'r') as f_old, h5py.File(file_new, 'r') as f_new:
        dates_old = f_old['imdates'][:]
        dates_new = f_new['imdates'][:]
        dates_new_valid = dates_new[~np.isin(dates_new, dates_old)]
        combined_dates = np.sort(np.concatenate((dates_old, dates_new_valid)))

        print(f"Total unique valid dates: {len(combined_dates)}")
        print("Combined dates:\n", combined_dates)

        temporal_dsets = ['bperp', 'cum', 'imdates']

        with h5py.File(file_out, 'w') as f_out:
            # copiar datasets estáticos
            for key in f_old.keys():
                if key not in temporal_dsets:
                    ds = f_old[key]
                    if ds.shape == ():
                        f_out.create_dataset(key, data=ds[()], dtype=ds.dtype)
                    else:
                        f_out.create_dataset(key, data=ds[:], dtype=ds.dtype)

            f_out.create_dataset('imdates', data=combined_dates, dtype='int32')

            for key in ['bperp', 'cum']:
                ds_old = f_old[key]
                ds_new = f_new[key]

                spatial_shape = ds_old.shape[1:]
                n_combined = len(combined_dates)
                dset_out = f_out.create_dataset(key, shape=(n_combined, *spatial_shape), dtype=ds_old.dtype)

                n_old = len(dates_old)
                dset_out[:n_old] = ds_old[:]

                idx_new_valid = np.where(np.isin(dates_new, dates_new_valid))[0]
                idx_combined_new = np.searchsorted(combined_dates, dates_new_valid)

                vals_new_valid = ds_new[idx_new_valid].astype(np.float64)

                # elegir punto base de unión
                union_idx = choose_union_index(ds_old, vals_new_valid, method=method, user_idx=user_idx)

                last_old_value = ds_old[union_idx].astype(np.float64)

                # trabajar con incrementos de la nueva
                increments = np.zeros_like(vals_new_valid)
                if len(vals_new_valid) > 0:
                    increments[0] = vals_new_valid[0]
                if len(vals_new_valid) > 1:
                    increments[1:] = vals_new_valid[1:] - vals_new_valid[:-1]

                accumulated = np.cumsum(increments, axis=0) + last_old_value

                for i, idx_c in enumerate(idx_combined_new):
                    dset_out[idx_c] = accumulated[i]

        print(" Combined data saved to:", file_out)


def main():
    parser = argparse.ArgumentParser(description="Combine LiCSBAS cum.h5 files for continuous accumulation")
    parser.add_argument("-t", "--tsadir", required=True, help="Time-series analysis directory (e.g., TS_GEOCml1clip)")
    parser.add_argument("--varianza", action="store_true", help="Elegir unión automáticamente minimizando varianza de pendientes")
    parser.add_argument("--union_idx", type=int, default=None, help="Elegir índice manual (-1 última, -2 penúltima, etc.)")

    args = parser.parse_args()
    tsadir = args.tsadir

    if not os.path.isdir(tsadir):
        print(f"Error: Directory not found: {tsadir}")
        sys.exit(1)

    print("Monitoring approach enabled")

    ifgdir = tsadir.replace("TS_", "")
    ifgdir = monitoring_lib.update_ifgdir12_16(ifgdir)
    ifgdir = ifgdir.replace("_update", "")
    tsadir = f"TS_{ifgdir}"
    tsadir_update = f"{tsadir}_update"

    oldfile = os.path.join(tsadir, "cum.h5")
    newfile = os.path.join(tsadir_update, "cum.h5")
    updatefile = os.path.join(tsadir, "cum_update.h5")

    if not os.path.exists(oldfile):
        print(f"Old file not found: {oldfile}")
        sys.exit(1)
    if not os.path.exists(newfile):
        print(f"New file not found: {newfile}")
        sys.exit(1)

    # decidir método
    if args.union_idx is not None:
        method = "manual"
    elif args.varianza:
        method = "varianza"
    else:
        method = "last"

    merge_cum_files_continuous_accumulate(oldfile, newfile, updatefile, method=method, user_idx=args.union_idx)


if __name__ == "__main__":
    main()

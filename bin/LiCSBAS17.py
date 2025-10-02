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


def compute_rms(vals_old, vals_new):
    """Calcula RMS entre dos series ya alineadas por offset."""
    if len(vals_old) == 0 or len(vals_new) == 0:
        return np.inf
    offset = vals_old[0] - vals_new[0]
    vals_new_aligned = vals_new + offset
    return np.sqrt(np.mean((vals_old - vals_new_aligned) ** 2))


def detect_step(vals, threshold_factor=2.0):
    """Detecta índice del primer step significativo"""
    diffs = np.diff(vals, axis=0)
    threshold = threshold_factor * np.std(diffs, axis=0)
    idx = np.where(np.abs(diffs) > threshold)[0]
    return idx[0] + 1 if len(idx) > 0 else None


def choose_union_index(dates_old, ds_old, dates_new, ds_new, method="varianza", user_idx=None, rms_threshold=None):
    """
    Decide dónde unir la nueva serie a la vieja.
    - Prioridad 1: step en ambas → unir al final de la vieja
    - Prioridad 2: step solo en vieja → unir justo antes del step
    - Prioridad 3: sin step → usar método ('varianza', 'manual', 'last')
    """
    step_idx_old = detect_step(ds_old)
    step_idx_new = detect_step(ds_new)

    # Caso 1: step en ambas
    if step_idx_old is not None and step_idx_new == step_idx_old:
        union_idx = len(ds_old) - 1
        print(f"Step en ambas series detectado. Unión en último índice de vieja: {union_idx}")
        return union_idx

    # Caso 2: step solo en vieja
    if step_idx_old is not None and (step_idx_new != step_idx_old):
        union_idx = step_idx_old - 1
        print(f"Step solo en vieja. Unión justo antes del step en índice: {union_idx}")
        return union_idx

    # Caso sin step: usar método
    n_old = len(dates_old)
    if method == "manual" and user_idx is not None:
        print(f"Método manual. Unión en índice {user_idx}")
        return user_idx  # ejemplo: -1 última, -2 penúltima

    if method == "varianza":
        # buscar solapamiento de fechas
        overlap = np.intersect1d(dates_old, dates_new)
        if len(overlap) == 0:
            print("⚠️ No hay solapamiento, usando última fecha")
            return n_old - 1

        idx_old_overlap = np.where(np.isin(dates_old, overlap))[0]
        idx_new_overlap = np.where(np.isin(dates_new, overlap))[0]

        vals_old_overlap = ds_old[idx_old_overlap].astype(np.float64)
        vals_new_overlap = ds_new[idx_new_overlap].astype(np.float64)

        if rms_threshold is None:
            incr_old = np.diff(vals_old_overlap, axis=0)
            incr_new = np.diff(vals_new_overlap, axis=0)
            rms_threshold = 2 * max(np.std(incr_old), np.std(incr_new))
            print(f"ℹ️ RMS threshold automático = {rms_threshold:.3f}")

        rms_full = compute_rms(vals_old_overlap, vals_new_overlap)
        print(f"📊 RMS en todo el solapamiento = {rms_full:.3f}")

        if rms_full < rms_threshold:
            best_idx = idx_old_overlap[0]
            print(f"✅ Unión en intersección (idx {best_idx})")
            return best_idx

        # probar descartando última/penúltima fecha
        for drop in [1, 2]:
            if len(idx_old_overlap) > drop:
                rms_drop = compute_rms(vals_old_overlap[:-drop], vals_new_overlap[:-drop])
                print(f"📊 RMS descartando últimas {drop} fechas old = {rms_drop:.3f}")
                if rms_drop < rms_threshold:
                    best_idx = idx_old_overlap[0]
                    print(f"⚠️ Últimas {drop} fechas old parecen outliers, unión en idx {best_idx}")
                    return best_idx

        print("❌ Solapamiento incoherente, usando última fecha old")
        return n_old - 1

    # Por defecto (si método 'last')
    return n_old - 1


def merge_cum_files_continuous_accumulate(file_old, file_new, file_out, method="varianza", user_idx=None):
    with h5py.File(file_old, 'r') as f_old, h5py.File(file_new, 'r') as f_new:
        dates_old = f_old['imdates'][:]
        dates_new = f_new['imdates'][:]
        dates_new_valid = dates_new[~np.isin(dates_new, dates_old)]
        combined_dates = np.sort(np.concatenate((dates_old, dates_new_valid)))

        print(f"Total unique valid dates: {len(combined_dates)}")

        temporal_dsets = ['bperp', 'cum', 'imdates']

        with h5py.File(file_out, 'w') as f_out:
            for key in f_old.keys():
                if key not in temporal_dsets:
                    ds = f_old[key]
                    if ds.shape == ():
                        f_out.create_dataset(key, data=ds[()], dtype=ds.dtype)
                    else:
                        f_out.create_dataset(key, data=ds[:], dtype=ds.dtype)

            f_out.create_dataset('imdates', data=combined_dates, dtype='int32')

            for key in ['bperp', 'cum']:
                ds_old = f_old[key].astype(np.float64)
                ds_new = f_new[key].astype(np.float64)

                spatial_shape = ds_old.shape[1:]
                n_combined = len(combined_dates)
                dset_out = f_out.create_dataset(key, shape=(n_combined, *spatial_shape), dtype=np.float64)

                n_old = len(dates_old)
                dset_out[:n_old] = ds_old[:]

                idx_new_valid = np.where(np.isin(dates_new, dates_new_valid))[0]
                idx_combined_new = np.searchsorted(combined_dates, dates_new_valid)

                vals_new_valid = ds_new[idx_new_valid]

                # Elegir punto base de unión con reglas de step
                union_idx = choose_union_index(dates_old, ds_old, dates_new, ds_new,
                                               method=method, user_idx=user_idx)
                last_old_value = ds_old[union_idx]

                increments = np.zeros_like(vals_new_valid)
                if len(vals_new_valid) > 0:
                    increments[0] = vals_new_valid[0]
                if len(vals_new_valid) > 1:
                    increments[1:] = vals_new_valid[1:] - vals_new_valid[:-1]

                accumulated = np.cumsum(increments, axis=0) + last_old_value

                for i, idx_c in enumerate(idx_combined_new):
                    dset_out[idx_c] = accumulated[i]

        print("✅ Combined data saved to:", file_out)


def main():
    parser = argparse.ArgumentParser(description="Combine LiCSBAS cum.h5 files with step detection and varianza default")
    parser.add_argument("-t", "--tsadir", required=True, help="Time-series analysis directory (e.g., TS_GEOCml1clip)")
    parser.add_argument("--varianza", action="store_true", help="Elegir unión automáticamente usando coherencia en solapamiento (opcional)")
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

    # decidir método: manual tiene prioridad, luego varianza por default
    if args.union_idx is not None:
        method = "manual"
    else:
        method = "varianza"

    merge_cum_files_continuous_accumulate(oldfile, newfile, updatefile, method=method, user_idx=args.union_idx)


if __name__ == "__main__":
    main()

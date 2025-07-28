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


def merge_cum_files_continuous_accumulate(file_old, file_new, file_out):
    with h5py.File(file_old, 'r') as f_old, h5py.File(file_new, 'r') as f_new:
        dates_old = f_old['imdates'][:]
        dates_new = f_new['imdates'][:]
        dates_new_valid = dates_new[~np.isin(dates_new, dates_old)]
        combined_dates = np.sort(np.concatenate((dates_old, dates_new_valid)))

        print(f"Total unique valid dates: {len(combined_dates)}")
        print("Combined dates:\n", combined_dates)

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
                ds_old = f_old[key]
                ds_new = f_new[key]

                spatial_shape = ds_old.shape[1:]
                n_combined = len(combined_dates)
                dset_out = f_out.create_dataset(key, shape=(n_combined, *spatial_shape), dtype=ds_old.dtype)

                n_old = len(dates_old)
                dset_out[:n_old] = ds_old[:]

                idx_new_valid = np.where(np.isin(dates_new, dates_new_valid))[0]
                idx_combined_new = np.searchsorted(combined_dates, dates_new_valid)

                last_old_value = ds_old[-1].astype(np.float64)
                vals_new_valid = ds_new[idx_new_valid].astype(np.float64)

                increments = np.zeros_like(vals_new_valid)
                increments[0] = vals_new_valid[0]
                if len(vals_new_valid) > 1:
                    increments[1:] = vals_new_valid[1:] - vals_new_valid[:-1]

                accumulated = np.cumsum(increments, axis=0) + last_old_value

                for i, idx_c in enumerate(idx_combined_new):
                    dset_out[idx_c] = accumulated[i]

        print("Combined data saved to:", file_out)


def main():
    parser = argparse.ArgumentParser(description="Combine LiCSBAS cum.h5 files for continuous accumulation")
    parser.add_argument("-t", "--tsadir", required=True, help="Time-series analysis directory (e.g., TS_GEOCml1clip)")

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
    print("Old file:",oldfile)
    newfile = os.path.join(tsadir_update, "cum.h5")
    print("New file:",newfile)
    updatefile = os.path.join(tsadir, "cum_update.h5")
    print("Update file:",updatefile)

    if not os.path.exists(oldfile):
        print(f"Old file not found: {oldfile}")
        sys.exit(1)
    if not os.path.exists(newfile):
        print(f"New file not found: {newfile}")
        sys.exit(1)

    merge_cum_files_continuous_accumulate(oldfile, newfile, updatefile)


if __name__ == "__main__":
    main()

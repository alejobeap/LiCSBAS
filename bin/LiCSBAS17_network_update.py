import os
import sys
import h5py
import numpy as np
import argparse
import LiCSBAS_monitoring as monitoring_lib
import argparse
import LiCSBAS_io_lib as io_lib
import LiCSBAS_tools_lib as tools_lib
import LiCSBAS_loop_lib as loop_lib
import LiCSBAS_plot_lib as plot_lib
import shutil
import datetime

def backup_file_if_exists(filepath):
    if os.path.exists(filepath):
        timestamp = datetime.datetime.now().strftime("%Y%m%d")
        backup_path = f"{filepath}.{timestamp}.bak"
        shutil.move(filepath, backup_path)
        print(f"Backup created: {backup_path}")

def read_first_column_from_file(filepath):
    """Read first column of a file, skipping headers/empty lines."""
    if not os.path.isfile(filepath):
        return []
    with open(filepath, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    return [line.split()[0] for line in lines[1:]]  # skip header


def collect_ifgs_from_info(info_dir):
    """Collect IFGs from info/13resid.txt inside a given directory."""
    resid_file = os.path.join(info_dir, "13resid.txt")
    return read_first_column_from_file(resid_file)


def main():
    parser = argparse.ArgumentParser(
        description="Create and update network LiCSBAS cum.h5 files for continuous accumulation"
    )
    parser.add_argument(
        "-t", "--tsadir",
        required=True,
        help="Time-series analysis directory (e.g., TS_GEOCml1clip)"
    )
    args = parser.parse_args()

    tsadir = args.tsadir
    if not os.path.isdir(tsadir):
        sys.exit(f"Error: Directory not found: {tsadir}")

    print("Monitoring approach enabled")

    # Update IFG directory name
    ifgdir = tsadir.replace("TS_", "")
    ifgdir = monitoring_lib.update_ifgdir12_16(ifgdir).replace("_update", "")
    tsadir = f"TS_{ifgdir}"
    tsadir_update = f"{tsadir}_update"

    infodir = os.path.join(tsadir, "info")
    infodir_update = os.path.join(tsadir_update, "info")

    # Collect IFGs from both directories
    all_ifg = set()
    all_ifg.update(collect_ifgs_from_info(infodir))
    all_ifg.update(collect_ifgs_from_info(infodir_update))
    all_ifg = sorted(all_ifg)

    ifgdir = os.path.abspath(ifgdir)
    if not os.path.isdir(tsadir):
        print(f"\nNo {tsadir} exists!", file=sys.stderr)
        return 1

    resultsdir = os.path.join(tsadir, "results")
    netdir = os.path.join(tsadir, "network")

    # Read bad IFG lists
    bad_ifg11 = io_lib.read_ifg_list(os.path.join(infodir, "11bad_ifg.txt"))
    bad_ifg12 = io_lib.read_ifg_list(os.path.join(infodir, "12bad_ifg.txt"))

    bad_ifg120_file = os.path.join(infodir, "120bad_ifg.txt")
    if os.path.exists(bad_ifg120_file):
        print("Adding also IFGs listed as bad in the optional 120 step")
        bad_ifg120 = io_lib.read_ifg_list(bad_ifg120_file)
        bad_ifg12 = list(set(bad_ifg12 + bad_ifg120))

    print("Adding also IFGs listed as no loop from file in info/12no_loop_ifg.txt")
    bad_ifg12no = io_lib.read_ifg_list(os.path.join(infodir, "12no_loop_ifg.txt"))

    bad_ifg_all = sorted(set(bad_ifg11 + bad_ifg12 + bad_ifg12no))

    # Remove bad IFGs
    ifgdates_all = io_lib.read_ifg_list(all_ifg)
    imdates_all = tools_lib.ifgdates2imdates(ifgdates_all)
    ifgdates = sorted(set(ifgdates_all) - set(bad_ifg_all))
    imdates = tools_lib.ifgdates2imdates(ifgdates)

    # Baselines
    bperp_file = os.path.join(ifgdir, "baselines")
    if os.path.exists(bperp_file):
        with open(bperp_file) as f:
            lines = [l.strip() for l in f if l.strip()]
        if len(lines) >= len(imdates):
            bperp = io_lib.read_bperp_file(bperp_file, imdates)
            bperp_all = io_lib.read_bperp_file(bperp_file, imdates_all)
        else:
            print("WARNING: Baselines file has fewer entries than needed. Using dummy values.")
            bperp = list(np.random.random(len(imdates)))
            bperp_all = list(np.random.random(len(imdates_all)))
    else:
        print("WARNING: Baselines file not found. Using dummy values.")
        bperp = list(np.random.random(len(imdates)))
        bperp_all = list(np.random.random(len(imdates_all)))

    # Plots
    os.makedirs(netdir, exist_ok=True)
    pngfile_all = os.path.join(netdir, "network13_all.png")
    backup_file_if_exists(pngfile_all)
    plot_lib.plot_network(ifgdates_all, bperp_all, [], pngfile_all)

    pngfile_bad = os.path.join(netdir, "network13.png")
    backup_file_if_exists(pngfile_bad)
    plot_lib.plot_network(ifgdates_all, bperp_all, bad_ifg_all, pngfile_bad)

    pngfile_nobad = os.path.join(netdir, "network13_nobad.png")
    backup_file_if_exists(pngfile_nobad)
    plot_lib.plot_network(ifgdates_all, bperp_all, bad_ifg_all, pngfile_nobad, plot_bad=False)



if __name__ == "__main__":
    main()

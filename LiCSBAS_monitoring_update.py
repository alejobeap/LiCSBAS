#!/usr/bin/env python3
#Based in some scripts from Original LiCSBAS
from matplotlib import pyplot as plt
import datetime
import os
import sys
import h5py
import numpy as np
import argparse
import LiCSBAS_monitoring as monitoring_lib
import shutil
import LiCSBAS_io_lib as io_lib
import LiCSBAS_tools_lib as tools_lib
import LiCSBAS_loop_lib as loop_lib
import LiCSBAS_plot_lib as plot_lib
import LiCSBAS_inv_lib as inv_lib
from matplotlib import dates as mdates
from matplotlib import colors
import datetime as dt



import os
import numpy as np
import h5py
import warnings

import os
import warnings
import numpy as np
import h5py
from datetime import date, timedelta

def merge_cum_files_auto_step(
    file_old,
    file_new,
    file_out,
    step_threshold=2.0,      # umbral absoluto del step (en unidades de 'cum', p.ej., mm)
    agg='median',            # 'median' o 'mean' para la serie representativa espacial
    mask=None,               # opcional: np.bool_(Y,X) para limitar la estadística espacial
    qc_pixel=None,           # opcional: tupla (y, x) para guardar gráfico de verificación
    mark_steps=True,         # dibujar líneas verticales en los steps detectados
    common_window_days=30,   # ***NUEVO***: ventana final (en días) para detectar steps cerca del fin de OLD
    overwrite=True,
    return_debug=False
):
    """
    Une dos cubos LiCSBAS (cum.h5) aplicando detección automática de step en una
    PEQUEÑA ventana temporal común al final del histórico (OLD) y reglas de unión:

      Reglas (primer step significativo por serie en la ventana, |Δ| >= step_threshold):
        - Si BOTH (OLD y NEW) tienen step → unir DESPUÉS del step en OLD.
        - Si SOLO OLD tiene step → unir INMEDIATAMENTE ANTES del step en OLD.
        - Si NO hay steps → unir al FINAL de OLD.

      NEW solo se usa para EXTENDER desde la unión hacia adelante (no se usa el solapamiento).
      La NEW se alinea por desplazamiento para garantizar continuidad en la fecha de unión.
      El inicio en NEW se elige como la primera fecha estrictamente posterior a la última
      fecha usada de OLD, para mantener imdates estrictamente crecientes.

    Parámetros clave:
      step_threshold: umbral absoluto para detectar el step (misma unidad que 'cum').
      agg:            'median' (robusto) o 'mean' para la serie temporal representativa.
      mask:           bool (Y,X). Si se da, la estadística espacial se calcula sobre mask=True.
      qc_pixel:       (y,x) para crear figura 'before/after' con la línea de unión.
      mark_steps:     si True, marca los steps detectados en el gráfico QC.
      common_window_days: ***NUEVO*** Nº de días antes del fin de OLD que se usan para buscar steps.

    Devuelve:
      Si return_debug=True, además de escribir el archivo, retorna un diccionario con detalles.
    """

    # ---------------- Helpers (fechas y series) ----------------
    def _imdate_int_to_date(i):
        s = f"{int(i):08d}"
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

    def _date_to_imdate_int(d):
        return int(d.strftime("%Y%m%d"))

    def _spatial_series(cube, mask, agg):
        # cube: (T, Y, X)
        T = cube.shape[0]
        s = np.full(T, np.nan, dtype=np.float64)
        if mask is not None:
            if mask.shape != cube.shape[1:]:
                raise ValueError("La 'mask' debe tener forma (Y, X) idéntica a los cubos.")
            my, mx = np.where(mask)
        for t in range(T):
            arr = cube[t].astype(np.float64)
            if mask is not None:
                vals = arr[my, mx]
            else:
                vals = arr.ravel()
            vals = vals[np.isfinite(vals)]
            if vals.size == 0:
                s[t] = np.nan
            else:
                if agg == 'median':
                    s[t] = np.nanmedian(vals)
                elif agg == 'mean':
                    s[t] = np.nanmean(vals)
                else:
                    raise ValueError("agg debe ser 'median' o 'mean'.")
        return s

    def _first_step_index(series, thresh):
        """
        Retorna i tal que el step ocurre entre i y i+1 (primer |diff| >= thresh).
        Si no hay, None. Ignora diffs no finitos.
        """
        diffs = np.diff(series)
        if diffs.size == 0:
            return None
        ok = np.isfinite(diffs) & (np.abs(diffs) >= float(thresh))
        idxs = np.where(ok)[0]
        return int(idxs[0]) if idxs.size > 0 else None

    # ---------------- Carga ----------------
    print("LOADING FILES")
    with h5py.File(file_old, 'r') as f_old, h5py.File(file_new, 'r') as f_new:
        dates_old = f_old["imdates"][:].astype(np.int64)   # (T_old,)
        dates_new = f_new["imdates"][:].astype(np.int64)   # (T_new,)

        cum_old = f_old["cum"][:]  # (T_old, Y, X)
        cum_new = f_new["cum"][:]  # (T_new, Y, X)

        # Chequeos básicos
        if cum_old.shape[1:] != cum_new.shape[1:]:
            raise ValueError("Las dimensiones espaciales (Y,X) de OLD y NEW no coinciden.")
        if dates_old.ndim != 1 or dates_new.ndim != 1:
            raise ValueError("imdates debe ser 1D en ambos archivos.")
        if not (np.all(np.diff(dates_old) > 0) and np.all(np.diff(dates_new) > 0)):
            raise ValueError("imdates en OLD y NEW deben ser estrictamente crecientes.")

        # Serie temporal representativa (robusta a NaNs)
        s_old = _spatial_series(cum_old, mask, agg)
        s_new = _spatial_series(cum_new, mask, agg)

        # ==============================
        # *** Ventana común final ***
        # ==============================
        end_old_int = int(dates_old[-1])
        end_old_dt  = _imdate_int_to_date(end_old_int)
        start_win_dt = end_old_dt - timedelta(days=int(common_window_days))
        start_win_int = _date_to_imdate_int(start_win_dt)

        # Máscara restringida SOLO a la ventana [end_old - N, end_old]
        # Nota: esto ya fuerza a mirar hacia el final de OLD.
        mask_old_win = (dates_old >= start_win_int) & (dates_old <= end_old_int)
        mask_new_win = (dates_new >= start_win_int) & (dates_new <= end_old_int)

        # Si NEW no tiene datos en la ventana, su step se considera "no detectado"
        s_old_win = s_old[mask_old_win]
        s_new_win = s_new[mask_new_win]

        # Detección de steps SOLO en la ventana final
        step_idx_old_local = _first_step_index(s_old_win, step_threshold) if s_old_win.size >= 2 else None
        step_idx_new_local = _first_step_index(s_new_win, step_threshold) if s_new_win.size >= 2 else None

        # Mapear a índices globales (en todo OLD/NEW)
        if step_idx_old_local is not None:
            step_idx_old = np.where(mask_old_win)[0][step_idx_old_local]
        else:
            step_idx_old = None

        if step_idx_new_local is not None:
            step_idx_new = np.where(mask_new_win)[0][step_idx_new_local]
        else:
            step_idx_new = None

        # ==============================
        # Decisión de unión según reglas
        # ==============================
        if (step_idx_old is not None) and (step_idx_new is not None):
            rule = "both_have_step → merge_after_old_step"
            idx_old_keep_end = step_idx_old + 1  # primer sample POST-step
        elif (step_idx_old is not None) and (step_idx_new is None):
            rule = "only_old_has_step → merge_immediately_before_old_step"
            idx_old_keep_end = step_idx_old      # último sample PRE-step
        else:
            rule = "no_steps_in_window → merge_at_end_of_old"
            idx_old_keep_end = len(dates_old) - 1  # fin de OLD

        # Seguridad
        idx_old_keep_end = int(np.clip(idx_old_keep_end, -1, len(dates_old) - 1))
        merge_date_old = dates_old[idx_old_keep_end] if idx_old_keep_end >= 0 else None

        # NEW comienza en la primera fecha estrictamente MAYOR a la fecha de unión
        if merge_date_old is None:
            idx_new_start = 0
        else:
            idx_new_start = int(np.searchsorted(dates_new, merge_date_old, side='right'))

        # Logging
        print("=== CONFIG VENTANA ===")
        print(f"Ventana común final (días): {common_window_days}")
        print(f"Ventana usada: [{start_win_int} .. {end_old_int}]")
        print("=== DECISIÓN DE UNIÓN ===")
        print(f"Regla aplicada: {rule}")
        print(f"step_idx_old(global): {step_idx_old}, step_idx_new(global): {step_idx_new}")
        print(f"idx_old_keep_end (última fecha OLD usada): {idx_old_keep_end} (date={merge_date_old})")
        print(f"idx_new_start (primera fecha NEW > merge_date_old): {idx_new_start} "
              f"(date={dates_new[idx_new_start] if idx_new_start < len(dates_new) else 'N/A'})")

        # ---------------- Construcción de fechas combinadas ----------------
        dates_old_chunk = dates_old[:idx_old_keep_end+1] if idx_old_keep_end >= 0 else np.array([], dtype=np.int64)
        if idx_new_start >= len(dates_new):
            warnings.warn("NEW no contiene fechas posteriores a la fecha de unión. El resultado contendrá solo OLD.")
            dates_new_chunk = np.array([], dtype=np.int64)
        else:
            dates_new_chunk = dates_new[idx_new_start:]

        combined_dates = np.concatenate([dates_old_chunk, dates_new_chunk]).astype(np.int64)
        if combined_dates.size > 1 and not np.all(np.diff(combined_dates) > 0):
            raise AssertionError("combined_dates no es estrictamente creciente. Revisa la lógica de idx_new_start.")

        print(f"OLD chunk: {len(dates_old_chunk)} dates | NEW chunk: {len(dates_new_chunk)} dates")
        print(f"Total COMBINED dates: {len(combined_dates)}")

        # ---------------- Alineación NEW en la frontera ----------------
        Y, X = cum_old.shape[1:]
        if idx_new_start < len(dates_new):
            if idx_old_keep_end >= 0:
                base_val = cum_old[idx_old_keep_end].astype(np.float64)   # (Y, X)
            else:
                base_val = np.zeros((Y, X), dtype=np.float64)

            new_anchor = cum_new[idx_new_start].astype(np.float64)
            aligned_new = base_val + (cum_new[idx_new_start:].astype(np.float64) - new_anchor)
            aligned_new = aligned_new.astype(np.float32)
        else:
            aligned_new = None  # no habrá NEW en la salida

        # ---------------- Escritura del archivo de salida ----------------
        if os.path.exists(file_out) and not overwrite:
            raise FileExistsError(f"{file_out} exists and overwrite=False")

        print("WRITING OUTPUT FILE")
        with h5py.File(file_out, "w") as f_out:
            # Copiar metadatos de OLD (excepto cum, bperp, imdates)
            for key in f_old.keys():
                if key not in ["cum", "bperp", "imdates"]:
                    d = f_old[key]
                    if d.shape == ():  # escalar
                        f_out.create_dataset(key, data=d[()])
                    else:
                        f_out.create_dataset(key, data=d[:])

            # Fechas combinadas
            f_out.create_dataset("imdates", data=combined_dates.astype(np.int32), dtype="int32")

            # Dataset 'cum'
            T2 = len(combined_dates)
            dset_out = f_out.create_dataset("cum", shape=(T2, Y, X), dtype="float32")

            # OLD hasta idx_old_keep_end
            if idx_old_keep_end >= 0:
                dset_out[:idx_old_keep_end+1] = cum_old[:idx_old_keep_end+1].astype(np.float32)

            # NEW alineada desde idx_new_start
            if aligned_new is not None and aligned_new.shape[0] > 0:
                dset_out[idx_old_keep_end+1:] = aligned_new

            # bperp combinado (NaN para OLD chunk, NEW desde idx_new_start si existe)
            if "bperp" in f_new.keys():
                bperp_old = np.full((len(dates_old_chunk),), np.nan, dtype=np.float32)
                if idx_new_start < len(dates_new):
                    bperp_new = f_new["bperp"][:].astype(np.float32)
                    bperp_new_chunk = bperp_new[idx_new_start:]
                else:
                    bperp_new_chunk = np.array([], dtype=np.float32)
                bperp_comb = np.concatenate([bperp_old, bperp_new_chunk]).astype(np.float32)
                f_out.create_dataset("bperp", data=bperp_comb)

            # Metadatos útiles de decisión
            f_out.attrs["merge_rule"] = rule
            f_out.attrs["step_threshold"] = float(step_threshold)
            f_out.attrs["common_window_days"] = int(common_window_days)
            f_out.attrs["step_idx_old_global"] = -1 if step_idx_old is None else int(step_idx_old)
            f_out.attrs["step_idx_new_global"] = -1 if step_idx_new is None else int(step_idx_new)
            f_out.attrs["idx_old_keep_end"] = int(idx_old_keep_end)
            f_out.attrs["idx_new_start"] = int(idx_new_start)
            f_out.attrs["window_start_int"] = int(start_win_int)
            f_out.attrs["window_end_int"] = int(end_old_int)

    print("Merged (auto-step, windowed) saved to:")
    print(" ", file_out)
    print("DONE")

    # ---------------- QC opcional ----------------
    try:
        import matplotlib.pyplot as plt

        # Panel 1: BEFORE (series representativas y ventana)
        fig, axs = plt.subplots(1, 2, figsize=(12.0, 4.5), dpi=160)

        axs[0].plot(dates_old, s_old, 'o-', label=f'OLD ({agg})')
        axs[0].plot(dates_new, s_new, 'o-', label=f'NEW ({agg})')
        # sombrear ventana usada
        axs[0].axvspan(start_win_int, end_old_int, color='yellow', alpha=0.15, label='Ventana step')
        # marcar steps (si están en global)
        if mark_steps and (step_idx_old is not None):
            v = dates_old[min(step_idx_old+1, len(dates_old)-1)]
            axs[0].axvline(v, color='r', ls='--', lw=1.2, label='Step OLD')
        if mark_steps and (step_idx_new is not None):
            v = dates_new[min(step_idx_new+1, len(dates_new)-1)]
            axs[0].axvline(v, color='m', ls='--', lw=1.2, label='Step NEW')
        axs[0].set_title('Before merge (series espaciales)')
        axs[0].set_xlabel('Time (imdates)')
        axs[0].set_ylabel('Displacement (units)')
        axs[0].legend()

        # Panel 2: AFTER (px o espacial)
        # Construcción de serie merged (para mostrar)
        if aligned_new is not None:
            # reconstruir serie representativa merged
            s_new_anchor = s_new[idx_new_start] if (idx_new_start < len(s_new)) else np.nan
            base_val = s_old[idx_old_keep_end] if idx_old_keep_end >= 0 else 0.0
            s_new_aligned = (s_new[idx_new_start:] - s_new_anchor) + base_val
            s_merged = np.concatenate([
                s_old[:idx_old_keep_end+1] if idx_old_keep_end >= 0 else np.array([]),
                s_new_aligned
            ])
            merged_dates_for_plot = np.concatenate([dates_old[:idx_old_keep_end+1], dates_new[idx_new_start:]])
        else:
            s_merged = s_old[:idx_old_keep_end+1]
            merged_dates_for_plot = dates_old[:idx_old_keep_end+1]

        axs[1].plot(merged_dates_for_plot, s_merged, 'o-', label='Merged (espacial)')
        axs[1].axvline(merge_date_old, color='r', ls='--', label='Time merged')
        axs[1].set_title('After merge')
        axs[1].set_xlabel('Time (imdates)')
        axs[1].set_ylabel('Displacement (units)')
        axs[1].legend()

        # Si se pidió un píxel concreto, guardamos figura adicional por píxel
        if qc_pixel is not None:
            y, x = qc_pixel
            old_px = cum_old[:, y, x]
            new_px = cum_new[:, y, x]
            if aligned_new is not None:
                merged_px = np.concatenate([
                    old_px[:idx_old_keep_end+1] if idx_old_keep_end >= 0 else np.array([]),
                    aligned_new[:, y, x]
                ])
                mdp = np.concatenate([dates_old[:idx_old_keep_end+1], dates_new[idx_new_start:]])
            else:
                merged_px = old_px[:idx_old_keep_end+1]
                mdp = dates_old[:idx_old_keep_end+1]

            fig2, ax2 = plt.subplots(1, 1, figsize=(9.5, 4.0), dpi=160)
            ax2.plot(dates_old, old_px, '.', color='0.7', ms=4, label='OLD px')
            ax2.plot(dates_new, new_px, '.', color='0.7', ms=4, label='NEW px')
            ax2.plot(mdp, merged_px, 'o-', label='Merged px')
            ax2.axvspan(start_win_int, end_old_int, color='yellow', alpha=0.15, label='Ventana step')
            ax2.axvline(merge_date_old, color='r', ls='--', label='Time merged')
            ax2.set_title(f'QC píxel ({y},{x})')
            ax2.set_xlabel('Time (imdates)')
            ax2.set_ylabel('Displacement (units)')
            ax2.legend()
            out_png_px = os.path.join(os.path.dirname(file_out), f"merge_qc_auto_step_px_{y}_{x}.png")
            fig2.tight_layout(); fig2.savefig(out_png_px); plt.close(fig2)
            print(f"QC pixel figure saved: {out_png_px}")

        out_png = os.path.join(os.path.dirname(file_out), "merge_qc_auto_step_window.png")
        fig.tight_layout(); fig.savefig(out_png); plt.close(fig)
        print(f"QC figure saved: {out_png}")

    except Exception as e:
        print("QC plot failed:", e)

    if return_debug:
        return {
            "rule": rule,
            "step_threshold": step_threshold,
            "common_window_days": common_window_days,
            "window_start_int": int(start_win_int),
            "window_end_int": int(end_old_int),
            "step_idx_old_global": -1 if step_idx_old is None else int(step_idx_old),
            "step_idx_new_global": -1 if step_idx_new is None else int(step_idx_new),
            "idx_old_keep_end": int(idx_old_keep_end),
            "merge_date_old": None if merge_date_old is None else int(merge_date_old),
            "idx_new_start": int(idx_new_start),
            "old_chunk_len": int(len(dates_old_chunk)),
            "new_chunk_len": int(len(dates_new_chunk)),
        }        
#%%
def plot_network(ifgdates, bperp, rm_ifgdates, pngfile, plot_bad=True, label_name='Removed IFG'):
    """
    Plot network of interferometric pairs.
    
    bperp can be dummy (-1~1).
    Suffix of pngfile can be png, ps, pdf, or svg.
    plot_bad
        True  : Plot bad ifgs by red lines
        False : Do not plot bad ifgs
    """
    if label_name is None:
        label_name = 'Removed IFG'

    imdates_all = tools_lib.ifgdates2imdates(ifgdates)
    n_im_all = len(imdates_all)
    imdates_dt_all = np.array(([dt.datetime.strptime(imd, '%Y%m%d') for imd in imdates_all])) ##datetime

    ifgdates = list(set(ifgdates)-set(rm_ifgdates))
    ifgdates.sort()
    imdates = tools_lib.ifgdates2imdates(ifgdates)
    n_im = len(imdates)
    imdates_dt = np.array(([dt.datetime.strptime(imd, '%Y%m%d') for imd in imdates])) ##datetime
    
    ### Identify gaps    
    G = inv_lib.make_sb_matrix(ifgdates)
    ixs_inc_gap = np.where(G.sum(axis=0)==0)[0]
    
    ### Plot fig
    figsize_x = np.round(((imdates_dt_all[-1]-imdates_dt_all[0]).days)/80)+2
    fig = plt.figure(figsize=(figsize_x, 6))
    ax = fig.add_axes([0.06, 0.12, 0.92,0.85])
    
    ### IFG blue lines
    for i, ifgd in enumerate(ifgdates):
        ix_m = imdates_all.index(ifgd[:8])
        ix_s = imdates_all.index(ifgd[-8:])
        label = 'IFG' if i==0 else '' #label only first
        plt.plot([imdates_dt_all[ix_m], imdates_dt_all[ix_s]], [bperp[ix_m],
                bperp[ix_s]], color='b', alpha=0.6, zorder=2, label=label, lw=1)

    ### IFG bad red lines
    if plot_bad:
        for i, ifgd in enumerate(rm_ifgdates):
            try:
             ix_m = imdates_all.index(ifgd[:8])
             ix_s = imdates_all.index(ifgd[-8:])
            except ValueError:
             continue
            label = label_name if i==0 else '' #label only first
            plt.plot([imdates_dt_all[ix_m], imdates_dt_all[ix_s]], [bperp[ix_m],
                    bperp[ix_s]], color='r', alpha=0.6, zorder=6, label=label, lw=1)

    ### Image points and dates
    ax.scatter(imdates_dt_all, bperp, alpha=0.6, zorder=4)
    for i in range(n_im_all):
        if i % 5 != 0:
           continue
#        if bperp[i] > np.median(bperp): 
        if bperp[i] > 0:
            va='bottom'
            offset = (+8)
        else: 
            va = 'top' 
            offset = (-8)
#        ax.annotate(imdates_all[i][4:6]+'/'+imdates_all[i][6:],
#                    (imdates_dt_all[i], bperp[i]), ha='center', va=va, zorder=8, fontsize=10, bbox=dict(facecolor="white", edgecolor="none", pad=0.01, alpha=0.7))


        ax.annotate(imdates_all[i][4:6]+'/'+imdates_all[i][6:],(imdates_dt_all[i], bperp[i]), ha='center', va=va, xytext=(0, offset), textcoords="offset points", zorder=8, fontsize=10, bbox=dict(facecolor="white", edgecolor="none", pad=0.01, alpha=0.7))


    ### gaps
    if len(ixs_inc_gap)!=0:
        gap_dates_dt = []
        for ix_gap in ixs_inc_gap:
            ddays_td = imdates_dt[ix_gap+1]-imdates_dt[ix_gap]
            gap_dates_dt.append(imdates_dt[ix_gap]+ddays_td/2)
        plt.vlines(gap_dates_dt, 0, 1, transform=ax.get_xaxis_transform(),
                   zorder=1, label='Gap', alpha=0.6, colors='k', linewidth=3)
        
    ### Locater        
#    loc = ax.xaxis.set_major_locator(mdates.AutoDateLocator())
#    loc = ax.xaxis.set_major_locator(mdates.YearLocator())

    loc = mdates.YearLocator()

    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(
        mdates.ConciseDateFormatter(loc)
    )


    try:  # Only support from Matplotlib 3.1
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(loc))
    except:
        #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d'))
        locator = mdates.AutoDateLocator()

        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(
            mdates.ConciseDateFormatter(locator)
        )
        for label in ax.get_xticklabels():
            label.set_rotation(20)
            label.set_horizontalalignment('right')
    ax.grid(which='major')

    ### Add bold line every 1yr
    ax.xaxis.set_minor_locator(mdates.YearLocator())
    ax.grid(which='minor', linewidth=2)

    ax.set_xlim((imdates_dt_all[0]-dt.timedelta(days=15),
                 imdates_dt_all[-1]+dt.timedelta(days=15)))

    ### Labels and legend
    plt.xlabel('Time')
    if np.all(np.abs(np.array(bperp))<=1): ## dummy
        plt.ylabel('dummy')
    else:
        plt.ylabel('Bperp [m]')
    
#    plt.legend()
    plt.legend(loc='best', frameon=True)

    ### Save
    plt.savefig(pngfile, bbox_inches='tight')
    plt.close()

    return len(ixs_inc_gap)

def combine_baselines(folder1, folder2):
    """
    Combine baselines from TWO TS folders.
    - Reads baselines from both folders
    - Removes duplicated lines
    - Sorts everything
    - Returns a temporary unified baseline file
    """

    import tempfile

    # Convert TS folder IFG folder name
    ifgdir1 = folder1#.replace("TS_", "")
    ifgdir2 = folder2#.replace("TS_", "")

    base1 = os.path.join(ifgdir1, "baselines")
    base2 = os.path.join(ifgdir2, "baselines")

    lines = []

    # Read folder1 baselines
    if os.path.exists(base1):
        with open(base1) as f:
            lines += [l.strip() for l in f if l.strip()]

    # Read folder2 baselines
    if os.path.exists(base2):
        with open(base2) as f:
            lines += [l.strip() for l in f if l.strip()]

    # Remove duplicates and sort
    lines = sorted(set(lines))

    # Write unified baselines to temporary file
    tmpfile = tempfile.NamedTemporaryFile(delete=False, mode="w")
    for l in lines:
        tmpfile.write(l + "\n")
    tmpfile.close()

#    print(f"Unified baselines file created â†’ {tmpfile.name}")
    return tmpfile.name


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

def create_network_from_two_ts(folder1, folder2, outdir):
    """
    Create a network ONLY using IFGs from folder1/info and folder2/info.
    Stores results in outdir/network.
    """

    print("\n=== BUILDING NETWORK ONLY ===")
#    print("FOLDER1:", folder1)
#    print("FOLDER2:", folder2)
#    print("OUTDIR :", outdir)

    infodir1 = os.path.join(folder1, "info")
    infodir2 = os.path.join(folder2, "info")

    # 1. Collect IFGs from both folders
    all_ifg = set()
    all_color = set()
    all_ifg.update(collect_ifgs_from_info(infodir1))   # => list of IFG strings
    all_ifg.update(collect_ifgs_from_info(infodir2))
    all_color.update(collect_ifgs_from_info(infodir2))
    all_color = sorted(all_color)
    all_ifg = sorted(all_ifg)

    print(f"Total IFGs collected: {len(all_ifg)}")

    # Output dirs
    outdir = os.path.abspath(outdir)
    netdir = os.path.join(outdir, "network")
    resultsdir = os.path.join(outdir, "results")
    os.makedirs(netdir, exist_ok=True)
    os.makedirs(resultsdir, exist_ok=True)

    # 2. Read BAD IFGs (ONLY FROM folder1/info)
    bad_ifg11 = io_lib.read_ifg_list(os.path.join(infodir1, "11bad_ifg.txt"))
    bad_ifg12 = io_lib.read_ifg_list(os.path.join(infodir1, "12bad_ifg.txt"))
    bad120_file = os.path.join(infodir1, "120bad_ifg.txt")
    if os.path.exists(bad120_file):
        print("Adding also 120bad_ifg.txt")
        bad_ifg120 = io_lib.read_ifg_list(bad120_file)
        bad_ifg12 = list(set(bad_ifg12 + bad_ifg120))

    bad_ifg12no_file = os.path.join(infodir1, "12no_loop_ifg.txt")

    if os.path.exists(bad_ifg12no_file):
         bad_ifg12no = io_lib.read_ifg_list(bad_ifg12no_file)
    else:
        bad_ifg12no = []

    bad_ifg_all = sorted(set(bad_ifg11 + bad_ifg12 + bad_ifg12no))

    # 3. Convert IFG strings â†’ imdates
    ifgdates_all = all_ifg
    imdates_all = tools_lib.ifgdates2imdates(ifgdates_all)

    # Remove bad IFGs
    ifgdates_good = sorted(set(ifgdates_all) - set(bad_ifg_all))
    imdates_good  = tools_lib.ifgdates2imdates(ifgdates_good)

    # 4. Baselines
    #ifgdir = folder1.replace("TS_", "")
    #bperp_file = os.path.join(ifgdir, "baselines")
    bperp_file = combine_baselines(folder1, folder2)

    if os.path.exists(bperp_file):
        with open(bperp_file) as f:
            lines = [l.strip() for l in f if l.strip()]

        if len(lines) >= len(imdates_good):
            bperp      = io_lib.read_bperp_file(bperp_file, imdates_good)
            bperp_all  = io_lib.read_bperp_file(bperp_file, imdates_all)
        else:
            print("WARNING: Baselines incomplete dummy values used.")
            bperp      = list(np.random.random(len(imdates_good)))
            bperp_all  = list(np.random.random(len(imdates_all)))

    else:
        print("WARNING: No baselines found dummy values used.")
        bperp      = list(np.random.random(len(imdates_good)))
        bperp_all  = list(np.random.random(len(imdates_all)))

    # 5. NETWORK plots
    print("Plotting network...")

    png_all = os.path.join(netdir, "network13_all.png")
#    backup_file_if_exists(png_all)
    plot_network(ifgdates_all, bperp_all, [], png_all)

    png_bad = os.path.join(netdir, "network13.png")
#    backup_file_if_exists(png_bad)
    plot_network(ifgdates_all, bperp_all, bad_ifg_all, png_bad)

    png_nobad = os.path.join(netdir, "network13_nobad.png")
#    backup_file_if_exists(png_nobad)
    plot_network(ifgdates_all, bperp_all, bad_ifg_all, png_nobad, plot_bad=False)

    png_nocolor = os.path.join(netdir, "network13_color.png")
#    backup_file_if_exists(png_nobad)
    plot_network(ifgdates_all, bperp_all, all_color, png_nocolor, plot_bad=True, label_name="Updated")


    print("\n=== NETWORK DONE ===")    
    

def copy_results(folder1, outfolder):

 results_src = os.path.join(folder1, "results")
 results_dst = os.path.join(outfolder, "results")

 results_srcinfo = os.path.join(folder1, "info")
 results_dstinfo = os.path.join(outfolder, "info")


 if not os.path.isdir(results_src):
    print("No results directory in", folder1)
    return

 os.makedirs(results_dst, exist_ok=True)
 os.makedirs(results_dstinfo, exist_ok=True)

 files_to_copy = [
    "mask",
    "coh_avg",
    "n_unw",
    "vstd",
    "maxTlen",
    "n_gap",
    "stc",
    "n_ifg_noloop",
    "n_loop_err",
    "resid_rms",
    "slc.mli",
    "hgt",
    "hgt.geo.tif"
 ]

 files_to_copy1 = [
    "EQA.dem_par",
 ]

 for f in files_to_copy:

    src = os.path.join(results_src, f)
    dst = os.path.join(results_dst, f)

    if os.path.exists(src):
        shutil.copy2(src, dst)
        print("Copied:", f)
    else:
        print("Missing:", f)

 print("Results files copied to", results_dst)

 for f in files_to_copy1:

    src = os.path.join(results_srcinfo, f)
    dst = os.path.join(results_dstinfo, f)

    if os.path.exists(src):
        shutil.copy2(src, dst)
        print("Copied:", f)
    else:
        print("Missing:", f)

 print("Results files copied to", results_dstinfo)


# STEP DETECTION (same concept as synthetic example)

def detect_step_cube(vals, min_step=2.0):
    """
    Detects the first time index where a step occurs in a 3D cube vals[t, y, x].
    Step is defined by spatial-mean absolute temporal difference > min_step.
    Returns the index t (same definition as your synthetic script).
    """
    diffs = vals[1:] - vals[:-1]                      # shape (T-1, Y, X)
    
    p90 = np.nanpercentile(np.abs(diffs), 90, axis=(1,2))
    idx = np.where(p90 > min_step)[0]

    return idx[0] + 1 if len(idx) > 0 else None



def merge_cum_files_boundary_align(
    file_old,
    file_new,
    file_out,
    qc_pixel=None,   # opcional: tupla (y, x) para generar grÃ¡fico de verificaciÃ³n
    overwrite=True
):
    """
    Une dos cubos LiCSBAS (cum.h5) garantizando continuidad en la 'lÃ­nea de uniÃ³n':
      - Usa OLD hasta la Ãºltima fecha strictly < primera fecha de NEW.
      - Desde la primera fecha de NEW, usa NEW alineado para que no haya salto.
    """
    print("LOADING FILES")
    with h5py.File(file_old, 'r') as f_old, h5py.File(file_new, 'r') as f_new:
        dates_old = f_old["imdates"][:].astype(np.int64)   # (T_old,)
        dates_new = f_new["imdates"][:].astype(np.int64)   # (T_new,)

        cum_old = f_old["cum"][:]  # (T_old, Y, X)
        cum_new = f_new["cum"][:]  # (T_new, Y, X)

        # 1) Fecha de uniÃ³n = primera fecha de NEW
        d0_new = dates_new.min()
        idx_new_start = np.argmin(np.abs(dates_new - d0_new))  # deberÃ­a ser 0

        # 2) Ãndice de OLD anterior a esa fecha
        old_before_mask = dates_old < d0_new
        if np.any(old_before_mask):
            idx_old_keep_end = np.where(old_before_mask)[0][-1]  # Ãºltimo OLD < primera NEW
        else:
            # OLD no tiene fechas anteriores -> no hay "antes"; unimos desde el comienzo
            idx_old_keep_end = -1

        print(f"Union date (first NEW): {d0_new}")
        print(f"Using OLD up to index: {idx_old_keep_end} (date={dates_old[idx_old_keep_end] if idx_old_keep_end>=0 else 'N/A'})")

        # 3) Valores base y desplazamiento para alinear NEW
        #    base_val: OLD en la fecha inmediatamente anterior (o nada si -1)
        if idx_old_keep_end >= 0:
            base_val = cum_old[idx_old_keep_end].astype(np.float64)   # (Y, X)
        else:
            # Si no hay OLD antes, usamos 0 como base para que NEW "defina" la referencia
            base_val = np.zeros_like(cum_new[0], dtype=np.float64)

        offset = cum_new[idx_new_start].astype(np.float64)            # NEW en su primera fecha
        vals_new_shifted = cum_new[idx_new_start:] - offset           # NEW referida a su t0
        aligned_new = base_val + vals_new_shifted                     # NEW alineada a OLD

        # 4) Construimos fechas combinadas (OLD<d0_new) + (NEW>=d0_new)
        dates_old_chunk = dates_old[:idx_old_keep_end+1] if idx_old_keep_end >= 0 else np.array([], dtype=np.int64)
        dates_new_chunk = dates_new[idx_new_start:]  # desde la primera de NEW

        combined_dates = np.concatenate([dates_old_chunk, dates_new_chunk])
        # Sanity: deben ir en orden creciente por construcciÃ³n
        assert np.all(np.diff(combined_dates) > 0), "combined_dates no es estrictamente creciente. Revisa las fechas."

        print(f"OLD chunk: {len(dates_old_chunk)} dates | NEW chunk: {len(dates_new_chunk)} dates")
        print(f"Total COMBINED dates: {len(combined_dates)}")

        # 5) Escribimos salida
        if os.path.exists(file_out) and not overwrite:
            raise FileExistsError(f"{file_out} exists and overwrite=False")

        print("WRITING OUTPUT FILE")
        with h5py.File(file_out, "w") as f_out:
            # Copiar metadatos de OLD (excepto cum, bperp, imdates)
            for key in f_old.keys():
                if key not in ["cum", "bperp", "imdates"]:
                    d = f_old[key]
                    if d.shape == ():  # escalar
                        f_out.create_dataset(key, data=d[()])
                    else:
                        f_out.create_dataset(key, data=d[:])

            # Fechas combinadas
            f_out.create_dataset("imdates", data=combined_dates.astype(np.int32), dtype="int32")

            # Dataset de salida
            Y, X = cum_old.shape[1:]
            T2 = len(combined_dates)
            dset_out = f_out.create_dataset("cum", shape=(T2, Y, X), dtype="float32")

            # 5a) OLD hasta la uniÃ³n
            if idx_old_keep_end >= 0:
                dset_out[:idx_old_keep_end+1] = cum_old[:idx_old_keep_end+1]

            # 5b) NEW alineado desde la uniÃ³n
            dset_out[idx_old_keep_end+1:] = aligned_new.astype(np.float32)

            # (Opcional) guardamos tambiÃ©n bperp si existe en NEW y/o OLD (no siempre tiene sentido)
            if "bperp" in f_new.keys():
                # bperp para NEW chunk; para OLD chunk ponemos NaN
                bperp_new = f_new["bperp"][:]  # (T_new,)
                bperp_old = np.full_like(dates_old_chunk, np.nan, dtype=np.float32)
                # bperp_new desde idx_new_start
                bperp_comb = np.concatenate([bperp_old, bperp_new[idx_new_start:]]).astype(np.float32)
                f_out.create_dataset("bperp", data=bperp_comb)

    print("Merged (boundary-align) saved to:")
    print(" ", file_out)
    print("DONE")

    # 6) QC opcional: grÃ¡fica tipo la de tu figura en un pÃ­xel
    if qc_pixel is not None:
        try:
            import matplotlib.pyplot as plt

            y, x = qc_pixel
            # Series 1D en el pÃ­xel (OLD, NEW, MERGED)
            old_t = np.arange(cum_old.shape[0])
            new_t = np.arange(cum_new.shape[0])
            merged_t = np.arange(len(combined_dates))

            old_series = cum_old[:, y, x]
            new_series = cum_new[:, y, x]
            merged_series = np.concatenate([
                old_series[:idx_old_keep_end+1] if idx_old_keep_end >= 0 else np.array([]),
                aligned_new[:, y, x]
            ])

            # Mapear a tiempo real (fechas int)
            fig, axs = plt.subplots(1, 2, figsize=(10, 4), dpi=160)
            axs[0].plot(dates_old, old_series, 'o-', label='Old series')
            axs[0].plot(dates_new, new_series, 'o-', label='New series')
            axs[0].axvline(d0_new, color='r', ls='--', label='Time merged')
            axs[0].set_title('Before merge')
            axs[0].set_xlabel('Time (imdates)')
            axs[0].set_ylabel('Displacement (mm)')
            axs[0].legend()

            axs[1].plot(combined_dates, merged_series, 'o-', label='Merged series')
            axs[1].axvline(d0_new, color='r', ls='--', label='Time merged')
            axs[1].set_title('After merge')
            axs[1].set_xlabel('Time (imdates)')
            axs[1].set_ylabel('Displacement (mm)')
            axs[1].legend()

            out_png = os.path.join(os.path.dirname(file_out), "merge_qc_pixel_%d_%d.png" % (y, x))
            plt.tight_layout()
            plt.savefig(out_png)
            plt.close()
            print(f"QC figure saved: {out_png}")
        except Exception as e:
            print("QC plot failed:", e)
            
# MERGE FUNCTION WITH STEP LOGIC
def merge_cum_files_stepaware(file_old, file_new, file_out, min_step=2.0):

    print("LOADING FILES")
    with h5py.File(file_old, 'r') as f_old, h5py.File(file_new, 'r') as f_new:

        dates_old = f_old["imdates"][:]
        dates_new = f_new["imdates"][:]

        cum_old = f_old["cum"][:]     # (T_old, Y, X)
        cum_new = f_new["cum"][:]     # (T_new, Y, X)

        print(f"Old dates: {len(dates_old)}")
        print(f"New dates: {len(dates_new)}")

        # Detect steps (exact logic as your synthetic script)
        print("DETECTING STEPS")
        step_old = detect_step_cube(cum_old, min_step)
        step_new = detect_step_cube(cum_new, min_step)

        print("Detected step in OLD  =", step_old)
        print("Detected step in NEW  =", step_new)

        step_both = (step_old is not None and step_new is not None)

        # same rules as your synthetic code
        if step_both:
            union_idx = step_old
        elif step_old is not None:
            union_idx = step_old - 1
        else:
            union_idx = len(dates_old) - 1

        print("Union index selected =", union_idx)

        # Extract NEW dates that do not exist in OLD
        print("MERGING DATES")
        dates_new_valid = dates_new[~np.isin(dates_new, dates_old)]
        idx_new_valid = np.where(np.isin(dates_new, dates_new_valid))[0]

        combined_dates = np.sort(np.concatenate([dates_old, dates_new_valid]))

        print(f"Valid NEW dates: {len(dates_new_valid)}")
        print(f"Total COMBINED dates: {len(combined_dates)}")

        # Prepare output HDF5

        print("WRITING OUTPUT FILE")
        with h5py.File(file_out, "w") as f_out:

            # copy metadata (everything except cum, bperp, imdates)
            for key in f_old.keys():
                if key not in ["cum", "bperp", "imdates"]:
                    data = f_old[key]

                    if data.shape == ():      # scalar dataset
                        f_out.create_dataset(key, data=data[()])
                    else:                     # normal array
                        f_out.create_dataset(key, data=data[:])

            # combined dates
            f_out.create_dataset("imdates", data=combined_dates, dtype="int32")

            # create output dataset
            T2 = len(combined_dates)
            Y, X = cum_old.shape[1:]
            dset_out = f_out.create_dataset("cum", shape=(T2, Y, X), dtype="float32")

            # Insert old data up to UNION index
            dset_out[:union_idx+1] = cum_old[:union_idx+1]

            # Compute increments in NEW series
            vals_new_valid = cum_new[idx_new_valid]

            base_val = cum_old[union_idx].astype(np.float64)

            # shift new series so first value matches base
            offset = vals_new_valid[0]

            vals_new_shifted = vals_new_valid - offset

            accumulated = base_val + vals_new_shifted

            idx_combined_new = np.searchsorted(combined_dates, dates_new_valid)

            for i, idxc in enumerate(idx_combined_new):
                dset_out[idxc] = accumulated[i]

        print("Step-aware merged datacube saved to:")
        print("  ", file_out)
        print("DONE")



# MAIN PROGRAM
def main():
    parser = argparse.ArgumentParser(
        description="Merge LiCSBAS cum.h5 time-series using step detection."
    )

    # Option 1 (old automatic mode)
    parser.add_argument("-t", "--tsadir",
                        help="Time-series analysis directory (auto mode)")

    # Option 2 (manual folders)
    parser.add_argument("--folder1",
                        help="First TS folder (old series)")
    parser.add_argument("--folder2",
                        help="Second TS folder (new series)")
    parser.add_argument("--out",
                        help="Output TS folder")

    parser.add_argument("--min-step", type=float, default=2.0,
                        help="Minimum step size to detect (default: 2.0 mm)")

    args = parser.parse_args()
    min_step = args.min_step



    # MODE 1: manual folders

    if args.folder1 and args.folder2 and args.out:

        oldfile = os.path.join(args.folder1, "cum_filt.h5")
        newfile = os.path.join(args.folder2, "cum_filt.h5")
        outfile = os.path.join(args.out, "cum_filt.h5")

        os.makedirs(args.out, exist_ok=True)

        print("MANUAL MERGE MODE")
        print("Old file:   ", oldfile)
        print("New file:   ", newfile)
        print("Output file:", outfile)

        #merge_cum_files_stepaware(oldfile, newfile, outfile, min_step)
#        merge_cum_files_boundary_align(oldfile, newfile, outfile, qc_pixel=(98,126))
        merge_cum_files_auto_step(oldfile, newfile, outfile, qc_pixel=(98,126))
        
        
        oldfile = os.path.join(args.folder1, "cum.h5")
        newfile = os.path.join(args.folder2, "cum.h5")
        outfile = os.path.join(args.out, "cum.h5")
        #merge_cum_files_boundary_align(oldfile, newfile, outfile, qc_pixel=(98,126))
        merge_cum_files_auto_step(oldfile, newfile, outfile, qc_pixel=(98,126))
        
        copy_results(args.folder1, args.out)
        create_network_from_two_ts(args.folder1, args.folder2, args.out)

        return



    # MODE 2: automatic mode
 
    if args.tsadir:

        tsadir = args.tsadir

        if not os.path.isdir(tsadir):
            print("ERROR: TS directory not found:", tsadir)
            sys.exit(1)

        print("AUTO MERGE MODE")

        ifgdir = tsadir.replace("TS_", "")
        ifgdir = monitoring_lib.update_ifgdir12_16(ifgdir)
        ifgdir = ifgdir.replace("_update", "")

        tsadir = f"TS_{ifgdir}"
        tsadir_update = f"{tsadir}_update"

        oldfile = os.path.join(tsadir, "cum_filt.h5")
        newfile = os.path.join(tsadir_update, "cum_filt.h5")
        outfile = os.path.join(tsadir, "cum_update.h5")

        print("Old file:   ", oldfile)
        print("New file:   ", newfile)
        print("Output file:", outfile)

        #merge_cum_files_stepaware(oldfile, newfile, outfile, min_step)
        merge_cum_files_boundary_align(oldfile, newfile, outfile, qc_pixel=(98,126))
        copy_results(args.folder1, args.out)
        return


    print("ERROR: You must provide either --tsadir OR --folder1 --folder2 --out")
    sys.exit(1)



if __name__ == "__main__":
    main()

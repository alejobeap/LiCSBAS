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


def choose_union_index(dates_old, ds_old, dates_new, ds_new, method="last", user_idx=None, rms_threshold=None):
    """
    Decide dónde unir la nueva serie a la vieja.
    - method="last": última fecha de old
    - method="manual": índice elegido por el usuario (-1 última, -2 penúltima, etc.)
    - method="varianza": usa coherencia en el solapamiento, con detección de outliers en los bordes
    - rms_threshold: si None, se calcula automáticamente usando 2σ de incrementos
    """
    n_old = len(dates_old)

    if method == "manual" and user_idx is not None:
        return user_idx  # ejemplo: -1 última, -2 penúltima

    if method == "varianza":
        # buscar solapamiento de fechas
        overlap = np.intersect1d(dates_old, dates_new)
        if len(overlap) == 0:
            print("⚠️ No hay solapamiento, usando última fecha")
            return -1

        idx_old_overlap = np.where(np.isin(dates_old, overlap))[0]
        idx_new_overlap = np.where(np.isin(dates_new, overlap))[0]

        vals_old_overlap = ds_old[idx_old_overlap].astype(np.float64)
        vals_new_overlap = ds_new[idx_new_overlap].astype(np.float64)

        # calcular rms_threshold automáticamente si no se pasa
        if rms_threshold is None:
            incr_old = np.diff(vals_old_overlap, axis=0)
            incr_new = np.diff(vals_new_overlap, axis=0)
            rms_threshold = 2 * max(np.std(incr_old), np.std(incr_new))
            print(f"ℹ️ RMS threshold automático (2σ de incrementos) = {rms_threshold:.3f}")

        # RMS normal en todo el solapamiento
        rms_full = compute_rms(vals_old_overlap, vals_new_overlap)
        print(f"📊 RMS en todo el solapamiento = {rms_full:.3f}")

        if rms_full < rms_threshold:
            # buena coherencia → unión en la primera fecha de solapamiento
            best_idx = idx_old_overlap[0]
            print(f"✅ Unión en intersección (idx {best_idx}, fecha {dates_old[best_idx]})")
            return best_idx

        # probar quitando la última fecha del old
        if len(idx_old_overlap) > 1:
            rms_drop_last = compute_rms(vals_old_overlap[:-1], vals_new_overlap[:-1])
            print(f"📊 RMS descartando última fecha old = {rms_drop_last:.3f}")
            if rms_drop_last < rms_threshold:
                best_idx = idx_old_overlap[0]
                print(f"⚠️ Última fecha old parece outlier, unión en intersección (fecha {dates_old[best_idx]})")
                return best_idx

        # probar quitando penúltima y última
        if len(idx_old_overlap) > 2:
            rms_drop_two = compute_rms(vals_old_overlap[:-2], vals_new_overlap[:-2])
            print(f"📊 RMS descartando últimas 2 fechas old = {rms_drop_two:.3f}")
            if rms_drop_two < rms_threshold:
                best_idx = idx_old_overlap[0]
                print(f"⚠️ Últimas 2 fechas old parecen outliers, unión en intersección (fecha {dates_old[best_idx]})")
                return best_idx

        # si nada mejora → step real → unir en la primera fecha nueva
        print("❌ Solapamiento incoherente o step en ambas series, usando última fecha old")
        return -1

    # por defecto: última fecha
    return -1


def merge_cum_files_continuous_accumulate(file_old, file_new, file_out, method="last", user_idx=None):
    with h5py.File(file_old, 'r') as f_old, h5py.File(file_new, 'r') as f_new:
        dates_old = f_old['imdates'][:]
        dates_new = f_new['imdates'][:]
        dates_new_valid = dates_new[~np.isin(dates_new, dates_old)]
        combined_dates = np.sort(np.concatenate((dates_old, dates_new_valid)))

        print(f"Total unique valid dates: {len(combined_dates)}")

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
                union_idx = choose_union_index(dates_old, ds_old, dates_new, ds_new,
                                               method=method, user_idx=user_idx)

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

        print("✅ Combined data saved to:", file_out)


def main():
    parser = argparse.ArgumentParser(description="Combine LiCSBAS cum.h5 files for continuous accumulation")
    parser.add_argument("-t", "--tsadir", required=True, help="Time-series analysis directory (e.g., TS_GEOCml1clip)")
    parser.add_argument("--varianza", action="store_true", help="Elegir unión automáticamente usando coherencia en solapamiento")
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


'''
Objetivo del script

El script combina dos series de tiempo acumuladas (cum.h5) generadas por LiCSBAS:

Serie antigua: ya procesada y acumulada.

Serie nueva: datos recientes que se quieren unir a la antigua.

El resultado es un archivo combinado (cum_update.h5) donde la acumulación es continua y se evita duplicar fechas.

Flujo principal

Preparación de directorios y archivos

Toma un directorio de análisis de series de tiempo (TS_...).

Localiza los archivos antiguos (cum.h5) y nuevos (TS_..._update/cum.h5).

Crea el archivo de salida (cum_update.h5).

Elección del punto de unión

Si las series tienen fechas en común, se analiza el solapamiento para decidir dónde unir:

Manual: el usuario elige el índice de unión.

Última: se une al final de la serie vieja.

Varianza: se busca coherencia en el solapamiento y se detectan posibles “steps” (cambios bruscos):

Calcula el RMS (raíz del error cuadrático medio) entre los valores antiguos y nuevos en las fechas solapadas.

Compara el RMS con un umbral automático, calculado como 2σ de los incrementos de la serie (diferencias entre fechas consecutivas).

Esto permite distinguir cambios bruscos reales de la variabilidad normal de la serie.

Si el RMS es bajo → unión en la primera fecha de solapamiento.

Si el RMS es alto, prueba descartando 1 o 2 últimas fechas de la serie vieja para ver si son outliers.

Si no mejora → hay un step real en ambas series → unión en la última fecha de la serie vieja.

Acumulación continua

Calcula los incrementos de la serie nueva.

Ajusta la nueva serie para que empiece desde el valor correspondiente de la serie vieja en el punto de unión.

Genera una acumulación continua sin duplicar fechas.

Guardado

Copia datasets estáticos (no temporales) directamente.

Combina imdates, bperp y cum en el archivo de salida.

Detalles técnicos clave

RMS automático:

Se calcula como 2σ de los incrementos en la zona de solapamiento, es decir, dos veces la desviación estándar de los cambios entre fechas consecutivas.

Esto hace que el script se adapte a series con distinta variabilidad, sin depender de un valor fijo de RMS.

Incrementos y acumulación

La serie nueva se transforma en incrementos (Δcum) para luego sumarlos al último valor de la serie vieja.

Esto garantiza que la serie combinada sea continuamente acumulada.

Manejo de steps

El script detecta pasos bruscos (“step”) en la serie vieja o en ambas.

Según dónde esté el step, decide unir en la primera fecha de solapamiento o en la última fecha de la serie vieja.

Mensajes que muestra al usuario

RMS en solapamiento = ... → indica coherencia entre series.

Última fecha old parece outlier → detecta si la serie vieja tiene valores atípicos.

Solapamiento incoherente o step en ambas series → el script decide unir al final de la serie vieja.

Combined data saved to ... → confirma que el archivo combinado se generó correctamente.

Resumen visual del proceso
Serie antigua:  |-----|-----|-----|-----|
Serie nueva:        |-----|-----|-----|
                    ↑ Unión ajustada por RMS
Acumulación final: |-----|-----|-----|-----|-----|-----|-----|


Si hay un step solo en la serie vieja → el script ajusta la unión para evitar el error.

Si el step está en ambas series → la unión se hace al final de la serie vieja.

 En pocas palabras:
El script garantiza que las series de tiempo acumuladas se unan de forma suave y continua, detectando automáticamente cambios bruscos y adaptando la unión según la coherencia entre las series, usando un umbral RMS automático basado en la variabilidad de los incrementos.
'''

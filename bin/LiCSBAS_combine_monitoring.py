import sys
import numpy as np
import h5py

def merge_cum_files_continuous_accumulate(file_old, file_new, file_out):
    # Open old cum.h5 and update cum.h5 #Abrir archivos viejo y nuevo
    with h5py.File(file_old, 'r') as f_old, h5py.File(file_new, 'r') as f_new:
        # Leer fechas
        dates_old = f_old['imdates'][:]
        dates_new = f_new['imdates'][:]

        # Filter new dates to avoid duplicates with old dates  #Filtrar fechas nuevas para evitar duplicados con viejo
        dates_new_valid = dates_new[~np.isin(dates_new, dates_old)]

        # Create sorted combined array # Crear array combinado ordenado
        combined_dates = np.sort(np.concatenate((dates_old, dates_new_valid)))

        print(f"Total fechas unicas validas: {len(combined_dates)}")
        print("Fechas combinadas:\n", combined_dates)

        temporal_dsets = ['bperp', 'cum', 'imdates']

        # Open output file #Abrir archivo de salida
        with h5py.File(file_out, 'w') as f_out:
            # Copy non-temporary datasets without overwriting temporaries or imdates #Copiar datasets no temporales sin sobrescribir temporales ni imdates
            for key in f_old.keys():
                if key not in temporal_dsets:
                    ds = f_old[key]
#                    f_out.create_dataset(key, data=ds[:], dtype=ds.dtype)
                    if ds.shape == ():  # dataset escalar
                       f_out.create_dataset(key, data=ds[()], dtype=ds.dtype)
                    else:
                       f_out.create_dataset(key, data=ds[:], dtype=ds.dtype)

            # Save combined dates #Guardar fechas combinadas
            f_out.create_dataset('imdates', data=combined_dates, dtype='int32')

            # For each temporary dataset (except imdates which we have already saved) #Para cada dataset temporal (excepto imdates que ya guardamos)
            for key in ['bperp', 'cum']:
                ds_old = f_old[key]
                ds_new = f_new[key]

                # Detectar dimensiones espaciales (todo excepto tiempo)
                spatial_shape = ds_old.shape[1:]
                n_combined = len(combined_dates)

                # Create output dataset with correct form #Crear dataset salida con forma correcta
                dset_out = f_out.create_dataset(key, shape=(n_combined, *spatial_shape), dtype=ds_old.dtype)

                # Copy old data in the result into the old date range #Copiar datos viejos en el resultado en el rango de fechas viejas
                n_old = len(dates_old)
                dset_out[:n_old] = ds_old[:]

                # Acumular datos nuevos ajustando para continuidad
                # Para cada fecha nueva valida, calcular indice en combined_dates
                # Y sumar incremento respecto a ultimo valor viejo

                # Accumulate new data adjusting for continuity
                # For each new valid date, calculate index in combined_dates
                # And add increment to last old value

                # Indices en new file para las fechas validas
                idx_new_valid = np.where(np.isin(dates_new, dates_new_valid))[0]

                # Indices en combined_dates para fechas nuevas validas
                idx_combined_new = np.searchsorted(combined_dates, dates_new_valid)

                # Last old accumulated for offset #Ultimo acumulado viejo para offset
                last_old_value = ds_old[-1]

                # Calculamos incrementos relativos en ds_new para fechas validas
                # Para acumulados: queremos que el primer nuevo valor sume a last_old_value,
                # y luego siga acumulando.

                # We calculate relative increments in ds_new for valid dates.
                # For accumulated: we want the first new value to add to last_old_value,
                # and then continue to accumulate.

                # Extract new valid values  #Extraer valores nuevos validos
                vals_new_valid = ds_new[idx_new_valid]

                # Convert to float to avoid overflow (optional) #Convertir a float para evitar overflow (opcional)
                vals_new_valid = vals_new_valid.astype(np.float64)
                last_old_value = last_old_value.astype(np.float64)

                # Estimate valid ds_new increments (difference between consecutive) #Calcular incrementos de ds_new validos (diferencia entre consecutivos)
                increments = np.zeros_like(vals_new_valid)
                increments[0] = vals_new_valid[0]  # primer valor es incremento bruto
                if len(vals_new_valid) > 1:
                    increments[1:] = vals_new_valid[1:] - vals_new_valid[:-1]

                # Ajustar el primer incremento para que coincida con la diferencia respecto a ultimo viejo
                # Buscamos el valor de ds_new correspondiente a la fecha anterior a la primera nueva valida, si existe
                # Si no, asumimos incremento directo.


                # Adjust the first increment to match the difference with respect to the last old one.
                # Look for the value of ds_new corresponding to the date before the first new valid date, if it exists.
                # If not, assume direct increment.
                # Ultimo valor acumulado del viejo para ese punto espacial

                offset = last_old_value

                # Calculamos acumulados nuevos continuos
                accumulated = np.cumsum(increments, axis=0) + offset

                # Guardar acumulados nuevos en el dataset de salida en sus posiciones
                for i, idx_c in enumerate(idx_combined_new):
                    dset_out[idx_c] = accumulated[i]

            print("Datos combinados y guardados en:", file_out)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso: python combinarh5.py archivo_viejo.h5 archivo_nuevo.h5 archivo_salida.h5")
        print("command: python LiCSBAS_combine_monitoring.py old_file.h5 new_file.h5 output_file.h5")
        sys.exit(1)
    
    merge_cum_files_continuous_accumulate(sys.argv[1], sys.argv[2], sys.argv[3])

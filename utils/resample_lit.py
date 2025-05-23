import pandas as pd
import numpy as np

def resample_data(df , twt_interval=0.5):
    """
    Melakukan resampling data berdasarkan interval kolom TWT
    """

    df['TWT'] = pd.to_numeric(df['TWT'],errors='coerce')
    df['MD'] = pd.to_numeric(df['MD'],errors='coerce')
    df['Gamma'] = pd.to_numeric(df['Gamma'] , errors='coerce')
    df['General discrete'] = pd.to_numeric(df['General discrete'], errors='coerce')
    df = df.dropna(subset=['TWT'])

    min_twt = np.floor(df['TWT'].min()/ twt_interval) * twt_interval
    max_twt = np.floor(df['TWT'].max()/ twt_interval) * twt_interval

    new_twt_points = np.arange(min_twt, max_twt, twt_interval)

    resampled_data = []

    for start_twt in new_twt_points:
        end_twt = start_twt + twt_interval
        mask = (df['TWT'] >= start_twt) & (df['TWT'] < end_twt)
        data_in_interval  = df[mask]

        if not data_in_interval.empty:
            avg_md = data_in_interval['MD'].mean()
            avg_gamma = data_in_interval['Gamma'].mean()
            avg_general_discrete = data_in_interval['General dicsrete'].mean()
            avg_general_rounded = int(np.ceil(avg_general_discrete))

            resampled_data.append({
                'TWT' : start_twt,
                'MD' : avg_md,
                'Gamma' : avg_gamma,
                'General_Discrete' : avg_general_rounded
            })

    return pd.DataFrame(resampled_data)
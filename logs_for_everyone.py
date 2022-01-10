import lasio
import pandas as pd
import numpy as np
from pathlib import Path
import glob
import os

def create_las_dataframe(las):
    """Concatenates curve data into a dataframe with the depth and well data"""
    las_output = []

    header_data = get_header_data(las)
    header_data['DEPTH'] = las.curves.DEPTH.data

    for curve in las.curves:
        if curve.mnemonic != 'DEPTH':
            curve_data = get_curve_data(curve)
            curve_data.update(header_data)
            df = pd.DataFrame(curve_data)
            las_output.append(df)

    return pd.concat(las_output)

def get_header_data(las):
    """Returns a dictionary of header information from LAS file"""
    header_data = {
        'UWI':las.well[-1].value,
        'WELL': las.well[5].value,
        'DEPTH_UOM': las.curves[0].unit,
        'START': las.well[0].value,
        'START_UOM': las.well[0].unit,
        'STOP': las.well[1].value,
        'STOP_UOM': las.well[1].unit,
        'STEP': las.well[2].value,
        'STEP_UOM': las.well[2].unit 
    }
    return header_data

def get_curve_data(curve):
    """Returns a dictionary of the curve data"""
    curve_data = {
        'MNEMONIC': curve.mnemonic, 
        'MNEMONIC_UOM': curve.unit, 
        'MNEMONIC_VALUE': curve.data,
        'MNEMONIC_WELLOG_COMMENT': np.nan
    }
    return curve_data

LAS_DIRECTORY = Path(r'C:/Users/aschnel/Juypiter_Notebooks/')
OUTPUT_DIRECTORY = Path(r'C:/Users/aschnel/Juypiter_Notebooks/AWS_Logs_Txt/')

# Define the directory path and create a list of the files in the directory as a variable
las_files = list(LAS_DIRECTORY.glob('*.las'))

for file in las_files:
    print('Processing:\t', file.name)
    output_file = file.name.split('.')[0] + '.txt'
    output_path = os.path.join(OUTPUT_DIRECTORY, output_file)

    las = lasio.read(file)
    las_output = create_las_dataframe(las)
    las_output.to_csv(output_path, sep='|', index=False)
    
    print('Saved:\t\t', las_output.shape)
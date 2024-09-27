import polars as pl
import pathlib

import data_processing
import core

def main(args):
    print('main')

    harvest_dtypes = [
        pl.Int32,
        pl.Int32,
        pl.Float64,
        pl.Float64,
        pl.Utf8,
        pl.Utf8,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Float64,
        pl.Int32,
        pl.Utf8
    ]

    harvest = pl.read_csv(
        args['path_harvest_data'],
        schema_overrides=harvest_dtypes)
    
    harvest_p1a0 = harvest.rename(
        {c: c+'_P1' for c in harvest.columns if c not in args['dimension_vars']})

    harvest_p1a1 = data_processing.generate_p1a1(harvest_p1a0, args)
    #core.write_data_files(harvest_p1a1, 1, 1, args)
    
    harvest_p2a0 = data_processing.generate_p2a0(harvest_p1a1, args)
    harvest_p2a1 = data_processing.generate_p2a1(harvest_p2a0, args)
    harvest_p2a2 = data_processing.generate_p2a2(harvest_p2a1, args)
    harvest_p2a3 = data_processing.generate_p2a3(harvest_p2a2, args)

    harvest_p3a0 = data_processing.generate_p3a0(harvest_p2a3, args)
    harvest_p3a1 = data_processing.generate_p3a1(harvest_p3a0, args)

    core.write_data_files(harvest_p3a1, 3, 1, args)

    print('End')

if __name__ == '__main__':
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'

    path_output.mkdir(parents=True, exist_ok=True)
    
    args = {}
    args['path_output'] = path_output
    args['path_harvest_data'] = path_input / 'HY1999-2016_20230922_P1A0.csv'
    args['path_qa_file_p1a0'] = path_input / 'qaFlagFile_p1a0_All.csv'
    args['path_qa_file_p2a0'] = path_input / 'qaFlagFile_p2a0_All.csv'
    args['path_qc_bounds_p2a1'] = path_input / 'qcBounds_p2.csv'
    args['path_qc_obs_hi_p2a2'] = path_input / 'qcHarvestIndex_p2.csv'
    args['path_qa_file_p3a0'] = path_input / 'qaFlagFile_p3a0_All.csv'

    args['dimension_vars'] = ['HarvestYear', 'ID2', 'Longitude', 'Latitude', 'SampleID', 'Crop', 'Comments']
    args['key_vars'] = ['HarvestYear', 'ID2']
    args['qc_suffixes'] = ['_qcApplied', '_qcResult', '_qcPhrase']
    args['p_suffixes'] = ['_P1', '_P2', '_P3']
    args['file_base_name'] = 'HY1999-2016'

    main(args)
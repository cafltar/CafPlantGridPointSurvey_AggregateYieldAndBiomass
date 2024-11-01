import polars as pl
import pathlib
import datetime
import pandas as pd

#https://github.com/cafltar/cafcore/releases/tag/v0.1.3
import cafcore.qc
import cafcore.file_io

def organize_columns(base_cols):
    ordered_cols = []
    for col in base_cols:
        ordered_cols.append(col)
        if(('_P1' in col)):
            ordered_cols.append(col + '_qcApplied')
            ordered_cols.append(col + '_qcResult')
            ordered_cols.append(col + '_qcPhrase')
    
    return ordered_cols

def calculate_qc_summary_col(row, cols, qcSuffix):
    total_cols = len(cols)
    sum_cols_with_qc = 0
    
    for col in cols:
        col_qc = col + qcSuffix
        
        if col_qc not in row.keys():
            continue

        if int(row[col_qc]) > 0:
            sum_cols_with_qc += 1

    coverage = (sum_cols_with_qc / total_cols) * 100

    return round(coverage, 1)
        
def get_metric_cols(all_cols, dim_cols):
    # strip out qc stuff
    qc_suffixes = ['_qcApplied', '_qcResult', '_qcPhrase']
    non_qc_cols = [col for col in all_cols if not any(qc_suffix in col for qc_suffix in qc_suffixes)]
    result = list(set(non_qc_cols) - set(dim_cols))

    return result
        
def append_qc_summary_cols(df:pl.DataFrame, dimension_vars):
    print ('write simplified qc files')
    # Takes dataframe with full detailed columns like _P1, _P2, _qcApplied, etc. and writes a simplified data file and a separate qc file

    # Write summary columns for qc applied and qc results
    metric_cols = get_metric_cols(df.columns, dimension_vars)

    qc_schema = {
        'HarvestYear': pl.Int32, 
        'ID2': pl.Int32, 
        'QCCoverage': pl.Float32, 
        'QCFlags': pl.Float32
    }
    
    qc_df = pl.DataFrame(schema=qc_schema)

    for row in df.iter_rows(named=True):
        qc_coverage = calculate_qc_summary_col(row, metric_cols, '_qcApplied')
        qc_flags = calculate_qc_summary_col(row, metric_cols, '_qcResult')
        harvest_year = row['HarvestYear']
        sample_ID = row['ID2']

        row_df = pl.DataFrame({
            'HarvestYear': harvest_year,
            'ID2': sample_ID,
            'QCCoverage': qc_coverage,
            'QCFlags': qc_flags},
            schema = qc_schema)
        
        qc_df.extend(row_df)

    df = df.join(qc_df, on = ['HarvestYear', 'ID2'], how='left')

    return df

def prune_columns_outside_p_level(df, processing_level, p_suffixes, qc_suffixes):
    df_result = df.clone()

    p_suffixes_drop = p_suffixes[processing_level:]

    # Drop columns ending in _P# if # is higher than processing level
    p_cols_drop = [col for col in df_result.columns if any(col.endswith(p_suffix) for p_suffix in p_suffixes_drop)]
    df_result = df_result.drop(p_cols_drop)

    for qc_suffix in qc_suffixes:
        for p_suffix in p_suffixes_drop:
            suffix = qc_suffix + p_suffix
            drop_cols = [col for col in df_result.columns if col.endswith(suffix)]
            df_result = df_result.drop(drop_cols)

    return df_result

def drop_columns_include_qc(df, columns):
    df_result = df.clone()

    cols_drop = [col for col in df_result.columns if any(column in col for column in columns)]

    df_result = df_result.drop(cols_drop)

    return df_result

# Basically copied cafcore.qc.process_qc_bounds_check but updating 3rd bit in qc bitstring
# Also need to add (hacky) logic to update qc columns for column(s) that are different than the one that is being checked with bounds
def process_qc_observation_bounds_check(
        df: pd.DataFrame, 
        affectedColNames: list,
        idColName: str, 
        lower: int, 
        upper: int,
        flagNulls: bool = False) -> pd.DataFrame:

    df_out = df.copy()

    for affectedColName in affectedColNames:

        qc_applied_colName = affectedColName + "_qcApplied"
        qc_result_colName = affectedColName + "_qcResult"
        qc_reason_colName = affectedColName + "_qcPhrase"

        # TODO: Need to handle adding previous QC checks - use bin() / bit manip
        # 000100 means level 3 check, or check using information from single observation
    
        if qc_applied_colName not in df.columns:
            df_out[qc_applied_colName] = "000000"
    
        df_out[qc_applied_colName] = df_out[qc_applied_colName].apply(lambda x: cafcore.qc.update_qc_bitstring(x, "000100"))

        if qc_result_colName not in df.columns:
            df_out[qc_result_colName] = "000000"

        # result code defaults to 0 (passed), set to 000100 (fail) if outside of bounds
        # TODO: Definitely refactor this ugly code
        if(flagNulls):
            df_out.update(df_out.loc[(pd.isna(df_out[idColName]) | (df_out[idColName] < lower) | (df_out[idColName] > upper)), qc_result_colName].apply(lambda x: cafcore.qc.update_qc_bitstring(x, "000100")))
        
            changePhrase = f"(Observation) Calculated value {idColName} is outside of bounds [{lower}, {upper}]"
            df_out.update(df_out.loc[(pd.isna(df_out[idColName]) | (df_out[idColName] < lower) | (df_out[idColName] > upper)), qc_reason_colName].apply(lambda x: cafcore.qc.update_phrase(x, changePhrase)))
        else:
            df_out.update(df_out.loc[((df_out[idColName] < lower) | (df_out[idColName] > upper)), qc_result_colName].apply(lambda x: cafcore.qc.update_qc_bitstring(x, "000100")))
        
            changePhrase = f"(Observation) Calculated value {idColName} is outside of bounds [{lower}, {upper}]"
            df_out.update(df_out.loc[((df_out[idColName] < lower) | (df_out[idColName] > upper)), qc_reason_colName].apply(lambda x: cafcore.qc.update_phrase(x, changePhrase)))

    return df_out

def write_csv_files(df, key, file_name, processing_level, accuracy_level, output_path, p_suffixes, qc_suffixes):
    date_today = datetime.datetime.now().strftime("%Y%m%d")
    pa_suffix = f'P{processing_level}A{accuracy_level}'

    #qc_suffixes = ['_qcApplied', '_qcResult', '_qcPhrase']
    #p_suffixes = ['_P1', '_P2', '_P3'] #dropping all, so no need to worry about specified processing level
    
    qc_cols = [col for col in df.columns if any(col.endswith(qc_suffix) for qc_suffix in qc_suffixes)]
    p_cols = [col for col in df.columns if any(col.endswith(p_suffix) for p_suffix in p_suffixes)]

    # Write all columns
    comprehensive_file_name = f'{file_name}_{pa_suffix}_Comprehensive_{str(date_today)}.csv'
    df.write_csv(output_path / comprehensive_file_name)

    # Write QC file
    df_qc = df.select(key + qc_cols)
    qc_file_name = f'{file_name}_{pa_suffix}_QC_{str(date_today)}.csv'
    df_qc.write_csv(output_path / qc_file_name)

    # Write clean dataset
    clean_file_name = f'{file_name}_{pa_suffix}_{str(date_today)}.csv'
    df_clean = (df
        .drop(qc_cols)
        .drop(p_cols)
    )

    df_clean.write_csv(output_path / clean_file_name)

    return df_qc, df_clean

def condense_processing_columns(df, processing_level, p_suffixes):
    df_result = df.clone()

    p_suffixes_keep = p_suffixes[0:processing_level]
    
    # Get base column names for cols at processing_level -- does not assume, e.g. a P3 col has a corresponding P1 col
    p_cols = [col for col in df_result.columns if any(col.endswith(p_suffix) for p_suffix in p_suffixes_keep)]
    p_cols_basenames = remove_substrings_from_list(p_cols, p_suffixes_keep)

    # Fill values 
    for col_base in p_cols_basenames:
        # Get a list of processing cols for this col_base (e.g. _P3, _P2, _P1)
        p = processing_level
        cols = []
        while p > 0:
            col_name = col_base + '_P' + str(p)
            if(col_name in df_result.columns):
                cols.append(col_name)
            p = p - 1
        
        # Set condensed col to values in highest processing level then remove from list
        df_result = (df_result
                     .with_columns(pl.col(cols[0]).alias(col_base))
        )
        cols.pop(0)

        # Now fill in blanks with values from columns of lower processing
        for col in cols:
            #df_result[col_base] = df_result[col_base].fillna(df_result[col])
            df_result = (df_result
                         .with_columns(pl.col(col_base).fill_null(pl.col(col)))
                         #.drop(col)
            )

    return df_result

def remove_substrings_from_list(strings_list, substrings_to_remove):
    """
    Removes specified substrings from each string in the input list.

    Args:
        strings_list (list): List of strings.
        substrings_to_remove (list): List of substrings to remove.

    Returns:
        list: New list of strings with specified substrings removed.
    """
    cleaned_strings = []
    for s in strings_list:
        for substring in substrings_to_remove:
            s = s.replace(substring, '')
        cleaned_strings.append(s)
    return cleaned_strings

def write_data_files(df, processing_level, accuracy_level, args):
    df_trim = prune_columns_outside_p_level(df, processing_level, args['p_suffixes'], args['qc_suffixes'])
    
    df_trim_qc = append_qc_summary_cols(df_trim, args['dimension_vars'])
    
    df_trim_qc_p = condense_processing_columns(df_trim_qc, processing_level, args['p_suffixes'])
    
    df_qc, df_clean = write_csv_files(
        df_trim_qc_p, 
        args['key_vars'], 
        args['file_base_name'], 
        processing_level, 
        accuracy_level, 
        args['path_output'], 
        args['p_suffixes'], 
        args['qc_suffixes'])
    
    return df_qc, df_clean
import polars as pl
import pandas as pd

#https://github.com/cafltar/cafcore/releases/tag/v0.1.3
import cafcore.qc
import cafcore.file_io

import core

def generate_p1a1(df, args):
    print('generate_p1a1')
    
    # Assign SampleIDs to ones without SampleIDs (mostly prior to 2010)
    df = (df
          .with_columns(
            pl.when(pl.col('HarvestYear') < 2010)
            .then(pl.concat_str([pl.col('HarvestYear'),pl.col('ID2')], separator='_'))
            .otherwise(pl.col('SampleID'))
            .alias('SampleID')
          )
    )
    
    # Load the QA file
    qa = pd.read_csv(args['path_qa_file_p1a0'])

    qaUpdate = qa[qa['Verb'] == 'Update']
    qaFlag = qa[qa['Verb'] == 'Flag']

    df_pd = df.to_pandas()
    df_qa = cafcore.qc.initialize_qc(df_pd, args['dimension_vars'])

    df_qa = cafcore.qc.set_quality_assurance_applied(df_qa, args['dimension_vars'], True, True)

    # Update values (copied from cafcore/qc.py)
    for index, row in qaUpdate.iterrows():
        prevValueSeries = df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"] + '_P1')]
        
        if len(prevValueSeries) == 0:
            raise Exception("ID not found, check QA File")
        if len(prevValueSeries) > 1:
            raise Exception("Multiple values found for given ID, check input dataframe")
        
        prevValue = prevValueSeries.values[0]
        changePhraseCol = row["Variable"] + "_P1_qcPhrase"

        if(pd.isna(row["NewVal"])):
            df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"] + '_P1')] = None
        else:
            df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"] + '_P1')] = row["NewVal"]

        changePhrase = "(Assurance) Previous val: {}, reason: {}".format(prevValue, row["Comment"])
        
        # Create column if not exist
        if changePhraseCol not in df_qa.columns:
            df_qa[changePhraseCol] = None

        series = df_qa.loc[(df_qa['SampleID'] == row["ID"]), changePhraseCol]

        df_qa.loc[(df_qa['SampleID'] == row["ID"]), changePhraseCol] = cafcore.qc.update_phrase(
            df_qa.loc[series.index[0], changePhraseCol],
            changePhrase
        )

    # Flag values by setting QA bit to fail
    for index, row in qaFlag.iterrows():
        reasonPhraseCol = row['Variable'] + '_P1_qcPhrase'
        changePhrase = "(Assurance) {}".format(row['Comment'])

        qaResultCol = row['Variable'] + '_P1_qcResult'

        phrase_series = df_qa.loc[(df_qa['SampleID'] == row['ID']), reasonPhraseCol]

        df_qa.loc[(df_qa['SampleID'] == row['ID']), reasonPhraseCol] = cafcore.qc.update_phrase(
            df_qa.loc[phrase_series.index[0], reasonPhraseCol],
            changePhrase
        )

        df_qa.loc[(df_qa['SampleID'] == row['ID']), qaResultCol] = cafcore.qc.update_qc_bitstring(
            '000001', '000000')
        
    return pl.from_pandas(df_qa)

def generate_p2a0(df, args):
    print("generate_p2a0")
    
    harvest_1999_2009 = df.filter((pl.col('HarvestYear') >= 1999) & (pl.col('HarvestYear') <= 2009))
    harvest_2010 = df.filter((pl.col('HarvestYear') == 2010))
    harvest_2011_2012 = df.filter((pl.col('HarvestYear') >= 2011) & (pl.col('HarvestYear') <= 2012))
    harvest_2013_2016 = df.filter((pl.col('HarvestYear') >= 2013) & (pl.col('HarvestYear') <= 2016))

    # Check that we didn't leave any rows behind (or duplicated)
    split_row_sum = (
        harvest_1999_2009.shape[0] + 
        harvest_2010.shape[0] + 
        harvest_2011_2012.shape[0] + 
        harvest_2013_2016.shape[0])
    if(df.shape[0] != split_row_sum):
        print("Warning! There was a loss or gain of rows")

    # Further split 1999-2010 calculations by way the sample was collected
    harvest_1999_2009_grain = harvest_1999_2009.filter(
        (pl.col('GrainSampleArea_P1') > 0) & 
        ((pl.col('BiomassSampleArea_P1') == 0) | pl.col('BiomassSampleArea_P1').is_null())
    )
    harvest_1999_2009_biomass = harvest_1999_2009.filter(
        ((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
        (pl.col('BiomassSampleArea_P1') > 0 )
    )
    harvest_1999_2009_both = harvest_1999_2009.filter(
        (pl.col('GrainSampleArea_P1') > 0) & 
        (pl.col('BiomassSampleArea_P1') > 0 )
    )
    harvest_1999_2009_neither = harvest_1999_2009.filter(
        (pl.col('GrainSampleArea_P1').is_null() | (pl.col('GrainSampleArea_P1') == 0)) &
        (pl.col('BiomassSampleArea_P1').is_null() | (pl.col('BiomassSampleArea_P1') == 0))
    )
    
    # Check sums
    harvest_1999_2009_split_check = (
        harvest_1999_2009_grain.shape[0] + 
        harvest_1999_2009_biomass.shape[0] + 
        harvest_1999_2009_both.shape[0] + 
        harvest_1999_2009_neither.shape[0]
    )
    if(harvest_1999_2009_split_check != harvest_1999_2009.shape[0]):
        print('WARNING: 1999-2010 dataframe splits dont add up')

    harvest_1999_2009_grain_calc = (harvest_1999_2009_grain
        .with_columns(pl.col('GrainMassWetInGrainSample_P1').alias('GrainMassWet_P1'))
        .with_columns(pl.col('GrainMassAirDryInGrainSample_P1').alias('GrainMassAirDry_P1'))
        .with_columns(
            pl.when(pl.col('GrainSampleArea_P1') > 0)
            .then(pl.col('GrainMassWet_P1') / pl.col('GrainSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldWet_P2'))
        .with_columns(
            pl.when(pl.col('GrainMassWet_P1') > 0)
            .then((pl.col('GrainMassWet_P1') - pl.col('GrainMassAirDry_P1')) / pl.col('GrainMassWet_P1'))
            .otherwise(None)
            .alias('GrainMoistureProportionPartial_P2'))
        .with_columns(
            pl.when(pl.col('GrainSampleArea_P1') > 0)
            .then(pl.col('GrainMassAirDry_P1') / pl.col('GrainSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldAirDry_P2'))
    )

    harvest_1999_2009_biomass_calc = (harvest_1999_2009_biomass
        # === Grain calculations ===
        .with_columns(pl.col('GrainMassWetInBiomassSample_P1')
            .alias('GrainMassWet_P1'))
        .with_columns(pl.col('GrainMassAirDryInBiomassSample_P1')
            .alias('GrainMassAirDry_P1'))
        .with_columns(
            pl.when(pl.col('GrainMassWet_P1') > 0)
            .then((pl.col('GrainMassWet_P1') - pl.col('GrainMassAirDry_P1')) / pl.col('GrainMassWet_P1'))
            .otherwise(None)
            .alias('GrainMoistureProportionPartial_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('GrainMassWet_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldWet_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('GrainMassAirDry_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldAirDry_P2'))
        
        # === Residue calculations ===
        # Residue mass wet via biomass - grain mass 
        .with_columns(
            pl.when(pl.col('BiomassWet_P1') > 0)
            .then(pl.col('BiomassWet_P1') - pl.col('GrainMassWetInBiomassSample_P1'))
            .otherwise(None)
            .alias('ResidueMassWet_P2')
        )
        # Residue subsample oisture proportions via: (wet - dry) / wet
        .with_columns(
            pl.when(pl.col('ResidueMassWetSubsample_P1') > 0)
            .then((pl.col('ResidueMassWetSubsample_P1') - pl.col('ResidueMassAirDrySubsample_P1')) / pl.col('ResidueMassWetSubsample_P1'))
            .otherwise(None)
            .alias('ResidueMoistureProportionPartialSubsample_P2')
        )
        # Residue mass dry via (residue wet) - (residue wet * moisture proportion from sub sample)
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionPartialSubsample_P2')))
            .alias('ResidueMassAirDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassWet_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassAirDry_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassAirDryPerArea_P2'))
    )

    # 
    harvest_1999_2009_both_calc = (harvest_1999_2009_both
        # === Grain ===
        # Grain in grain sample
        .with_columns(
            (pl.col('GrainMassWet_P1') - pl.col('GrainMassWetInBiomassSample_P1'))
            .alias('GrainMassWetInGrainSample_P2')
        )
        # Moisture in grain
        .with_columns(
            pl.when(pl.col('GrainMassWet_P1') > 0)
            .then((pl.col('GrainMassWet_P1') - pl.col('GrainMassAirDry_P1')) / pl.col('GrainMassWet_P1'))
            .otherwise(None)
            .alias('GrainMoistureProportionPartial_P2'))
        # Use moisture proportion in grain to calculate dry mass of grain in the grain samples
        .with_columns(
            (pl.col('GrainMassWetInGrainSample_P2') - (pl.col('GrainMassWetInGrainSample_P2') * pl.col('GrainMoistureProportionPartial_P2')))
            .alias('GrainMassAirDryInGrainSample_P2')
        )

        # === Residue ===
        # Residue
        .with_columns(
            pl.when(pl.col('BiomassWet_P1') > 0)
            .then(pl.col('BiomassWet_P1') - pl.col('GrainMassWetInBiomassSample_P1'))
            .otherwise(None)
            .alias('ResidueMassWet_P2')
        )
        # Residue subsample moisture proportions via: (wet - dry) / wet
        .with_columns(
            pl.when(pl.col('ResidueMassWetSubsample_P1') > 0)
            .then((pl.col('ResidueMassWetSubsample_P1') - pl.col('ResidueMassAirDrySubsample_P1')) / pl.col('ResidueMassWetSubsample_P1'))
            .otherwise(None)
            .alias('ResidueMoistureProportionPartialSubsample_P2')
        )
        # Residue mass dry from residue moisture proportions
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionPartialSubsample_P2')))
            .alias('ResidueMassAirDry_P2')
        )

        # === Biomass ===
        # Use moisture proportion in grain to calculate dry mass of grain in the biomass samples
        .with_columns(
            (pl.col('GrainMassWetInBiomassSample_P1') - (pl.col('GrainMassWetInBiomassSample_P1') * pl.col('GrainMoistureProportionPartial_P2')))
            .alias('GrainMassAirDryInBiomassSample_P2')
        )

        # === Per area ===
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('GrainMassWet_P1') / (pl.col('GrainSampleArea_P1') + pl.col('BiomassSampleArea_P1')))
            .otherwise(None)
            .alias('GrainYieldWet_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('GrainMassAirDry_P1') / (pl.col('GrainSampleArea_P1') + pl.col('BiomassSampleArea_P1')))
            .otherwise(None)
            .alias('GrainYieldAirDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassWet_P2') / (pl.col('BiomassSampleArea_P1')))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassAirDry_P2') / (pl.col('BiomassSampleArea_P1')))
            .otherwise(None)
            .alias('ResidueMassAirDryPerArea_P2'))
    )

    harvest_1999_2009_calc = pl.concat([harvest_1999_2009_grain_calc, 
                                        harvest_1999_2009_biomass_calc, 
                                        harvest_1999_2009_both_calc,
                                        harvest_1999_2009_neither],
                                        rechunk=True,
                                        how = 'diagonal')

    harvest_2010_calc = (harvest_2010
        .with_columns(
            (pl.col('BiomassWet_P1') - pl.col('GrainMassWet_P1'))
            .alias('ResidueMassWet_P2')
        )
        .with_columns(
            pl.when(pl.col('GrainMassWet_P1') > 0)
            .then((pl.col('GrainMassWet_P1') - pl.col('GrainMassAirDry_P1')) / pl.col('GrainMassWet_P1'))
            .otherwise(None)
            .alias('GrainMoistureProportionPartial_P2')
        )
        # Residue subsample moisture proportions via: (wet - dry) / wet
        .with_columns(
            pl.when(pl.col('ResidueMassWetSubsample_P1') > 0)
            .then((pl.col('ResidueMassWetSubsample_P1') - pl.col('ResidueMassAirDrySubsample_P1')) / pl.col('ResidueMassWetSubsample_P1'))
            .otherwise(None)
            .alias('ResidueMoistureProportionPartialSubsample_P2')
        )
        # Residue mass dry from residue moisture proportions
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionPartialSubsample_P2')))
            .alias('ResidueMassAirDry_P2')
        )

        .with_columns(
            pl.when((pl.col('GrainSampleArea_P1') > 0) & 
                ((pl.col('BiomassSampleArea_P1') == 0) | pl.col('BiomassSampleArea_P1').is_null()))
            .then(pl.col('GrainMassWet_P1') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassWet_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldWet_P2')
        )
        .with_columns(
            pl.when((pl.col('GrainSampleArea_P1') > 0) & 
                ((pl.col('BiomassSampleArea_P1') == 0) | pl.col('BiomassSampleArea_P1').is_null()))
            .then(pl.col('GrainMassAirDry_P1') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassAirDry_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldAirDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassWet_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassAirDry_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassAirDryPerArea_P2')
        )
    )
    
    harvest_2011_2012_calc = (harvest_2011_2012
        .with_columns(
            (pl.col('BiomassWet_P1') - pl.col('GrainMassWet_P1'))
            .alias('ResidueMassWet_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassWet_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2')
        )
        .with_columns(
            pl.when((pl.col('GrainSampleArea_P1') > 0) & 
                ((pl.col('BiomassSampleArea_P1') == 0) | pl.col('BiomassSampleArea_P1').is_null()))
            .then(pl.col('GrainMassWet_P1') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassWet_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldWet_P2')
        )
    )
    
    harvest_2013_2016_calc = (harvest_2013_2016
        .with_columns(
            (pl.col('BiomassAirDry_P1') - pl.col('GrainMassAirDry_P1'))
            .alias('ResidueMassAirDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassAirDry_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassAirDryPerArea_P2')
        )
        .with_columns(
            pl.when((pl.col('GrainSampleArea_P1') > 0) & 
                ((pl.col('BiomassSampleArea_P1') == 0) | pl.col('BiomassSampleArea_P1').is_null()))
            .then(pl.col('GrainMassAirDry_P1') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassAirDry_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldAirDry_P2')
        )
    )
    
    harvest_P2 = pl.concat([
        harvest_1999_2009_calc, 
        harvest_2010_calc, 
        harvest_2011_2012_calc, 
        harvest_2013_2016_calc], 
        rechunk=True, 
        how='diagonal')

    col_order = [
            'HarvestYear', 
            'ID2', 
            'Longitude', 
            'Latitude', 
            'SampleID', 
            'Crop', 
            'CropExists_P1',
            'GrainSampleArea_P1', 
            'GrainMassWet_P1', 
            'GrainMassWetInGrainSample_P1', 
            'GrainMassWetInGrainSample_P2',
            'GrainMassAirDryInGrainSample_P1', 
            'GrainMassAirDryInGrainSample_P2',
            'GrainMassAirDry_P1', 
            'GrainMoistureProportionPartial_P2',
            'GrainMoisture_P1', 
            'GrainProtein_P1', 
            'GrainStarch_P1', 
            'GrainWGlutDM_P1', 
            'GrainOilDM_P1', 
            'GrainTestWeight_P1', 
            'GrainCarbon_P1', 
            'GrainNitrogen_P1', 
            'GrainSulfur_P1', 
            'BiomassSampleArea_P1', 
            'BiomassWet_P1', 
            'BiomassAirDry_P1', 
            'GrainMassWetInBiomassSample_P1', 
            'GrainMassAirDryInBiomassSample_P1', 
            'GrainMassAirDryInBiomassSample_P2', 
            'ResidueMassWetSubsample_P1', 
            'ResidueMassAirDrySubsample_P1', 
            'ResidueMoistureProportionPartialSubsample_P2', 
            'ResidueCarbon_P1', 
            'ResidueNitrogen_P1', 
            'ResidueSulfur_P1', 
            'GrainYieldWet_P2', 
            'GrainYieldAirDry_P2', 
            'ResidueMassWet_P2', 
            'ResidueMassAirDry_P2', 
            'ResidueMassWetPerArea_P2', 
            'ResidueMassAirDryPerArea_P2',
            'Comments'
        ]
    
    all_columns_ordered = core.organize_columns(col_order)

    harvest_P2 = (harvest_P2
        .select(all_columns_ordered)
        .sort(['HarvestYear', 'ID2']))
    
    return harvest_P2

def generate_p2a1(df, args):
    print('generate_p2a1')

    drop_cols = [
        'GrainSampleArea_P1', 
        'GrainMassWet_P1', 
        'GrainMassWetInGrainSample_P1', 
        'GrainMassWetInGrainSample_P2',
        'GrainMassAirDryInGrainSample_P1', 
        'GrainMassAirDryInGrainSample_P2',
        'GrainMassAirDry_P1', 
        'GrainMassDry0_P2',
        'GrainMoistureProportionPartial_P2',
        'BiomassSampleArea_P1', 
        'BiomassWet_P1', 
        'BiomassAirDry_P1', 
        'GrainMassWetInBiomassSample_P1', 
        'GrainMassAirDryInBiomassSample_P1', 
        'GrainMassAirDryInBiomassSample_P2', 
        'ResidueMassWetSubsample_P1', 
        'ResidueMassAirDrySubsample_P1', 
        'ResidueMoistureProportionPartialSubsample_P2',
        'ResidueMassWet_P2', 
        'ResidueMassAirDry_P2']
    
    df_prune = core.drop_columns_include_qc(df, drop_cols)

    # Load the QA file
    qa = pd.read_csv(args['path_qa_file_p2a0'])

    qaUpdate = qa[qa['Verb'] == 'Update']
    qaFlag = qa[qa['Verb'] == 'Flag']

    df_pd = df_prune.to_pandas()

    p1_cols = [col for col in df_pd.columns if '_P1' in col]
    df_qa = cafcore.qc.initialize_qc(df_pd, args['dimension_vars'] + p1_cols)

    df_qa = cafcore.qc.set_quality_assurance_applied(df_qa, args['dimension_vars'] + p1_cols, True, True)

    # Update values (copied from generate_p1a1, which was copied from cafcore/qc.py -- meaning I need to move this to cafcore)
    for index, row in qaUpdate.iterrows():
        prevValueSeries = df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"])]
        
        if len(prevValueSeries) == 0:
            raise Exception("ID not found, check QA File")
        if len(prevValueSeries) > 1:
            raise Exception("Multiple values found for given ID, check input dataframe")
        
        prevValue = prevValueSeries.values[0]
        changePhraseCol = row["Variable"] + "_qcPhrase"

        if(pd.isna(row["NewVal"])):
            df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"])] = None
        else:
            df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"])] = row["NewVal"]

        changePhrase = "(Assurance) Previous val: {}, reason: {}".format(prevValue, row["Comment"])
        
        # Create column if not exist
        if changePhraseCol not in df_qa.columns:
            df_qa[changePhraseCol] = None

        series = df_qa.loc[(df_qa['SampleID'] == row["ID"]), changePhraseCol]

        df_qa.loc[(df_qa['SampleID'] == row["ID"]), changePhraseCol] = cafcore.qc.update_phrase(
            df_qa.loc[series.index[0], changePhraseCol],
            changePhrase
        )

    # Flag values by setting QA bit to fail
    for index, row in qaFlag.iterrows():
        reasonPhraseCol = row['Variable'] + '_qcPhrase'
        changePhrase = "(Assurance) {}".format(row['Comment'])

        qaResultCol = row['Variable'] + '_qcResult'

        phrase_series = df_qa.loc[(df_qa['SampleID'] == row['ID']), reasonPhraseCol]

        df_qa.loc[(df_qa['SampleID'] == row['ID']), reasonPhraseCol] = cafcore.qc.update_phrase(
            df_qa.loc[phrase_series.index[0], reasonPhraseCol],
            changePhrase
        )

        df_qa.loc[(df_qa['SampleID'] == row['ID']), qaResultCol] = cafcore.qc.update_qc_bitstring(
            '000001', '000000')

    return pl.from_pandas(df_qa).sort(by=['HarvestYear', 'ID2'])

def generate_p2a2(df, args):
    print('generate_p2a2')

    # Convert to pandas since cafcore works with it
    df_pd = df.to_pandas()

    rows_original = df.shape[0]

    # Load file with bounds checks
    qc_point_params = pd.read_csv(args['path_qc_bounds_p2a1']).dropna(subset=['Lower', 'Upper'])

    # Get unique crops in df, filer df and params for each unique crop, merge them to new df
    crops_in_data = df_pd['Crop'].unique()

    result = pd.DataFrame()
    
    for crop in crops_in_data:
        # Some Crops have values of None, so handle that with generic bounds
        if crop == None:
            df_crop = df_pd[df_pd['Crop'].isna()]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == 'Generic']
        else:
            df_crop = df_pd[df_pd['Crop'] == crop]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == crop]

        #qcPointParamsCrop = qcPointParams[qcPointParams["Crop"] == crop]
        #dfCrop = df_result[df_result["Crop"] == crop]

        for paramIndex, param_row in qc_point_params_crop.iterrows():
            df_crop = cafcore.qc.process_qc_bounds_check(df_crop, param_row['FieldName'], param_row['Lower'], param_row['Upper'])

        result = pd.concat([result, df_crop], axis=0, ignore_index=True)

    # There are some Crop values that are None, add them back here
    #crop_nones = df_result[df_result['Crop'] == None]
    #result = pd.concat([result, crop_nones])

    rows_result = result.shape[0]

    if rows_original != rows_result:
        raise Exception("Resultant dataframe is different size than original")

    return pl.from_pandas(result).sort(by=['HarvestYear', 'ID2'])

def generate_p2a3(df, args):
    print('generate_p2a3')
    # Convert to pandas
    df_pd = df.to_pandas()

    rows_original = df.shape[0]

    # Load HI csv
    qc_point_params = pd.read_csv(args['path_qc_obs_hi_p2a2']).dropna(subset=['Lower', 'Upper'])

    # Create HI column
    df_pd['HarvestIndex'] = df_pd['GrainYieldAirDry_P2'] / (df_pd['GrainYieldAirDry_P2'] + df_pd['ResidueMassAirDryPerArea_P2'])

    # cafcore.qc does not have a observation check function, so manually call:
        # cafcore.qc.update_qc_bitstring
        # cafcore.qc.update_phrase
    # Get unique crops in df, filer df and params for each unique crop, merge them to new df
    crops_in_data = df_pd['Crop'].unique()

    result = pd.DataFrame()
    
    for crop in crops_in_data:
        # Some Crops have values of None, so handle that with generic bounds
        if crop == None:
            df_crop = df_pd[df_pd['Crop'].isna()]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == 'Generic']
        else:
            df_crop = df_pd[df_pd['Crop'] == crop]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == crop]

        for paramIndex, param_row in qc_point_params_crop.iterrows():
            # Convert affected columns string to list
            affected_cols = param_row['AffectedColumns'].split(',')
            df_crop = core.process_qc_observation_bounds_check(df_crop, affected_cols, param_row['FieldName'], param_row['Lower'], param_row['Upper'])

        result = pd.concat([result, df_crop], axis=0, ignore_index=True)
    
    result = result.drop(['HarvestIndex'], axis=1)
    # See process_qc_dataset_row in ProcessHarvest common.py
    rows_result = result.shape[0]

    if rows_original != rows_result:
        raise Exception("Resultant dataframe is different size than original")

    return pl.from_pandas(result).sort(by=['HarvestYear', 'ID2'])

def generate_p3a0(df, args):
    print('generate_p3a0')

    harvest_p3 = df.clone()

    # TODO: Move this to a function if I copy/paste one more time...

    # Calculate average moisture proportion to get dry mass where there is none then merge means into df
    harvest_with_residue_masses = (harvest_p3
        .filter((pl.col('ResidueMassWetPerArea_P2') > 0) & (pl.col('ResidueMassAirDryPerArea_P2') > 0))
        .with_columns(
            ((pl.col('ResidueMassWetPerArea_P2') - pl.col('ResidueMassAirDryPerArea_P2')) / pl.col('ResidueMassWetPerArea_P2'))
                      .alias('ResidueMoistureProportionFromWetAndDryMassPerAreas_P2')
            )
    )
    residue_moisture_proportion_means = (harvest_with_residue_masses
        .filter(
            (pl.col('ResidueMassWetPerArea_P2_qcResult') == '000000') &
            (pl.col('ResidueMassAirDryPerArea_P2_qcResult') == '000000'))
        .group_by(['Crop'], maintain_order=True)
        .agg(pl.col('ResidueMoistureProportionFromWetAndDryMassPerAreas_P2').drop_nans().mean().alias('ResidueMoistureProportionMeanCrop_P3')))

    harvest_p3 = harvest_p3.join(residue_moisture_proportion_means, on=['Crop'], how='left')

    # Calculate dependent variable if not exist: ResidueMassAirDryPerArea_P3
    harvest_p3 = (
        harvest_p3.with_columns(
            pl.when((pl.col('ResidueMassAirDryPerArea_P2').is_null()) & (pl.col('ResidueMassWetPerArea_P2').is_not_null()) & (pl.col('ResidueMoistureProportionMeanCrop_P3').is_not_null()))
            .then((pl.col('ResidueMassWetPerArea_P2') - (pl.col('ResidueMassWetPerArea_P2') * pl.col('ResidueMoistureProportionMeanCrop_P3'))))
            .otherwise(None)
            .alias('ResidueMassAirDryPerArea_P3')
        )
    )

    # Now do the same but for yield (other than 2011 and 2012, there's only 1 missing air dry value)
    # Calculate average moisture proportion to get dry mass where there is none then merge means into df
    harvest_with_yield_masses = (harvest_p3
        .filter((pl.col('GrainYieldWet_P2') > 0) & (pl.col('GrainYieldAirDry_P2') > 0))
        .with_columns(
            ((pl.col('GrainYieldWet_P2') - pl.col('GrainYieldAirDry_P2')) / pl.col('GrainYieldWet_P2'))
                      .alias('GrainMoistureProportionFromWetAndDryMassPerAreas_P2')
            )
    )
    grain_moisture_proportion_means = (harvest_with_yield_masses
        .filter(
            (pl.col('GrainYieldWet_P2_qcResult') == '000000') &
            (pl.col('GrainYieldAirDry_P2_qcResult') == '000000'))
        .group_by(['Crop'], maintain_order=True)
        .agg(pl.col('GrainMoistureProportionFromWetAndDryMassPerAreas_P2').drop_nans().mean().alias('GrainMoistureProportionMeanCrop_P3')))

    harvest_p3 = harvest_p3.join(grain_moisture_proportion_means, on=['Crop'], how='left')

    # Calculate dependent variable: GrainYieldAirDry_P3
    harvest_p3 = (
        harvest_p3.with_columns(
            pl.when((pl.col('GrainYieldAirDry_P2').is_null()) & (pl.col('GrainYieldWet_P2').is_not_null()) & (pl.col('GrainMoistureProportionMeanCrop_P3').is_not_null()))
            .then((pl.col('GrainYieldWet_P2') - (pl.col('GrainYieldWet_P2') * pl.col('GrainMoistureProportionMeanCrop_P3'))))
            .otherwise(None)
            .alias('GrainYieldAirDry_P3')
        )
    )

    return harvest_p3.sort(by=['HarvestYear', 'ID2'])

def generate_p3a1(df, args):
    print('generate_p3a1')

    # Load the QA file
    qa = pd.read_csv(args['path_qa_file_p3a0'])

    qaUpdate = qa[qa['Verb'] == 'Update']
    qaFlag = qa[qa['Verb'] == 'Flag']

    drop_cols = ['ResidueMoistureProportionMeanCrop_P3', 'GrainMoistureProportionMeanCrop_P3']
    df_pd = df.drop(drop_cols).to_pandas()

    p1_cols = [col for col in df_pd.columns if '_P1' in col]
    p2_cols = [col for col in df_pd.columns if '_P2' in col]
    no_qc_cols = args['dimension_vars'] + p1_cols + p2_cols

    df_qa = cafcore.qc.initialize_qc(df_pd, no_qc_cols)

    df_qa = cafcore.qc.set_quality_assurance_applied(df_qa, no_qc_cols, True, True)

    # Update values (copied from generate_p2a0, which was copied from generate_p1a1, which was copied from cafcore/qc.py -- meaning I need to move this to cafcore)
    for index, row in qaUpdate.iterrows():
        prevValueSeries = df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"])]
        
        if len(prevValueSeries) == 0:
            raise Exception("ID not found, check QA File")
        if len(prevValueSeries) > 1:
            raise Exception("Multiple values found for given ID, check input dataframe")
        
        prevValue = prevValueSeries.values[0]
        changePhraseCol = row["Variable"] + "_qcPhrase"

        if(pd.isna(row["NewVal"])):
            df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"])] = None
        else:
            df_qa.loc[(df_qa['SampleID'] == row["ID"]), (row["Variable"])] = row["NewVal"]

        changePhrase = "(Assurance) Previous val: {}, reason: {}".format(prevValue, row["Comment"])
        
        # Create column if not exist
        if changePhraseCol not in df_qa.columns:
            df_qa[changePhraseCol] = None

        series = df_qa.loc[(df_qa['SampleID'] == row["ID"]), changePhraseCol]

        df_qa.loc[(df_qa['SampleID'] == row["ID"]), changePhraseCol] = cafcore.qc.update_phrase(
            df_qa.loc[series.index[0], changePhraseCol],
            changePhrase
        )

    # Flag values by setting QA bit to fail
    for index, row in qaFlag.iterrows():
        reasonPhraseCol = row['Variable'] + '_qcPhrase'
        changePhrase = "(Assurance) {}".format(row['Comment'])

        qaResultCol = row['Variable'] + '_qcResult'

        phrase_series = df_qa.loc[(df_qa['SampleID'] == row['ID']), reasonPhraseCol]

        df_qa.loc[(df_qa['SampleID'] == row['ID']), reasonPhraseCol] = cafcore.qc.update_phrase(
            df_qa.loc[phrase_series.index[0], reasonPhraseCol],
            changePhrase
        )

        df_qa.loc[(df_qa['SampleID'] == row['ID']), qaResultCol] = cafcore.qc.update_qc_bitstring(
            '000001', '000000')

    # TODO: Do bounds checks
    return pl.from_pandas(df_qa).sort(by=['HarvestYear', 'ID2'])

def generate_p3a2(df, args):
    print('generate_p3a2')

    # Convert to pandas since cafcore works with it
    df_pd = df.to_pandas()

    rows_original = df.shape[0]

    # Load file with bounds checks
    qc_point_params = pd.read_csv(args['path_qc_bounds_p3a1']).dropna(subset=['Lower', 'Upper'])

    # Get unique crops in df, filer df and params for each unique crop, merge them to new df
    crops_in_data = df_pd['Crop'].unique()

    result = pd.DataFrame()
    
    for crop in crops_in_data:
        # Some Crops have values of None, so handle that with generic bounds
        if crop == None:
            df_crop = df_pd[df_pd['Crop'].isna()]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == 'Generic']
        else:
            df_crop = df_pd[df_pd['Crop'] == crop]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == crop]

        #qcPointParamsCrop = qcPointParams[qcPointParams["Crop"] == crop]
        #dfCrop = df_result[df_result["Crop"] == crop]

        for paramIndex, param_row in qc_point_params_crop.iterrows():
            df_crop = cafcore.qc.process_qc_bounds_check(df_crop, param_row['FieldName'], param_row['Lower'], param_row['Upper'])

        result = pd.concat([result, df_crop], axis=0, ignore_index=True)

    # There are some Crop values that are None, add them back here
    #crop_nones = df_result[df_result['Crop'] == None]
    #result = pd.concat([result, crop_nones])

    rows_result = result.shape[0]

    if rows_original != rows_result:
        raise Exception("Resultant dataframe is different size than original")

    return pl.from_pandas(result).sort(by=['HarvestYear', 'ID2'])

def generate_p3a3(df, args):
    print('generate_p3a3')
    # Convert to pandas
    df_pd = df.to_pandas()

    rows_original = df.shape[0]

    # Load HI csv
    qc_point_params = pd.read_csv(args['path_qc_obs_hi_p3a2']).dropna(subset=['Lower', 'Upper'])

    # Create HI column
    df_pd['HarvestIndex'] = df_pd['GrainYieldAirDry_P3'] / (df_pd['GrainYieldAirDry_P3'] + df_pd['ResidueMassAirDryPerArea_P3'])

    # cafcore.qc does not have a observation check function, so manually call:
        # cafcore.qc.update_qc_bitstring
        # cafcore.qc.update_phrase
    # Get unique crops in df, filer df and params for each unique crop, merge them to new df
    crops_in_data = df_pd['Crop'].unique()

    result = pd.DataFrame()
    
    for crop in crops_in_data:
        # Some Crops have values of None, so handle that with generic bounds
        if crop == None:
            df_crop = df_pd[df_pd['Crop'].isna()]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == 'Generic']
        else:
            df_crop = df_pd[df_pd['Crop'] == crop]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == crop]

        for paramIndex, param_row in qc_point_params_crop.iterrows():
            # Convert affected columns string to list
            affected_cols = param_row['AffectedColumns'].split(',')
            df_crop = core.process_qc_observation_bounds_check(df_crop, affected_cols, param_row['FieldName'], param_row['Lower'], param_row['Upper'])

        result = pd.concat([result, df_crop], axis=0, ignore_index=True)
    
    result = result.drop(['HarvestIndex'], axis=1)
    # See process_qc_dataset_row in ProcessHarvest common.py
    rows_result = result.shape[0]

    if rows_original != rows_result:
        raise Exception("Resultant dataframe is different size than original")

    return pl.from_pandas(result).sort(by=['HarvestYear', 'ID2'])
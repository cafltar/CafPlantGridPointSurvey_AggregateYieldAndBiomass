import polars as pl
import pathlib
import datetime
import pandas as pd

#https://github.com/cafltar/cafcore/releases/tag/v0.1.3
import cafcore.qc
import cafcore.file_io

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
    qa = pd.read_csv(args['path_qa_file'])

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
        
    # TODO: Return a Polar dataframe, not pandas
    return pl.from_pandas(df_qa)

def generate_p2a1(df, args):
    print("generate_p2a1")
    
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
            (pl.col('GrainMassWet_P1') - (pl.col('GrainMassWet_P1') * (pl.col('GrainMoisture_P1') / 100)))
            .alias('GrainMassDry0_P2')
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
            .then(pl.col('GrainMassDry0_P2') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassDry0_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldDry0_P2')
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
            (pl.col('GrainMassAirDry_P1') - (pl.col('GrainMassAirDry_P1') * (pl.col('GrainMoisture_P1') / 100)))
            .alias('GrainMassDry0_P2')
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
            pl.when((pl.col('GrainSampleArea_P1') > 0) & 
                ((pl.col('BiomassSampleArea_P1') == 0) | pl.col('BiomassSampleArea_P1').is_null()))
            .then(pl.col('GrainMassDry0_P2') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassDry0_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldDry0_P2')
        )
    )
    
    harvest_P2 = pl.concat([
        harvest_1999_2009_calc, 
        harvest_2010_calc, 
        harvest_2011_2012_calc, 
        harvest_2013_2016_calc], 
        rechunk=True, 
        how='diagonal')
    
    #harvest_P2 = harvest_P2.rename(
    #    {c: c+'_P1' for c in harvest_P2.columns if c not in ['HarvestYear', 'ID2', 'Longitude', 'Latitude', 'SampleID', 'Crop'] if '_P2' not in c})
    
    col_order = [
            'HarvestYear', 
            'ID2', 
            'Longitude', 
            'Latitude', 
            'SampleID', 
            'Crop', 
            'GrainSampleArea_P1', 
            'GrainMassWet_P1', 
            'GrainMassWetInGrainSample_P1', 
            'GrainMassWetInGrainSample_P2',
            'GrainMassAirDryInGrainSample_P1', 
            'GrainMassAirDryInGrainSample_P2',
            'GrainMassAirDry_P1', 
            'GrainMassDry0_P2',
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
            'Comments', 
            'GrainYieldWet_P2', 
            'GrainYieldAirDry_P2',
            'GrainYieldDry0_P2', 
            'ResidueMassWet_P2', 
            'ResidueMassAirDry_P2', 
            'ResidueMassWetPerArea_P2', 
            'ResidueMassAirDryPerArea_P2'
        ]
    
    all_columns_ordered = organize_columns(col_order)

    # TODO: Include qc columns here
    harvest_P2 = (harvest_P2
        .select(all_columns_ordered)
        .sort(['HarvestYear', 'ID2']))
    
    return harvest_P2

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
        

def append_qc_summary_cols(df:pl.DataFrame, processing_level, args):
    print ('write simplified qc files')
    # Takes dataframe with full detailed columns like _P1, _P2, _qcApplied, etc. and writes a simplified data file and a separate qc file

    # Write summary columns for qc applied and qc results
    metric_cols = get_metric_cols(df.columns, args['dimension_vars'])

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
        dtypes=harvest_dtypes)
    
    harvest_p1a0 = harvest.rename(
        {c: c+'_P1' for c in harvest.columns if c not in args['dimension_vars']})

    harvest_p1a1 = generate_p1a1(harvest_p1a0, args)
    harvest_p1a1_trim = prune_columns_outside_p_level(harvest_p1a1, 1)
    harvest_p1a1_qc = append_qc_summary_cols(harvest_p1a1_trim, 1, args)
    harvest_p1a1_qc_p = condense_processing_columns(harvest_p1a1_qc, 1)
    write_csv_files(
        harvest_p1a1_qc_p, 
        ['HarvestYear', 'ID2'], 'HY1999-2016', 1, 1, args['path_output'])
    

    harvest_p2a1 = generate_p2a1(harvest_p1a1, args)
    harvest_p2a1_qc = append_qc_summary_cols(harvest_p2a1, 2, args)

    date_today = datetime.datetime.now().strftime("%Y%m%d")
    write_name_P1A1 = 'HY1999-2016_' + str(date_today) + '_P1A1.csv'
    write_name_P2A1 = 'HY1999-2016_' + str(date_today) + '_P2A1.csv'
    
    harvest_p1a1.write_csv(args['path_output'] / str(write_name_P1A1))
    harvest_p2a1.write_csv(args['path_output'] / str(write_name_P2A1))

    print('End')

def prune_columns_outside_p_level(df, processing_level):
    df_result = df.clone()
    qc_suffixes = ['_qcApplied', '_qcResult', '_qcPhrase']
    p_suffixes = ['_P1', '_P2', '_P3']
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

def write_csv_files(df, key, file_name, processing_level, accuracy_level, output_path):
    date_today = datetime.datetime.now().strftime("%Y%m%d")
    pa_suffix = f'P{processing_level}A{accuracy_level}'

    qc_suffixes = ['_qcApplied', '_qcResult', '_qcPhrase']
    p_suffixes = ['_P1', '_P2', '_P3'] #dropping all, so no need to worry about specified processing level
    
    qc_cols = [col for col in df.columns if any(col.endswith(qc_suffix) for qc_suffix in qc_suffixes)]
    p_cols = [col for col in df.columns if any(col.endswith(p_suffix) for p_suffix in p_suffixes)]

    # Write all columns
    comprehensive_file_name = f'{file_name}_{pa_suffix}_Comprehensive_{str(date_today)}.csv'
    df.write_csv(output_path / comprehensive_file_name)

    # Write QC file
    qc_file_name = f'{file_name}_{pa_suffix}_QC_{str(date_today)}.csv'
    (df
        .select(key + qc_cols)
        .write_csv(output_path / qc_file_name)
    )

    # Write clean dataset
    clean_file_name = f'{file_name}_{pa_suffix}_{str(date_today)}.csv'
    (df
        .drop(qc_cols)
        .drop(p_cols)
        .write_csv(output_path / clean_file_name)
    )


def condense_processing_columns(df, processing_level):
    df_result = df.clone()
    p_suffixes = ['_P1', '_P2', '_P3']
    p_suffixes_keep = p_suffixes[0:processing_level]

    #p_suffixes_keep = ['_P1']
    #if processing_level == 2:
    #    p_suffixes_keep = p_suffixes_keep + ['_P2']
    #elif processing_level == 3:
    #    p_suffixes_keep = p_suffixes_keep + ['_P2', '_P3']

    # Drop any columns with processing levels higher than processing_level
    #p_suffixes_drop = [p for p in p_suffixes if p not in p_suffixes_keep]
    #p_cols_drop = [col for col in df_result.columns if any(col.endswith(p_suffix) for p_suffix in p_suffixes_drop)]
    #df_result = df_result.drop(p_cols_drop)
    
    # Get base column names for cols at processing_level -- does not assume, e.g. a P3 col has a corresponding P1 col
    #p_cols = [col for col in df.columns if any(p_suffix in col for p_suffix in p_suffixes_keep)]
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
        #df_result[col_base] = df_result[cols[0]]
        df_result = (df_result
                     .with_columns(pl.col(cols[0]).alias(col_base))
                     #.drop(cols[0])
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

def get_qc_dataset(df, index_cols):
    qc_suffixes = ['_qcApplied', '_qcResult', '_qcPhrase']
    qc_cols = [col for col in df.columns if any(qc_suffix in col for qc_suffix in qc_suffixes)]
    keep_cols = index_cols + qc_cols

    qa_df = df[keep_cols]

    return qa_df


def write_data_files(df, file_prefix, processing_level, accuracy_level):
    qa = get_qc_dataset(df, ['HarvestYear', 'ID2'])
    return True

if __name__ == '__main__':
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'

    path_output.mkdir(parents=True, exist_ok=True)
    
    args = {}
    args['path_output'] = path_output
    args['path_harvest_data'] = path_input / 'HY1999-2016_20230922_P1A0.csv'
    args['path_qa_file'] = path_input / 'qaFlagFile_All.csv'

    args['dimension_vars'] = ['HarvestYear', 'ID2', 'Longitude', 'Latitude', 'SampleID', 'Crop', 'Comments']

    main(args)
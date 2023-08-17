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
        .with_columns(pl.col('GrainMassOvenDryInGrainSample_P1').alias('GrainMassOvenDry_P1'))
        .with_columns(
            pl.when(pl.col('GrainSampleArea_P1') > 0)
            .then(pl.col('GrainMassWet_P1') / pl.col('GrainSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldWet_P2'))
        .with_columns(
            pl.when(pl.col('GrainMassWet_P1') > 0)
            .then((pl.col('GrainMassWet_P1') - pl.col('GrainMassOvenDry_P1')) / pl.col('GrainMassWet_P1'))
            .otherwise(None)
            .alias('GrainMoistureProportion_P2'))
        .with_columns(
            pl.when(pl.col('GrainSampleArea_P1') > 0)
            .then(pl.col('GrainMassOvenDry_P1') / pl.col('GrainSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2'))
    )

    harvest_1999_2009_biomass_calc = (harvest_1999_2009_biomass
        # === Grain calculations ===
        .with_columns(pl.col('GrainMassWetInBiomassSample_P1')
            .alias('GrainMassWet_P1'))
        .with_columns(pl.col('GrainMassOvenDryInBiomassSample_P1')
            .alias('GrainMassOvenDry_P1'))
        .with_columns(
            pl.when(pl.col('GrainMassWet_P1') > 0)
            .then((pl.col('GrainMassWet_P1') - pl.col('GrainMassOvenDry_P1')) / pl.col('GrainMassWet_P1'))
            .otherwise(None)
            .alias('GrainMoistureProportion_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('GrainMassWet_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldWet_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('GrainMassOvenDry_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2'))
        
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
            .then((pl.col('ResidueMassWetSubsample_P1') - pl.col('ResidueMassOvenDrySubsample_P1')) / pl.col('ResidueMassWetSubsample_P1'))
            .otherwise(None)
            .alias('ResidueMoistureProportionSubsample_P2')
        )
        # Residue mass dry via (residue wet) - (residue wet * moisture proportion from sub sample)
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionSubsample_P2')))
            .alias('ResidueMassOvenDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassWet_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassOvenDry_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassOvenDryPerArea_P2'))
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
            .then((pl.col('GrainMassWet_P1') - pl.col('GrainMassOvenDry_P1')) / pl.col('GrainMassWet_P1'))
            .otherwise(None)
            .alias('GrainMoistureProportion_P2'))
        # Use moisture proportion in grain to calculate dry mass of grain in the grain samples
        .with_columns(
            (pl.col('GrainMassWetInGrainSample_P2') - (pl.col('GrainMassWetInGrainSample_P2') * pl.col('GrainMoistureProportion_P2')))
            .alias('GrainMassOvenDryInGrainSample_P2')
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
            .then((pl.col('ResidueMassWetSubsample_P1') - pl.col('ResidueMassOvenDrySubsample_P1')) / pl.col('ResidueMassWetSubsample_P1'))
            .otherwise(None)
            .alias('ResidueMoistureProportionSubsample_P2')
        )
        # Residue mass dry from residue moisture proportions
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionSubsample_P2')))
            .alias('ResidueMassOvenDry_P2')
        )

        # === Biomass ===
        # Use moisture proportion in grain to calculate dry mass of grain in the biomass samples
        .with_columns(
            (pl.col('GrainMassWetInBiomassSample_P1') - (pl.col('GrainMassWetInBiomassSample_P1') * pl.col('GrainMoistureProportion_P2')))
            .alias('GrainMassOvenDryInBiomassSample_P2')
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
            .then(pl.col('GrainMassOvenDry_P1') / (pl.col('GrainSampleArea_P1') + pl.col('BiomassSampleArea_P1')))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassWet_P2') / (pl.col('BiomassSampleArea_P1')))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassOvenDry_P2') / (pl.col('BiomassSampleArea_P1')))
            .otherwise(None)
            .alias('ResidueMassOvenDryPerArea_P2'))
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
            .then((pl.col('GrainMassWet_P1') - pl.col('GrainMassOvenDry_P1')) / pl.col('GrainMassWet_P1'))
            .otherwise(None)
            .alias('GrainMoistureProportion_P2')
        )
        # Residue subsample moisture proportions via: (wet - dry) / wet
        .with_columns(
            pl.when(pl.col('ResidueMassWetSubsample_P1') > 0)
            .then((pl.col('ResidueMassWetSubsample_P1') - pl.col('ResidueMassOvenDrySubsample_P1')) / pl.col('ResidueMassWetSubsample_P1'))
            .otherwise(None)
            .alias('ResidueMoistureProportionSubsample_P2')
        )
        # Residue mass dry from residue moisture proportions
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionSubsample_P2')))
            .alias('ResidueMassOvenDry_P2')
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
            .then(pl.col('GrainMassOvenDry_P1') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassOvenDry_P1') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassWet_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea_P1') > 0)
            .then(pl.col('ResidueMassOvenDry_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('ResidueMassOvenDryPerArea_P2')
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
            .alias('GrainMassOvenDry_P2')
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
            .then(pl.col('GrainMassOvenDry_P2') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassOvenDry_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2')
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
            .alias('GrainMassOvenDry_P2')
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
            .then(pl.col('GrainMassOvenDry_P2') / pl.col('GrainSampleArea_P1'))
            .when(((pl.col('GrainSampleArea_P1') == 0) | pl.col('GrainSampleArea_P1').is_null()) & 
                (pl.col('BiomassSampleArea_P1') > 0))
            .then(pl.col('GrainMassOvenDry_P2') / pl.col('BiomassSampleArea_P1'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2')
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
    
    harvest_P2 = (harvest_P2
        .select([
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
            'GrainMassOvenDryInGrainSample_P1', 
            'GrainMassOvenDryInGrainSample_P2',
            'GrainMassAirDry_P1', 
            'GrainMassOvenDry_P1', 
            'GrainMassOvenDry_P2',
            'GrainMoistureProportion_P2',
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
            'GrainMassOvenDryInBiomassSample_P1', 
            'GrainMassOvenDryInBiomassSample_P2', 
            'ResidueMassWetSubsample_P1', 
            'ResidueMassOvenDrySubsample_P1', 
            'ResidueMoistureProportionSubsample_P2', 
            'ResidueCarbon_P1', 
            'ResidueNitrogen_P1', 
            'ResidueSulfur_P1', 
            'Comments_P1', 
            'GrainYieldWet_P2', 
            'GrainYieldAirDry_P2',
            'GrainYieldOvenDry_P2', 
            'ResidueMassWet_P2', 
            'ResidueMassOvenDry_P2', 
            'ResidueMassWetPerArea_P2', 
            'ResidueMassOvenDryPerArea_P2',  
            'ResidueMassAirDry_P2', 
            'ResidueMassAirDryPerArea_P2'
        ])
        .sort(['HarvestYear', 'ID2']))
    
    return harvest_P2

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
        pl.Float64,
        pl.Int32,
        pl.Utf8
    ]

    harvest = pl.read_csv(
        args['path_harvest_data'],
        dtypes=harvest_dtypes)
    
    harvest_p1 = harvest.rename(
        {c: c+'_P1' for c in harvest.columns if c not in args['dimension_vars']})

    harvest_p1a1 = generate_p1a1(harvest_p1, args)
    harvest_p2a1 = generate_p2a1(harvest_p1a1, args)
    
    date_today = datetime.datetime.now().strftime("%Y%m%d")
    #write_name_P2A0 = 'HY1999-2016_' + str(date_today) + '_P2A0.csv'   
    #.write_csv(args['path_output'] / str(write_name_P2A0)))

    print('End')

if __name__ == '__main__':
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'

    path_output.mkdir(parents=True, exist_ok=True)
    
    args = {}
    args['path_output'] = path_output
    args['path_harvest_data'] = path_input / 'HY1999-2016_20230815_P1A0.csv'
    args['path_qa_file'] = path_input / 'qaFlagFile_All.csv'

    args['dimension_vars'] = ['HarvestYear', 'ID2', 'Longitude', 'Latitude', 'SampleID', 'Crop', 'Comments']

    main(args)
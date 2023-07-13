import polars as pl
import pathlib
import datetime

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
        pl.Utf8
    ]

    harvest = pl.read_csv(
        args['path_harvest_data'],
        dtypes=harvest_dtypes)
    
    
    harvest_1999_2009 = harvest.filter((pl.col('HarvestYear') >= 1999) & (pl.col('HarvestYear') <= 2009))
    harvest_2010 = harvest.filter((pl.col('HarvestYear') == 2010))
    harvest_2011_2012 = harvest.filter((pl.col('HarvestYear') >= 2011) & (pl.col('HarvestYear') <= 2012))
    harvest_2013_2016 = harvest.filter((pl.col('HarvestYear') >= 2013) & (pl.col('HarvestYear') <= 2016))

    # Check that we didn't leave any rows behind (or duplicated)
    split_row_sum = (
        harvest_1999_2009.shape[0] + 
        harvest_2010.shape[0] + 
        harvest_2011_2012.shape[0] + 
        harvest_2013_2016.shape[0])
    if(harvest.shape[0] != split_row_sum):
        print("Warning! There was a loss or gain of rows")

    # Further split 1999-2010 calculations by way the sample was collected
    harvest_1999_2009_grain = harvest_1999_2009.filter(
        (pl.col('GrainSampleArea') > 0) & 
        ((pl.col('BiomassSampleArea') == 0) | pl.col('BiomassSampleArea').is_null())
    )
    harvest_1999_2009_biomass = harvest_1999_2009.filter(
        ((pl.col('GrainSampleArea') == 0) | pl.col('GrainSampleArea').is_null()) & 
        (pl.col('BiomassSampleArea') > 0 )
    )
    harvest_1999_2009_both = harvest_1999_2009.filter(
        (pl.col('GrainSampleArea') > 0) & 
        (pl.col('BiomassSampleArea') > 0 )
    )
    harvest_1999_2009_neither = harvest_1999_2009.filter(
        (pl.col('GrainSampleArea').is_null() | (pl.col('GrainSampleArea') == 0)) &
        (pl.col('BiomassSampleArea').is_null() | (pl.col('BiomassSampleArea') == 0))
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
        .with_columns(pl.col('GrainMassWetInGrainSample').alias('GrainMassWet'))
        .with_columns(pl.col('GrainMassOvenDryInGrainSample').alias('GrainMassOvenDry'))
        .with_columns(
            pl.when(pl.col('GrainSampleArea') > 0)
            .then(pl.col('GrainMassWet') / pl.col('GrainSampleArea'))
            .otherwise(None)
            .alias('GrainYieldWet_P2'))
        .with_columns(
            pl.when(pl.col('GrainMassWet') > 0)
            .then((pl.col('GrainMassWet') - pl.col('GrainMassOvenDry')) / pl.col('GrainMassWet'))
            .otherwise(None)
            .alias('GrainMoistureProportion_P2'))
        .with_columns(
            pl.when(pl.col('GrainSampleArea') > 0)
            .then(pl.col('GrainMassOvenDry') / pl.col('GrainSampleArea'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2'))
    )

    harvest_1999_2009_biomass_calc = (harvest_1999_2009_biomass
        # === Grain calculations ===
        .with_columns(pl.col('GrainMassWetInBiomassSample')
            .alias('GrainMassWet'))
        .with_columns(pl.col('GrainMassOvenDryInBiomassSample')
            .alias('GrainMassOvenDry'))
        .with_columns(
            pl.when(pl.col('GrainMassWet') > 0)
            .then((pl.col('GrainMassWet') - pl.col('GrainMassOvenDry')) / pl.col('GrainMassWet'))
            .otherwise(None)
            .alias('GrainMoistureProportion_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('GrainMassWet') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('GrainYieldWet_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('GrainMassOvenDry') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2'))
        
        # === Residue calculations ===
        # Residue mass wet via biomass - grain mass 
        .with_columns(
            pl.when(pl.col('BiomassWet') > 0)
            .then(pl.col('BiomassWet') - pl.col('GrainMassWetInBiomassSample'))
            .otherwise(None)
            .alias('ResidueMassWet_P2')
        )
        # Residue subsample oisture proportions via: (wet - dry) / wet
        .with_columns(
            pl.when(pl.col('ResidueMassWetSubsample') > 0)
            .then((pl.col('ResidueMassWetSubsample') - pl.col('ResidueMassOvenDrySubsample')) / pl.col('ResidueMassWetSubsample'))
            .otherwise(None)
            .alias('ResidueMoistureProportionSubsample_P2')
        )
        # Residue mass dry via (residue wet) - (residue wet * moisture proportion from sub sample)
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionSubsample_P2')))
            .alias('ResidueMassOvenDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('ResidueMassWet_P2') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('ResidueMassOvenDry_P2') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('ResidueMassOvenDryPerArea_P2'))
    )

    # 
    harvest_1999_2009_both_calc = (harvest_1999_2009_both
        # === Grain ===
        # Grain in grain sample
        .with_columns(
            (pl.col('GrainMassWet') - pl.col('GrainMassWetInBiomassSample'))
            .alias('GrainMassWetInGrainSample_P2')
        )
        # Moisture in grain
        .with_columns(
            pl.when(pl.col('GrainMassWet') > 0)
            .then((pl.col('GrainMassWet') - pl.col('GrainMassOvenDry')) / pl.col('GrainMassWet'))
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
            pl.when(pl.col('BiomassWet') > 0)
            .then(pl.col('BiomassWet') - pl.col('GrainMassWetInBiomassSample'))
            .otherwise(None)
            .alias('ResidueMassWet_P2')
        )
        # Residue subsample moisture proportions via: (wet - dry) / wet
        .with_columns(
            pl.when(pl.col('ResidueMassWetSubsample') > 0)
            .then((pl.col('ResidueMassWetSubsample') - pl.col('ResidueMassOvenDrySubsample')) / pl.col('ResidueMassWetSubsample'))
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
            (pl.col('GrainMassWetInBiomassSample') - (pl.col('GrainMassWetInBiomassSample') * pl.col('GrainMoistureProportion_P2')))
            .alias('GrainMassOvenDryInBiomassSample_P2')
        )

        # === Per area ===
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('GrainMassWet') / (pl.col('GrainSampleArea') + pl.col('BiomassSampleArea')))
            .otherwise(None)
            .alias('GrainYieldWet_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('GrainMassOvenDry') / (pl.col('GrainSampleArea') + pl.col('BiomassSampleArea')))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('ResidueMassWet_P2') / (pl.col('BiomassSampleArea')))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2'))
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('ResidueMassOvenDry_P2') / (pl.col('BiomassSampleArea')))
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
            (pl.col('BiomassWet') - pl.col('GrainMassWet'))
            .alias('ResidueMassWet_P2')
        )
        .with_columns(
            pl.when(pl.col('GrainMassWet') > 0)
            .then((pl.col('GrainMassWet') - pl.col('GrainMassOvenDry')) / pl.col('GrainMassWet'))
            .otherwise(None)
            .alias('GrainMoistureProportion_P2')
        )
        # Residue subsample moisture proportions via: (wet - dry) / wet
        .with_columns(
            pl.when(pl.col('ResidueMassWetSubsample') > 0)
            .then((pl.col('ResidueMassWetSubsample') - pl.col('ResidueMassOvenDrySubsample')) / pl.col('ResidueMassWetSubsample'))
            .otherwise(None)
            .alias('ResidueMoistureProportionSubsample_P2')
        )
        # Residue mass dry from residue moisture proportions
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionSubsample_P2')))
            .alias('ResidueMassOvenDry_P2')
        )

        .with_columns(
            pl.when((pl.col('GrainSampleArea') > 0) & 
                ((pl.col('BiomassSampleArea') == 0) | pl.col('BiomassSampleArea').is_null()))
            .then(pl.col('GrainMassWet') / pl.col('GrainSampleArea'))
            .when(((pl.col('GrainSampleArea') == 0) | pl.col('GrainSampleArea').is_null()) & 
                (pl.col('BiomassSampleArea') > 0))
            .then(pl.col('GrainMassWet') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('GrainYieldWet_P2')
        )
        .with_columns(
            pl.when((pl.col('GrainSampleArea') > 0) & 
                ((pl.col('BiomassSampleArea') == 0) | pl.col('BiomassSampleArea').is_null()))
            .then(pl.col('GrainMassOvenDry') / pl.col('GrainSampleArea'))
            .when(((pl.col('GrainSampleArea') == 0) | pl.col('GrainSampleArea').is_null()) & 
                (pl.col('BiomassSampleArea') > 0))
            .then(pl.col('GrainMassOvenDry') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('ResidueMassWet_P2') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('ResidueMassOvenDry_P2') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('ResidueMassOvenDryPerArea_P2')
        )
    )
    
    harvest_2011_2012_calc = (harvest_2011_2012
        .with_columns(
            (pl.col('BiomassWet') - pl.col('GrainMassWet'))
            .alias('ResidueMassWet_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('ResidueMassWet_P2') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('ResidueMassWetPerArea_P2')
        )
        .with_columns(
            (pl.col('GrainMassWet') - (pl.col('GrainMassWet') * (pl.col('GrainMoisture') / 100)))
            .alias('GrainMassOvenDry_P2')
        )
        .with_columns(
            pl.when((pl.col('GrainSampleArea') > 0) & 
                ((pl.col('BiomassSampleArea') == 0) | pl.col('BiomassSampleArea').is_null()))
            .then(pl.col('GrainMassWet') / pl.col('GrainSampleArea'))
            .when(((pl.col('GrainSampleArea') == 0) | pl.col('GrainSampleArea').is_null()) & 
                (pl.col('BiomassSampleArea') > 0))
            .then(pl.col('GrainMassWet') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('GrainYieldWet_P2')
        )
        .with_columns(
            pl.when((pl.col('GrainSampleArea') > 0) & 
                ((pl.col('BiomassSampleArea') == 0) | pl.col('BiomassSampleArea').is_null()))
            .then(pl.col('GrainMassOvenDry_P2') / pl.col('GrainSampleArea'))
            .when(((pl.col('GrainSampleArea') == 0) | pl.col('GrainSampleArea').is_null()) & 
                (pl.col('BiomassSampleArea') > 0))
            .then(pl.col('GrainMassOvenDry_P2') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('GrainYieldOvenDry_P2')
        )
    )
    
    harvest_2013_2016_calc = (harvest_2013_2016
        .with_columns(
            (pl.col('BiomassAirDry') - pl.col('GrainMassAirDry'))
            .alias('ResidueMassAirDry_P2')
        )
        .with_columns(
            pl.when(pl.col('BiomassSampleArea') > 0)
            .then(pl.col('ResidueMassAirDry_P2') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('ResidueMassAirDryPerArea_P2')
        )
        .with_columns(
            (pl.col('GrainMassAirDry') - (pl.col('GrainMassAirDry') * (pl.col('GrainMoisture') / 100)))
            .alias('GrainMassOvenDry_P2')
        )
        .with_columns(
            pl.when((pl.col('GrainSampleArea') > 0) & 
                ((pl.col('BiomassSampleArea') == 0) | pl.col('BiomassSampleArea').is_null()))
            .then(pl.col('GrainMassAirDry') / pl.col('GrainSampleArea'))
            .when(((pl.col('GrainSampleArea') == 0) | pl.col('GrainSampleArea').is_null()) & 
                (pl.col('BiomassSampleArea') > 0))
            .then(pl.col('GrainMassAirDry') / pl.col('BiomassSampleArea'))
            .otherwise(None)
            .alias('GrainYieldAirDry_P2')
        )
        .with_columns(
            pl.when((pl.col('GrainSampleArea') > 0) & 
                ((pl.col('BiomassSampleArea') == 0) | pl.col('BiomassSampleArea').is_null()))
            .then(pl.col('GrainMassOvenDry_P2') / pl.col('GrainSampleArea'))
            .when(((pl.col('GrainSampleArea') == 0) | pl.col('GrainSampleArea').is_null()) & 
                (pl.col('BiomassSampleArea') > 0))
            .then(pl.col('GrainMassOvenDry_P2') / pl.col('BiomassSampleArea'))
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
    
    harvest_P2 = harvest_P2.rename(
        {c: c+'_P1' for c in harvest_P2.columns if c not in ['HarvestYear', 'ID2', 'Longitude', 'Latitude', 'SampleID', 'Crop'] if '_P2' not in c})

    date_today = datetime.datetime.now().strftime("%Y%m%d")
    write_name_P2A0 = 'HY1999-2016_' + str(date_today) + '_P2A0.csv'

    (harvest_P2
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
    .sort(['HarvestYear', 'ID2'])
    .write_csv(args['path_output'] / str(write_name_P2A0)))

    print('End')

if __name__ == '__main__':
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'

    path_output.mkdir(parents=True, exist_ok=True)
    
    args = {}
    args['path_output'] = path_output
    args['path_harvest_data'] = path_input / 'HY1999-2016_20230710_P1A0.csv'

    main(args)
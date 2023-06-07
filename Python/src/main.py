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
    
    harvest_1999_2010 = harvest.filter((pl.col('HarvestYear') >= 1999) & (pl.col('HarvestYear') <= 2010))
    harvest_2011_2012 = harvest.filter((pl.col('HarvestYear') >= 2011) & (pl.col('HarvestYear') <= 2012))
    harvest_2013_2016 = harvest.filter((pl.col('HarvestYear') >= 2013) & (pl.col('HarvestYear') <= 2016))

    # Check that we didn't leave any rows behind (or duplicated)
    split_row_sum = harvest_1999_2010.shape[0] + harvest_2011_2012.shape[0] + harvest_2013_2016.shape[0]
    if(harvest.shape[0] != split_row_sum):
        print("Warning! There was a loss or gain of rows")

    # Further split 1999-2010 calculations by way the sample was collected
    harvest_1999_2010_grain = harvest_1999_2010.filter(
        (pl.col('GrainSampleArea') > 0) & 
        ((pl.col('BiomassSampleArea') == 0) | pl.col('BiomassSampleArea').is_null())
    )
    harvest_1999_2010_biomass = harvest_1999_2010.filter(
        ((pl.col('GrainSampleArea') == 0) | pl.col('GrainSampleArea').is_null()) & 
        (pl.col('BiomassSampleArea') > 0 )
    )
    harvest_1999_2010_both = harvest_1999_2010.filter(
        (pl.col('GrainSampleArea') > 0) & 
        (pl.col('BiomassSampleArea') > 0 )
    )
    harvest_1999_2010_neither = harvest_1999_2010.filter(
        (pl.col('GrainSampleArea').is_null() | (pl.col('GrainSampleArea') == 0)) &
        (pl.col('BiomassSampleArea').is_null() | (pl.col('BiomassSampleArea') == 0))
    )
    
    # Check sums
    harvest_1999_2010_split_check = (
        harvest_1999_2010_grain.shape[0] + 
        harvest_1999_2010_biomass.shape[0] + 
        harvest_1999_2010_both.shape[0] + 
        harvest_1999_2010_neither.shape[0]
    )
    if(harvest_1999_2010_split_check != harvest_1999_2010.shape[0]):
        print('WARNING: 1999-2010 dataframe splits dont add up')

    harvest_1999_2010_grain_calc = (harvest_1999_2010_grain
        .with_columns(pl.col('GrainMassWetInGrainSample').alias('GrainMassWet'))
        .with_columns(pl.col('GrainMassOvenDryInGrainSample').alias('GrainMassOvenDry'))
        .with_columns((pl.col('GrainMassWet') / pl.col('GrainSampleArea')).alias('GrainYieldWet_P2'))
        .with_columns(
            ((pl.col('GrainMassWet') - pl.col('GrainMassOvenDry')) / pl.col('GrainMassWet'))
            .alias('GrainMoistureProportion_P2'))
        .with_columns((pl.col('GrainMassOvenDry') / pl.col('GrainSampleArea')).alias('GrainYieldOvenDry_P2'))
    )

    harvest_1999_2010_biomass_calc = (harvest_1999_2010_biomass
        # Grain calculations
        .with_columns(pl.col('GrainMassWetInBiomassSample').alias('GrainMassWet'))
        .with_columns(pl.col('GrainMassOvenDryInBiomassSample').alias('GrainMassOvenDry'))
        .with_columns((pl.col('GrainMassWet') / pl.col('BiomassSampleArea')).alias('GrainYieldWet_P2'))
        .with_columns(
            ((pl.col('GrainMassWet') - pl.col('GrainMassOvenDry')) / pl.col('GrainMassWet'))
            .alias('GrainMoistureProportion_P2'))
        .with_columns((pl.col('GrainMassOvenDry') / pl.col('BiomassSampleArea')).alias('GrainYieldOvenDry_P2'))
        
        # Residue calculations
        # Residue mass wet via biomass - grain mass 
        .with_columns(
            (pl.col('BiomassWet') - pl.col('GrainMassWetInBiomassSample'))
            .alias('ResidueMassWet_P2')
        )
        # Residue subsample oisture proportions via: (wet - dry) / wet
        .with_columns(
            ((pl.col('ResidueMassWetSubsample') - pl.col('ResidueMassOvenDrySubsample')) / pl.col('ResidueMassWetSubsample'))
             .alias('ResidueMoistureProportionSubsample_P2')
        )
        # Residue mass dry via (residue wet) - (residue wet * moisture proportion from sub sample)
        .with_columns(
            (pl.col('ResidueMassWet_P2') - (pl.col('ResidueMassWet_P2') * pl.col('ResidueMoistureProportionSubsample_P2')))
            .alias('ResidueMassOvenDry_P2')
        )

        #TODO: Add columns for per area residue (wet and dry)
    )

    harvest_1999_2010_calc = pl.concat([harvest_1999_2010_grain_calc, 
                                        harvest_1999_2010_biomass_calc, 
                                        harvest_1999_2010_both,
                                        harvest_1999_2010_neither],
                                        rechunk=True,
                                        how = 'diagonal')

    harvest_2011_2012_calc = (harvest_2011_2012
        .with_columns(
            (pl.col('BiomassWet') - pl.col('GrainMassWet'))
            .alias('ResidueMassWet_P2')
        )
        .with_columns(
            (pl.col('BiomassWet') - (pl.col('BiomassWet') * (pl.col('GrainMoisture') / 100)))
            .alias('GrainMassOvenDry_P2')
        ))
    
    harvest_2013_2016_calc = (harvest_2013_2016
        .with_columns(
            (pl.col('BiomassWet') - pl.col('GrainMassWet'))
            .alias('ResidueMassWet_P2')
        )
        .with_columns(
            (pl.col('BiomassWet') - (pl.col('BiomassWet') * (pl.col('GrainMoisture') / 100)))
            .alias('GrainMassOvenDry_P2')
        ))
    
    harvest_P2 = pl.concat([harvest_1999_2010_calc, harvest_2011_2012_calc, harvest_2013_2016_calc], rechunk=True, how='diagonal')
    
    print('End')

if __name__ == '__main__':
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'
    
    args = {}
    args['path_harvest_data'] = path_input / 'HY1999-2016_20230607_P1A0.csv'

    main(args)
import astropy.time
from lsst.daf.butler import Butler
import move_embargo_args



def populate_fake_butler(time):
    """
    Uses our code (maybe some other code) to move files to our fake butler
    from '/repo/embargo'
    
    We could create a new code that
    """
    # Probably will have to run the main program in 
    
    butler = Butler('/repo/embargo')
    registry = butler.registry
    dest = Butler('fake_from/', writeable=True)
    scratch_registry = dest.registry
    datasetType = 'raw'
    collections = 'LATISS/raw/all'
    instrument = 'LATISS'
    band = 'g'

    dataId = {'instrument': instrument}

    # Define time window to be slightly larger than the embargo period of 30 days
    window = astropy.time.TimeDelta(35, format='jd')
    time_astropy = astropy.time.Time(time)
    int_time = int(time_astropy.datetime.strftime("%Y%m%d"))


    within_window = []
    times = []
    for i, dt in enumerate(registry.queryDimensionRecords('exposure', dataId=dataId, datasets=datasetType,
                                                          collections=collections,
                                                          where="now - window < exposure.day_obs and now + window > exposure.day_obs",
                                                          bind={"now": int_time,
                                                                "window": 35})):
        end_time = dt.timespan.end
        if (time_astropy - window < end_time) and (time_astropy + window > end_time):
            within_window.append(dt.id)
            times.append(end_time)
    print(f'moving {len(within_window)} data refs')
    print(within_window)
    print(times)
    # Query the DataIds after embargo period
    datasetRefs = registry.queryDatasets(datasetType, dataId=dataId, collections=collections,
                                         where="exposure.id IN (exposure_ids)",
                                         bind={"exposure_ids": within_window})
    dest.transfer_from(butler, source_refs=datasetRefs, transfer='copy',
                           skip_missing=True, register_dataset_types=True,
                           transfer_dimensions=True)
        
        


if __name__ == '__main__':
    # do the thing
    time_list = ['2023-04-18 20:27:39.012635']
    for time in time_list:
        
        populate_fake_butler(time)

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
    datasetType = namespace.datasettype
    collections = namespace.collections

    # Dataset to move
    # dataID must include
    if not [x for x in (namespace.instrument, namespace.detector, namespace.band) if x is None]:
        dataId = {'instrument': namespace.instrument, 'detector': namespace.detector,
                  'band': namespace.band}
    elif not [x for x in (namespace.instrument,
                          namespace.detector,
                          namespace.band,
                          namespace.exposure) if x is None]:
        dataId = {'instrument': namespace.instrument, 'detector': namespace.detector,
                  'band': namespace.band, 'exposure': namespace.exposure}
    else:
        dataId = {'instrument': namespace.instrument}

    # Define time window to be slightly larger than the embargo period of 30 days
    embargo_period = astropy.time.TimeDelnow = astropy.time.Time.now()
    int_time = int(time.datetime.strftime("%Y%m%d"))

    within_window = []
    for i, dt in enumerate(registry.queryDimensionRecords('exposure', dataId=dataId, datasets=datasetType,
                                                          collections=collections,
                                                          where="now - exposure.day_obs < embargo_period or now + exposure.day_obs > embargo_period",
                                                          bind={"now": int_time,
                                                                "embargo_period": 35})):
        end_time = dt.timespan.end
        if (now - end_time < embargo_period) or (now + end_time > embargo_period):
            within_window.append(dt.id)

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

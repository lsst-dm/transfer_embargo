from lsst.daf.butler import Butler
import move_embargo_args



def populate_fake_butler(time):
    """
    Uses our code (maybe some other code) to move files to our fake butler
    from '/repo/embargo'
    
    We could create a new code that
    """
    # Probably will have to run the main program in 
    
    butler = Butler(namespace.fromrepo)
    registry = butler.registry
    dest = Butler(namespace.torepo, writeable=True)
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

    # Define embargo period
    embargo_period = astropy.time.TimeDelta(namespace.embargodays, format='jd')
    now = astropy.time.Time.now()
    int_now = int(now.datetime.strftime("%Y%m%d"))

    # The Dimensions query
    # If now - observation_end_time_in_embargo > embargo period : move
    # Else: don't move
    # Save data Ids of these observations into a list
    after_embargo = []

    for i, dt in enumerate(registry.queryDimensionRecords('exposure', dataId=dataId, datasets=datasetType,
                                                          collections=collections,
                                                          where="now - exposure.day_obs > embargo_period",
                                                          bind={"now": int_now,
                                                                "embargo_period": namespace.embargodays})):
        end_time = dt.timespan.end
        if now - end_time > embargo_period:
            after_embargo.append(dt.id)

    # Query the DataIds after embargo period
    datasetRefs = registry.queryDatasets(datasetType, dataId=dataId, collections=collections,
                                         where="exposure.id IN (exposure_ids)",
                                         bind={"exposure_ids": after_embargo})

    # Copy the Dataset after embargo period from
    # embargo butler to scratch butler.
    if TRANSFER:
        dest.transfer_from(butler, source_refs=datasetRefs, transfer='copy',
                           skip_missing=True, register_dataset_types=True,
                           transfer_dimensions=True)
        
        


if __name__ == '__main__':
    # do the thing
    time_list = [time_1, time_2]
    for time in time_list:
        
        populate_fake_butler(time)

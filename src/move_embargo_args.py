import argparse
import astropy.time
from lsst.daf.butler import Butler

# remove_collection clears the collection from scratch_butler if set to True.
REMOVE_COLLECTION = False

# transfers data from embargo to scratch butler when set to True.
TRANSFER = False


def parse_args():
    parser = argparse.ArgumentParser(description='Transferring data from embargo butler to another butler')

    # at least one arg in dataId needed for 'where' clause.
    parser.add_argument(
        "-f", "--fromrepo", type=str,
        required=True, default='/repo/embargo',
        help="Butler Repository path from which data is transferred. Input str. Default = '/repo/embargo'")
    parser.add_argument(
        "-t", "--torepo", type=str, default='/home/j/jarugula/scratch',
        required=True, help="Repository to which data is transferred. Input str")
    parser.add_argument("-d", "--embargodays", type=int, required=True, default=30,
                        help="Embargo time period in days. Input int")
    parser.add_argument("--instrument", type=str, required=True, default='LATISS',
                        help="Instrument. Input str")
    parser.add_argument("--datasettype", type=str, required=False, default='raw',
                        help="Dataset type. Input str")
    parser.add_argument("--collections", type=str, required=False, default='LATISS/raw/all',
                        help="Data Collections. Input str")
    parser.add_argument("--detector", type=int, required=False, default=0,
                        help="Detector number. Input int")
    parser.add_argument("--band", type=str, required=False, default='g',
                        help="Band. Input str")
    parser.add_argument("--exposure", type=int, required=False, default=2022091400696,
                        help="Exposure id. Input int")

    return parser.parse_args()


if __name__ == "__main__":
    namespace = parse_args()
    # Define embargo and scratch butler
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
    elif not [x for x in (namespace.instrument, namespace.detector, namespace.band, namespace.exposure) if x is None]:
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
    # Remove collection from scratch butler
    if REMOVE_COLLECTION:
        dest.pruneCollection(collections, purge=True, unstore=True)

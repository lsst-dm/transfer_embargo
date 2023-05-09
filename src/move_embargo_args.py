import argparse
import astropy.time
from lsst.daf.butler import Butler, Timespan

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
    parser.add_argument("--embargohours", type=float, required=True, default=80.0,
                        help="Embargo time period in hours. Input float")
    parser.add_argument("--instrument", type=str, required=True, default='LATISS',
                        help="Instrument. Input str")
    parser.add_argument("--datasettype", type=str, required=False, default='raw',
                        help="Dataset type. Input str")
    parser.add_argument("--collections", type=str, required=False, default='LATISS/raw/all',
                        help="Data Collections. Input str")
    parser.add_argument("--nowtime", type=str, required=False, default='now',
                        help="Now time in (ISO, TAI timescale). If left blank it will \
                        use astropy.time.Time.now.")
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
    dataId = {'instrument': namespace.instrument}

    # Define embargo period
    embargo_period = astropy.time.TimeDelta(namespace.embargohours*3600., format='sec')
    if namespace.nowtime != 'now':
        now = astropy.time.Time(namespace.nowtime, scale='tai', format='iso')
    else:
        now = astropy.time.Time.now()
    timespan_embargo = Timespan(now - embargo_period, now)
    # The Dimensions query
    # If (now - embargo period, now) does not overlap
    # with observation time interval: move
    # Else: don't move
    # Save data Ids of these observations into a list
    after_embargo = [dt.id for dt in
                     registry.queryDimensionRecords('exposure', dataId=dataId, datasets=datasetType,
                                                    collections=collections,
                                                    where="NOT exposure.timespan OVERLAPS\
                                                    timespan_embargo",
                                                    bind={"timespan_embargo": timespan_embargo})]
    # after_embargo = []
    # for i, dt in enumerate(registry.queryDimensionRecords('exposure',
    #                          dataId=dataId,
    #                          datasets=datasetType,
    #                          collections=collections,
    #                          where="NOT exposure.timespan\
    #                          OVERLAPS\
    #                          timespan_embargo",
    #                          bind={"timespan_embargo": timespan_embargo})):
    #     end_time = dt.timespan.end
    #     if now - end_time >= embargo_period:
    #        after_embargo.append(dt.id)

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

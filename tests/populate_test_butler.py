import argparse
import astropy.time
from lsst.daf.butler import Butler
import move_embargo_args

def parse_args():
    parser = argparse.ArgumentParser(description='Transferring data into fake_from butler, for use in testing')
    # at least one arg in dataId needed for 'where' clause.
    parser.add_argument(
        "-f", "--fromrepo", type=str,
        required=True, default='/repo/embargo',
        help="Butler Repository path from which data is transferred. Input str. Default = '/repo/embargo'")
    parser.add_argument(
        "-t", "--torepo", type=str,
        required=True, help="Repository to which data is transferred. Input str")
    parser.add_argument("-m", "--move_times", type=str, required=True,
                        help="List of times you want to move", nargs="*")
    parser.add_argument("-d", "--window_days", type=int, required=True, default=35,
                        help="Time window around each time list entry you want to move")
    return parser.parse_args()

def populate_fake_butler(from_repo, to_repo, time, window_days, verbose = False):
    """
    Moves files to our fake butler (fake_from) from '/repo/embargo'
    
    """
    # Probably will have to run the main program in 
    
    butler = Butler(from_repo)
    registry = butler.registry
    dest = Butler(to_repo, writeable=True)
    scratch_registry = dest.registry
    datasetType = 'raw'
    collections = 'LATISS/raw/all'
    instrument = 'LATISS'
    band = 'g'
    dataId = {'instrument': instrument}

    # Define time window to be slightly larger than the embargo period of 30 days
    window = astropy.time.TimeDelta(window_days, format='jd')
    time_astropy = astropy.time.Time(time)
    int_time = int(time_astropy.datetime.strftime("%Y%m%d"))
    within_window = []
    times = []
    for i, dt in enumerate(registry.queryDimensionRecords('exposure', dataId=dataId, datasets=datasetType,
                                                          collections=collections,
                                                          where="now - window < exposure.day_obs and now + window > exposure.day_obs",
                                                          bind={"now": int_time,
                                                                "window": window_days})):
        end_time = dt.timespan.end
        if (time_astropy - window < end_time) and (time_astropy + window > end_time):
            within_window.append(dt.id)
            times.append(end_time)
    if verbose:
        print(f'moving {len(within_window)} data refs')
    # Query the DataIds after embargo period
    datasetRefs = registry.queryDatasets(datasetType, dataId=dataId, collections=collections,
                                         where="exposure.id IN (exposure_ids)",
                                         bind={"exposure_ids": within_window})
    dest.transfer_from(butler, source_refs=datasetRefs, transfer='copy',
                           skip_missing=True, register_dataset_types=True,
                           transfer_dimensions=True)

def main():
    namespace = parse_args()
    for time in namespace.move_times:
        populate_fake_butler(namespace.fromrepo, namespace.torepo, time, namespace.window_days, verbose = True)


if __name__ == '__main__':
    main()


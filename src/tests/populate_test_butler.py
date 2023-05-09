import argparse
import astropy.time
from lsst.daf.butler import Butler, Timespan
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser(description='Transferring data \
        into fake_from butler, for use in testing')
    # at least one arg in dataId needed for 'where' clause.
    parser.add_argument(
        "-f", "--fromrepo", type=str,
        required=True, default='/repo/embargo',
        help="Butler Repository path from which data is transferred. Input str. Default = '/repo/embargo'")
    parser.add_argument(
        "-t", "--torepo", type=str,
        required=True, help="Repository to which data is transferred. Input str")
    parser.add_argument("-m", "--move_times", type=str, required=True,
                        help="List of times you want to move", nargs="+")
    parser.add_argument("-d", "--window_days", type=int, required=True, default=35,
                        help="Time window around each time list entry you want to move")
    return parser.parse_args()


def populate_fake_butler(from_repo, to_repo, time, window_days, verbose=False):
    """
    Moves files to our fake butler (fake_from) from '/repo/embargo'
    """
    butler = Butler(from_repo)
    registry = butler.registry
    dest = Butler(to_repo, writeable=True)
    datasetType = 'raw'
    collections = 'LATISS/raw/all'
    instrument = 'LATISS'
    dataId = {'instrument': instrument}
    # Define time window to be slightly larger
    # than the embargo period of 30 days
    window = astropy.time.TimeDelta(window_days, format='jd')
    time_astropy = astropy.time.Time(time)
    # second part is an open bracket, goes down to seconds
    timespan = Timespan(time_astropy - window, time_astropy + window)
    within_window = []
    for i, dt in enumerate(registry.queryDimensionRecords('exposure', dataId=dataId, datasets=datasetType,
                                                          collections=collections,
                                                          where="exposure.timespan OVERLAPS timespan",
                                                          bind={"timespan": timespan})):
        within_window.append(dt.id)
    if verbose:
        print(f'trying to move {len(within_window)} data refs')
    # Query the DataIds after embargo period
    datasetRefs = registry.queryDatasets(datasetType, dataId=dataId, collections=collections,
                                         where="exposure.id IN (exposure_ids)",
                                         bind={"exposure_ids": within_window})
    if verbose:
        print(f'beginning the move from {from_repo} to {to_repo}')
    out = dest.transfer_from(butler, source_refs=datasetRefs, transfer='copy',
                             skip_missing=True, register_dataset_types=True,
                             transfer_dimensions=True)
    if verbose:
        print(f'{np.shape(out)} files have been moved')


def main():
    namespace = parse_args()
    for time in namespace.move_times:
        print(f'moving this time {time}')
        populate_fake_butler(namespace.fromrepo,
                             namespace.torepo,
                             time,
                             namespace.window_days,
                             verbose=True)


if __name__ == '__main__':
    main()
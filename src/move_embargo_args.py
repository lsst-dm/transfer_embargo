import argparse
from lsst.daf.butler import Butler
import astropy

remove_collection = False # function to remove a collection from scratch butler
transfer = False

def parse_args(args):
    parser = argparse.ArgumentParser(description='Transferring data from embargo butler to another butler')

    # Define all the arguments
    # at least one arg in dataId needed for 'where' clause.
    parser.add_argument("-fromrepo", "--fromrepo", type=str, metavar='/repo/embargo', required=True, default='/repo/embargo', help="Butler Repository path from which data is transferred. Input str. Default = '/repo/embargo'")
    parser.add_argument("-torepo", "--torepo", type=str, metavar='/home/j/jarugula/scratch', required=True, help = "Repository to which data is transferred. Input str")
    parser.add_argument("-days", "--embargodays", type=int, metavar=30, required=True, help = "Embargo time period in days. Input int")
    parser.add_argument("-instrument", "--instrument", type=str, metavar='LATISS', required=True, help = "Instrument. Input str")
    parser.add_argument("-dtype", "--datasettype", type=str, metavar='raw', required=False, help = "Dataset type. Input str")
    parser.add_argument("-coll", "--collections", type=str, metavar='LATISS/raw/all', required=False, help = "Data Collections. Input str")
    parser.add_argument("-detector", "--detector", type=int, metavar=0, required=False, help = "Detector number. Input int")
    parser.add_argument("-band", "--band", type=str, metavar='g', required=False, help = "Band. Input str")
    parser.add_argument("-exposure", "--exposure", type=int, metavar=2022091400696,  required=False, help = "Exposure id. Input int")

    return parser.parse_args(args)

# Define embargo and scratch butler
butler = Butler(args.fromrepo)
registry = butler.registry

scratch_butler = Butler(args.torepo, writeable=True)
scratch_registry = scratch_butler.registry

datasetType = args.datasettype
collections = args.collections

# Dataset to move
# dataID must include
if not [x for x in (args.instrument, args.detector, args.band) if x is None]: 
    dataId = {'instrument': args.instrument, 'detector': args.detector, 'band':args.band}
elif not [x for x in (args.instrument, args.detector, args.band, args.exposure) if x is None]: 
    dataId = {'instrument': args.instrument, 'detector': args.detector, 'band':args.band, 'exposure':args.exposure}
else:
    dataId={'instrument': args.instrument}

# Define embargo period 
embargo_period = astropy.time.TimeDelta(args.embargodays, format='jd') # 30 julian days?
int_embargo_period = int(embargo_period.datetime.days)
now = astropy.time.Time.now()
int_now = int(now.datetime.strftime("%Y%m%d"))

# The Dimensions query 
# If now - observation_end_time_in_embargo > embargo period : move   Else: don't move
# Save data Ids of these observations into a list
after_embargo = []

for i, dt in enumerate(registry.queryDimensionRecords('exposure',dataId=dataId,datasets=datasetType,collections=collections,
                                                     where="now - exposure.day_obs > embargo_period",
                                                     bind={"now": int_now, "embargo_period": int_embargo_period})):
    end_time = dt.timespan.end
    if now - end_time > embargo_period:
        after_embargo.append(dt.id)

# Query the DataIds after embargo period
datasetRefs = registry.queryDatasets(datasetType, dataId=dataId, collections=collections,
                                         where="exposure.id IN (exposure_ids)",
                                         bind={"exposure_ids": after_embargo})

# Copy the Dataset after embargo period from embargo butler to scratch butler
if transfer:
    scratch_butler.transfer_from(butler, source_refs=datasetRefs, transfer='copy',skip_missing=True,register_dataset_types=True,transfer_dimensions=True)
    
# Remove collection from scratch butler   
if remove_collection:
    scratch_butler.pruneCollection(collections, purge=True, unstore=True)
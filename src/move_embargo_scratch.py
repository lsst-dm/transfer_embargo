# .py version of Sree's .ipynb file of the same name
from lsst.daf.butler import Butler
import astropy

butler = Butler('/repo/embargo')
registry = butler.registry

# was '/home/j/jargula/scratch'
scratch_butler = Butler('/sdf/home/r/rnevin/scratch', writeable=True)
scratch_registry = scratch_butler.registry

# The following will eventually be a list we can read in
datasetType = 'raw'
collections = 'LATISS/raw/all'
dataId = {'instrument': 'LATISS', 'detector': 0, 'band':'i'}

embargo_period = astropy.time.TimeDelta(30, format='jd') # 30 julian days?
now = astropy.time.Time.now()

print(embargo_period)
print(now)

#datasetRefs_after_embargo = []
after_embargo = []
for i, dt in enumerate(registry.queryDimensionRecords('exposure',dataId=dataId,datasets=datasetType,collections=collections)):
    end_time = dt.timespan.end
    #print(end_time)
    if now - end_time > embargo_period:
        after_embargo.append(dt.id)
        #datasetRefs_after_embargo.append(dt)

# Use IN list to transfer to the scratch butler
for i,exposure_id in enumerate(after_embargo[0:10]):
    datasetRefs = registry.queryDatasets(datasetType, dataId=dataId, collections=collections,
                                         where="exposure.id = exposure_id",
                                         bind={"exposure_id": exposure_id})
    scratch_butler.transfer_from(butler, source_refs=datasetRefs, transfer='copy',skip_missing=True,register_dataset_types=True,transfer_dimensions=True)
    
# Queries to check if datasets have transferred
transferred_datasetRefs = scratch_registry.queryDatasets(datasetType, dataId=dataId, collections=collections,
                                    where="exposure.id = exposure_id",
                                    # bind={"exposure_id": 2022110300001}
                                    bind={"exposure_id": 2022101201223})
for i, ref in enumerate(transferred_datasetRefs):
    print(ref.dataId.full)

transferred_datasetRefs_all = scratch_registry.queryDatasets(datasetType, dataId=dataId, collections=collections)
for i, ref in enumerate(transferred_datasetRefs_all):
     print(ref.dataId.full)

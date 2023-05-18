from lsst.daf.butler import Butler

butler = Butler('/repo/main')
registry = butler.registry

test_from_butler = Butler('../../tests/data/test_from', writeable=True)
test_from_butler_registry = test_from_butler.registry

datasetType = 'raw'
collections = 'LATISS/raw/all'
dataId = {'instrument': 'LATISS'}

exposure_ids = [2019111300059, 2019111300061, 2020011700002, 2020011700003, 2020011700004, 2020011700005, 2020011700006]

datasetRefs = registry.queryDatasets(datasetType, dataId=dataId, collections=collections,
                                     where="exposure.id IN (exposure_ids)",
                                     bind={"exposure_ids": exposure_ids}).expanded()


test_from_butler.transfer_from(butler, source_refs=datasetRefs, transfer='copy',
                               skip_missing=True, register_dataset_types=True,
                               transfer_dimensions=True)

if __name__ == "__main__":
    pass

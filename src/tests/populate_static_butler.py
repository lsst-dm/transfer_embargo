from lsst.daf.butler import Butler
import argparse

# def parse_args():
#     parser = argparse.ArgumentParser(
#         description="Transferring data from main to test_from butler"
#     )
#     parser.add_argument(
#         "fromrepo",
#         type=str,
#         help="Repository to which data is transferred. Input str",
#     )
#     return parser.parse_args()

if __name__ == "__main__":
    # namespace = parse_args()
    butler = Butler("/repo/main")
    registry = butler.registry
    #test_from_butler = Butler(namespace.fromrepo, writeable=True)
    test_from_butler = Butler("../../tests/data/test_from", writeable=True)
    test_from_butler_registry = test_from_butler.registry

    datasetType = "raw"
    collections = "LATISS/raw/all"
    dataId = {"instrument": "LATISS"}

    exposure_ids = [
        2019111300059,
        2019111300061,
        2020011700002,
        2020011700003,
        2020011700004,
        2020011700005,
        2020011700006,
    ]

    datasetRefs = registry.queryDatasets(
        datasetType,
        dataId=dataId,
        collections=collections,
        where="exposure.id IN (exposure_ids)",
        bind={"exposure_ids": exposure_ids},
    ).expanded()


    test_from_butler.transfer_from(
        butler,
        source_refs=datasetRefs,
        transfer="copy",
        skip_missing=True,
        register_dataset_types=True,
        transfer_dimensions=True,
    )

    test_datasetRefs = test_from_butler_registry.queryDatasets(datasetType=..., collections=...)
    print(
            "Butler URI in test_from:",
                [test_from_butler.getURI(datasetType,dataId=dt.dataId.full, collections=collections)
                for dt in test_datasetRefs],
        )


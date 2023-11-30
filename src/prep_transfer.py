from collections import Counter, defaultdict
from lsst.daf.butler import (
    DataCoordinate,
    DataId,
    DataIdValue,
    Dimension,
    DimensionElement,
    DimensionRecord,
    DimensionUniverse,
)
from lsst.daf.butler import Progress


def prep_for_ingest(target_registry, target_butler, 
                    source_registry, source_butler, 
                    source_refs, 
                    register_dataset_types = True, 
                    # register_collection = True,
                    transfer_dimensions=True):
    
    """
    this code is taken from: 
    https://github.com/lsst/daf_butler/blob/1e3d68c2a155215c755c62404ea5fdd1de110740/python/lsst/daf/butler/direct_butler.py
    L1920-L1949
    
    This code registers DatasetType and collection to the target_butler.
    Does not register instrument
    
    Run this code before ingest
    """
    
    progress = Progress("lsst.daf.butler.Butler.prep_for_ingest")
        
    source_dataset_types = set()
    grouped_refs = defaultdict(list)
    for ref in source_refs:
        grouped_refs[ref.datasetType, ref.run].append(ref)
        source_dataset_types.add(ref.datasetType)
        
    newly_registered_dataset_types = set() 
    for datasetType in source_dataset_types:
        if register_dataset_types:
            if target_registry.registerDatasetType(datasetType):
                newly_registered_dataset_types.add(datasetType)
        else:
            # If the dataset type is missing, let it fail immediately.
            target_dataset_type = target_registry.getDatasetType(datasetType.name)
            # print(target_dataset_type)
            if target_dataset_type != datasetType:
                raise ConflictingDefinitionError(
                    "Source butler dataset type differs from definition"
                    f" in target butler: {datasetType} !="
                    f" {target_dataset_type}"
                )
                
    if newly_registered_dataset_types:
        print( "Registered the following dataset types in the target Butler: ",
              ", ".join(d.name for d in newly_registered_dataset_types),
             )
    else:
        print("All required dataset types are known to the target Butler")
        
    """   
    # register collection
    # This is written only for run collection
    source_collection = set()
    for ref in source_refs:
        source_collection.add(ref.run)
        
    newly_registered_collection = set() 
    for collection in source_collection:
        if register_collection:
            collection_type = source_registry.getCollectionType(collection)
            if target_registry.registerCollection(collection, CollectionType(collection_type)):
                newly_registered_collection.add(collection)
                
        else:
            target_collection = target_registry.getCollectionSummary(collection)
            if target_collection != source_registry.getCollectionSummary(collection):
                raise ConflictingDefinitionError(
                    "Source butler collection differs from definition"
                    f" in target butler: {collection} !="
                    f" {target_collection}"
                )
                                
    if newly_registered_collection:
        print( "Registered the following collection in the target Butler: ",
              ", ".join(collection for collection in newly_registered_collection),
             )
    else:
        print("All required collections are known to the target Butler")
    """
    
        
    dimension_records: dict[DimensionElement, dict[DataCoordinate, DimensionRecord]] = defaultdict(dict)
    if transfer_dimensions:
        elements = frozenset(
                element
                for element in target_butler.dimensions.getStaticElements()
                if element.hasTable() and element.viewOf is None
            )
        dataIds = {ref.dataId for ref in source_refs}
        
        for dataId in dataIds:
            if not dataId.hasRecords():
                if registry := getattr(source_butler, "registry", None):
                    dataId = registry.expandDataId(dataId)
                else:
                    raise TypeError("Input butler needs to be a full butler to expand DataId.")
            # for element_name in dataId.dimensions.elements:
            #     record = dataId.records[element_name]
                for record in dataId.records.values():
                    if record is not None and record.definition in elements:
                        dimension_records[record.definition].setdefault(record.dataId, record)
                    
#         print('dimension records after', dimension_records)
        
#         print("Ensuring that dimension records exist for transferred datasets.")
#         for element, r in dimension_records.items():
#             records = [r[dataId] for dataId in r]
#             dest_butler._registry.insertDimensionData(element, *records, skip_existing=True)
        

    handled_collections: set[str] = set()
    
    if dimension_records:
        # print('dimension records after', dimension_records)
        print("Ensuring that dimension records exist for transferred datasets.")
        
        for element, r in dimension_records.items():
            records = [r[dataId] for dataId in r]
            # Assume that if the record is already present that we can
            # use it without having to check that the record metadata
            # is consistent.
            target_registry.insertDimensionData(element, *records, skip_existing=True)


    n_imported = 0

    for (datasetType, run), refs_to_import in progress.iter_item_chunks(
        grouped_refs.items(), desc="Importing to registry by run and dataset type"
    ):
        if run not in handled_collections:
            # May need to create output collection. If source butler
            # has a registry, ask for documentation string.
            run_doc = None
            if registry := getattr(source_butler, "registry", None):
                run_doc = registry.getCollectionDocumentation(run)
            registered = target_registry.registerRun(run, doc=run_doc)
            handled_collections.add(run)
            if registered:
                print("Creating output run %s", run)

        n_refs = len(refs_to_import)
        print( "Importing %d ref%s of dataset type %s into run %s" % (
            n_refs,
            "" if n_refs == 1 else "s",
            datasetType.name,
            run,))
        # Assume we are using UUIDs and the source refs will match
        # those imported.
        imported_refs = target_registry._importDatasets(refs_to_import)
        assert set(imported_refs) == set(refs_to_import)
        n_imported += len(imported_refs)
        
        # assert len(source_refs) == n_imported
        print("Imported %d datasets into destination butler" % (n_imported))
        
    return _

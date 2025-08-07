#!/usr/bin/env python
import argparse

from lsst.daf.butler import (
    Butler,
    CollectionType,
)


def parse_args():
    """Parses and returns command-line arguments
    for transferring data between Butler repositories.

    This function sets up an argument parser for transferring
    data from an embargo Butler repository to another Butler repository.
    It defines several arguments and their options, including source
    and destination repositories.

    Returns
    -------
    ns : argparse.Namespace
        An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Transferring data from embargo butler to another butler."
    )
    parser.add_argument(
        "--src-repo",
        type=str,
        required=True,
        help="Butler repository from which collection information is drawn.",
    )
    parser.add_argument(
        "--dest-repo",
        type=str,
        required=True,
        help=(
            "Butler repository in which collection chain will be defined"
            " (or re-defined if already existing)."
        ),
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help=(
            "Do not define the chain in the destination repo;"
            " only show what would be done."
        ),
    )
    parser.add_argument(
        "--collection",
        type=str,
        required=True,
        help=(
            "Specify a parent collection in the source repo for which"
            " you want to recreate the same chain in the destination repo."
            " Constituent determination is automatic."
            " Must be a CHAINED collection."
        ),
    )
    parser.add_argument(
        "--skip-recursive",
        action="store_true",
        help=(
            "Skip defining any recursive chains (child collections that are also chains)"
            " found within COLLECTION. Default is False."
            " In most cases it is best not to specify this option."
        ),
    )

    ns = parser.parse_args()
    return ns


def recreate_chain(source_butler, dest_butler, collection,
                   dry_run=False, skip_recursive=False):
    collinfo = source_butler.collections.get_info(collection)
    if collinfo.type != CollectionType.CHAINED:
        raise ValueError(f"Error: {collection} is not a CHAINED collection.")

    """
    define the chained collection in the dest repo by adding
    all children in the src_repo to be children in the dest_repo.
    note that this assumes all of the child collections have already
    been transferred. But first, we should deal with recursive chains
    (i.e. child collections of the collections of interest that are
    themselves chained collections), if any exist.
    """
    for ichild in collinfo.children:
        childinfo = source_butler.collections.get_info(ichild)
        if childinfo.type == CollectionType.CHAINED:
            if not skip_recursive:
                recreate_chain(source_butler,
                               dest_butler,
                               ichild,
                               dry_run=dry_run)
    # After checking for any recursive chains, do the definition
    # in the destination repo.
    if dry_run:
        print(f"This is a dry run. I would define the {collection} chain"
              f" as having the following children: {collinfo.children}.")
    else:
        dest_butler.collection_chains.redefine_chain(collection,
                                                     collinfo.children)


def main():
    args = parse_args()
    # Define embargo and destination butler
    source_butler = Butler(args.src_repo)
    dest_butler = Butler(args.dest_repo, writeable=True)
    recreate_chain(source_butler,
                   dest_butler,
                   args.collection,
                   args.dry_run,
                   args.skip_recursive)


if __name__ == "__main__":
    main()

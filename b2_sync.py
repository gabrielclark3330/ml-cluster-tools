import b2sdk.v2 as b2
import sys
import time
import argparse

def b2_sync(args):
    source = args.source #'b2://zyphra-datasets/processed_mistral/annealing_datasets'
    destination = args.destination #'/nfsdata/datasets/processed_mistral/annealing_datasets'

    print(f"Source: {source}")
    print(f"Destination: {destination}")
    print(f"dry_run={args.dry_run}, max_workers={args.max_workers}")
    
    info = b2.InMemoryAccountInfo()
    b2_api = b2.B2Api(info)

    application_key_id = ''
    application_key = ''
    b2_api.authorize_account("production", application_key_id, application_key)

    source = b2.parse_folder(source, b2_api)
    destination = b2.parse_folder(destination, b2_api)

    policies_manager = b2.ScanPoliciesManager(exclude_all_symlinks=True)

    synchronizer = b2.Synchronizer(
            max_workers=10,
            policies_manager=policies_manager,
            dry_run=args.dry_run,
            allow_empty_source=True,
        )

    no_progress = False

    with b2.SyncReport(sys.stdout, no_progress) as reporter:
        synchronizer.sync_folders(
            source_folder=source,
            dest_folder=destination,
            now_millis=int(round(time.time() * 1000)),
            reporter=reporter,
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    b2_sync(args)
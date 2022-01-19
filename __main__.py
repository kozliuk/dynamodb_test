from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

import boto3
from python_dynamodb_lock.python_dynamodb_lock import DynamoDBLockClient

dynamodb_resource = boto3.resource("dynamodb")

lock_client = DynamoDBLockClient(dynamodb_resource, table_name="test")


def client(idx, file_name):
    with lock_client.acquire_lock(
        partition_key=file_name, retry_timeout=timedelta(seconds=3600)
    ):
        print(f"{idx}: processing - {file_name}")
        import time

        time.sleep(10)
        print(f"{idx}: processed - {file_name}")
        return idx, file_name


def main():
    file_name = "example_file.csv"
    pool = ThreadPoolExecutor(max_workers=3)
    futures = []
    for idx in range(5):
        future = pool.submit(lambda idx=idx: client(idx, file_name))
        futures.append(future)
    for fut in as_completed(futures):
        idx, file_name = fut.result()
        print(f"{idx}: result - {file_name}")
    lock_client.close()


if __name__ == "__main__":
    main()

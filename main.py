from google.cloud import storage
import csv
import os
from pathlib import Path

def convert_csv(bucket_name, dest_bucket_name, subdir=""):
    # Create Storage client
    storage_client = storage.Client(project='my-project')
    
    # create reference to source bucket
    src_bucket = storage_client.bucket(bucket_name)

    # create destination bucket if it doesn't exist
    try:
        if storage_client.get_bucket(dest_bucket_name).exists():
            print("BUCKET EXISTS")
            dest_bucket = storage_client.bucket(dest_bucket_name)
    except:
        print("CREATING BUCKET {}".format(dest_bucket_name))
        dest_bucket = storage_client.create_bucket(dest_bucket_name, location='us')

    # get list of objects in source bucket
    blobs = storage_client.list_blobs(bucket_name, prefix=subdir)

    # Process all objects in source bucket.
    # 1. Download From GCS bucket
    # 2. Convert '|' delimiter to ',' delimiter
    # 3. Write converted file back to local filesystem
    # 4. Upload to GCS
    # 5. Remove both files from local filesystem
    for blob in blobs:
        if Path(blob.name).suffix == ".csv":
            process_file(blob.name, src_bucket, dest_bucket)
        else:
            print("SKIPPING {}".format(blob.name))
            

def process_file(src_file_name, src_bucket, dest_bucket):
    print("PROCESSING {}".format(src_file_name))

    local_src_file = "/tmp/{}".format(src_file_name)
    local_src_dir = "/tmp/{}".format('/'.join(src_file_name.split('/')[0:-1]))
    
    if not os.path.exists(local_src_dir):
        os.makedirs(local_src_dir)

    src_file = src_bucket.blob(src_file_name)
    src_file.download_to_filename(local_src_file)

    # Output
    local_dest_file = "/tmp/output/{}".format(src_file_name)
    local_dest_dir = "/tmp/output/{}".format('/'.join(src_file_name.split('/')[0:-1]))
    
    if not os.path.exists(local_dest_dir):
        os.makedirs(local_dest_dir)

    with open(local_src_file, "r") as file_pipe:
        with open(local_dest_file, 'w') as file_comma:
                csv.writer(file_comma, delimiter=',').writerows(csv.reader(file_pipe, delimiter='|'))
    
    dest_blob = dest_bucket.blob(src_file_name)
    dest_blob.upload_from_filename("/tmp/output/{}".format(src_file_name))

    print("UPLOADED {}".format(dest_blob.name))

    # Remove Temp files
    os.remove("/tmp/{}".format(src_file_name))
    os.remove("/tmp/output/{}".format(src_file_name))

    
def main():
    src_bucket_name = "my-bucket"
    dest_bucket_name = '{}-converted'.format(src_bucket_name)
    subdir = ""
    convert_csv(src_bucket_name, dest_bucket_name, subdir)
    
    print('Success!')

if __name__ == "__main__":
    main()
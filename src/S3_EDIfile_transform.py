from boto3.session import Session
from boto3 import client
from datetime import datetime
import os

print('started at {}'.format(str(datetime.now())))

ACCESS_KEY = 'aaa'
SECRET_KEY = 'bbb'

# for s3 bucket search
session = Session(aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY)

# for edi file downloading and uploading
s3 = client('s3', aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY)

s3_client = session.client('s3')
s3_session = session.resource('s3')

bucket_name = 'df-dev-data'
bucket = s3_session.Bucket(bucket_name)

file_counter = 0
wrong_files = {}
log_list = []

# using set for better search performance
log_set = set(line.strip() for line in open('log-archive-309-N10.txt'))

# loop on files
for file_object in list(bucket.objects.filter(Prefix='archive-309/')):
    # check if the file is already corrected
    if file_object.key in log_set:
        pass
    else:
        print(file_object.key)
        file_counter += 1
        if file_counter % 100 == 0:
            print(file_counter)
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_object.key)
        binary_body = obj.get('Body').read()
        body = binary_body.decode('utf-8')
        body = body.replace('~', '\n')
        wrong_lines = []

        # loop on body
        for i, line in enumerate(iter(body.splitlines())):

            lines = line.split('~')

            # loop on lines inside files
            for segment in lines:

                segment_as_list = segment.split('*')

                # check segment validity
                try:
                    if (segment_as_list[0] == 'N10' and len(segment_as_list[1]) > 15) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[2]) > 45) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[3]) > 48) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[4]) > 1) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[5]) > 30) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[6]) > 8) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[7]) > 1) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[8]) > 10) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[9]) > 30) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[10]) > 3) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[11]) > 3) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[12]) > 3) \
                    or (segment_as_list[0] == 'N10' and len(segment_as_list[13]) > 3):
                        wrong_lines.append(i + 1)  # add line numbers which contains wrong segment
                except IndexError:
                    pass

        splitted_objectpath = file_object.key.split('/')

        # work with wrong data
        if len(wrong_lines) > 0:
            wrong_files[file_object.key] = wrong_lines   # file names with wrong data and the list with line numbers
            s3.download_file(bucket_name, file_object.key, splitted_objectpath[2])  # file download

            # rewrite data (sub optimal)
            with open(splitted_objectpath[2], "r") as f:
                local_file = f.readlines()
            with open(splitted_objectpath[2], "w") as f:
                for data in local_file:
                    # need to split data in local file order to work on line level
                    splited_local = data.split('~')
                    line_cnt = 0

                    for line in splited_local:
                        line_cnt += 1
                        if line_cnt not in wrong_files[file_object.key]:  # exclude lines which are wrong
                            f.write(line + '~')

            # change file name to corrected for new object name
            current_file_name_splited = splitted_objectpath[2].split('.')
            new_file_name = current_file_name_splited[0] + '_CORRECTED.' + current_file_name_splited[1]
            new_object_path_name = 'shipper-309/' + splitted_objectpath[1] + '/' + new_file_name

            # upload file back to s3
            with open(splitted_objectpath[2], "rb") as f:
                s3.upload_fileobj(f, bucket_name, new_object_path_name)

            # log file names to exclude in future runs
            with open('log-archive-309-N10.txt', 'a+') as log:
                log.write(file_object.key + '\n' + new_object_path_name + '\n')

    # remove unused file
    os.remove(splitted_objectpath[2])

print('finished at {}'.format(str(datetime.now())))


import datetime
import pytz

import ffmpeg
import pandas as pd


"""
TODO:
-check that end_time > start_time
-this script is meant to be sitting on the webserver
    -try adding it to the current website
"""

class S3Session:
    """
        onject for handling s3 sessions
        (check, storage, retrieval)
    """
    def __init__(self, aws_bucket="cam-tester1"):
        config = self.read_config()

        self.test_bucket = config["AWS"]["bucket"]
        self.working_dir = config["PATHS"]["working_dir"]
        self.vids_path = config["PATHS"]["vids"]
        self.session = boto3.Session(
            aws_access_key_id=config["AWS"]["aws_access_key"],
            aws_secret_access_key=config["AWS"]["aws_secret_key"],
        )

        self.s3_resource = self.session.resource("s3")

        # test_file = self.create_random_file("tester.txt")

        # self.upload("cam-tester1", test_file)


    def download(self, bucket_name, file_name):
        """
        downloads file from bucket by filename
        :param bucket_name: string
        :param file_name: string
        :return: desired object
        """
        dwnld_path = self.working_dir+'aws_'+file_name
        download_file_name = f"{dwnld_path}"
        self.s3_resource.Object(bucket_name, file_name).download_file(
            download_file_name
        )

        return download_file_name


    def download_from_list(self, files_list, bucket_name):
        """
            given a list of filepaths, download and return list of downloaded filepaths
        """
        for vid in download_list:
            session.download(test_bucket, vid)
            print(f"downloaded {vid}")

        return download_list




if __name__== '__main__':

    test_bucket = "cam-tester1"
    meta_file_name = "metadata.csv"
    reference_time = datetime.datetime(
                2018, 1, 1, tzinfo=datetime.timezone.utc
            )#must be stored in an additional metadata file, probbaly a .json is best


    #download the metadata file
    session = S3Session()
    meta_file = session.download(test_bucket, meta_file_name)

    meta_data = pd.read_csv(meta_file)


    def get_epoch_times(start, end, reference):
        """
            convert datetime objects into epoch (seconds) using the
            reference time
        """

        start_epoch = (start-reference).total_seconds()
        end_epoch = (end-reference).total_seconds()

        return start_epoch, end_epoch

    #astimezone(utc)
    #datetime.timezone.utc
    utc=pytz.timezone("UTC")
    request_start_time = utc.localize(datetime.datetime.strptime('2019-10-06T17:14:22+00:00',
                        '%Y-%m-%dT%H:%M:%S+00:00'))
    request_end_time = utc.localize(datetime.datetime.strptime('2019-10-06T17:15:10+00:00',
                        '%Y-%m-%dT%H:%M:%S+00:00'))

    start_epoch, end_epoch = get_epoch_times(request_start_time, request_end_time, reference_time)


    #finding the vids that are within this time range
    ends_in = meta_data.loc[(meta_data['epoch_end'] > start_epoch) & (meta_data['epoch_end'] < end_epoch)]
    starts_in = meta_data.loc[(meta_data['epoch_start'] > start_epoch) & (meta_data['epoch_start'] < end_epoch)]

    selected_vids = pd.concat([starts_in, ends_in]).drop_duplicates()
    selected_vids.sort_values(['epoch_end'], inplace = True)

    ## download all the vids

    session.download_from_list(selected_vids['file_name'].to_list(), test_bucket)

    #concatenate the vids and save to file
    ffmpeg.concat(*download_list).output('testout_1.mkv').run()


from configparser import ConfigParser
import datetime
#import logging
import uuid
import os

import boto3
from botocore.exceptions import ClientError
import enzyme
import pandas as pd


class S3Session:

    def __init__(self):
        config = self.read_config()

        self.test_bucket = "cam-tester1"

        self.session = boto3.Session(
                aws_access_key_id=config["AWS"]["aws_access_key"],
                aws_secret_access_key=config["AWS"]["aws_secret_key"],
            )

        self.s3_resource = self.session.resource("s3")


        #test_file = self.create_random_file("tester.txt")

        #self.upload("cam-tester1", test_file)


    def upload(self, bucket_name, file_name, key=None):
        """
        takes already processed files and uploads them to
        :param bucket_name: string
        :param file_name: string
        :return: True if file was added to resource,
        otherwise False
        """
        try:
            f= (file_name if not key else key)
            self.s3_resource.Object(bucket_name, f).upload_file(Filename = file_name)
        except Exception as e:
            return False, e
        return True, None


    def download(self, bucket_name, file_name):
        """
        downloads file from bucket by filename
        :param bucket_name: string
        :param file_name: string
        :return: desired object
        """
        download_file_name = f'/home/alejandro/Scripts/Cam1/{"aws_"+file_name}'
        self.s3_resource.Object(bucket_name, file_name).download_file(
            download_file_name)

        return download_file_name

    def get_all_bucket_objects(self, bucket_name):
        """
        looks into bucket and returns list of all file names
        :param bucket_name: string
        :return: list of all object names in bucket
        """
        object_list = []
        bucket = self.s3_resource.Bucket(bucket_name)

        for obj in bucket.objects.all():
            object_list.append(obj.key)

        return object_list

file_name
    @staticmethod
    def create_random_file(file_name):
        rand_file_name = "".join([str(uuid.uuid4().hex[:4]),"_",file_name])
        with open(rand_file_name, "w") as f:
            f.write("this shit"*100)
        f.close()
        return rand_file_name


    @staticmethod
    def read_config():
        config = ConfigParser()
        config.read("settings.ini")
        return config



def bucket_exists(bucket_name):
    """Determine whether bucket_name exists and the user has permission to access it

    :param bucket_name: string
    :return: True if the referenced bucket_name exists, otherwise False
    """

    s3 = boto3.client('s3')
    try:
        response = s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.debug(e)
        return False
    return True


class VidManager(S3Session):

    def __init__(self):
        super(VidManager, self).__init__()
        self.cam1_path = "/home/alejandro/cam1/"
        self.reference_time = datetime.datetime(2018, 1, 1, tzinfo=datetime.timezone.utc)


        self.sweeper()

    def sweeper(self):
        """collect vids stored locally,
            make new metadata file,
             upload vids and metadata to s3
        """

        self.current_local_files = self.get_local_vids(self.cam1_path)
        self.aws_files = self.get_all_bucket_objects(self.test_bucket)
        new_metadata = self.make_metadata_file()

        for f in self.current_local_files:
            print(f.split("/")[-1])
            if (f.split("/")[-1] not in self.aws_files and
                f.split("/")[-1] in new_metadata["file_name"].to_list()):
                result, err = self.upload(self.test_bucket, f, key = f.split("/")[-1]))
                if result:
                    os.remove(f)
                else:
                    print(f"***ERROR: could not upload the following filr\n {f}")
                    print(f"error code: {err}")
        #self.aws_files = self.get_all_bucket_objects(self.test_bucket)

        breakpoint()



    def get_local_vids(self, cam_path):
        """return all file name remaining in the camera path

        """
        vid_files = []
        # r=root, d=directories, f = files
        for r, d, f in os.walk(cam_path):
            for file in f:
                if '.mkv' in file:
                    vid_files.append(os.path.join(r, file))

        return vid_files

    def make_metadata_file(self):
        """
        Extract meta data, store in running file
        video start time is stored in the file name
        while the video duraiton must be extracted using enzyme
        """
        vid_meta_list = []
        for vid in self.current_local_files:

            start_list = self.parse_file_name(vid)
            start_time = datetime.datetime(*start_list, tzinfo=datetime.timezone.utc)
            duration = self.get_vid_duraiton(vid)
            end_time = start_time + duration
            epoch_start = (start_time-self.reference_time).total_seconds()
            epoch_end = (end_time - self.reference_time).total_seconds()

            vid_meta_list.append((vid.split("/")[-1],
                start_time.isoformat(),
                epoch_start,
                end_time.isoformat(),
                epoch_end,
                duration.total_seconds()))

        # store metadata in dataframe, add to meta.csv, upload to aws
        local_metadata = pd.DataFrame(vid_meta_list, columns = ["file_name",
            "start",
            "epoch_start",
            "end",
            "epoch_end",
            "duration (s)"], index = None)

        local_metadata.to_csv("meta.csv", index = False)
        aws_metadata = pd.read_csv(
            self.download(self.test_bucket, "meta.csv"))

        new_metadata = pd.concat([local_metadata, aws_metadata], ignore_index = True)
        new_metadata.drop_duplicates(subset ="file_name", inplace = True)

        # filenames which may not be in the bucket
        new_uploads = local_metadata["file_name"][~local_metadata["file_name"].isin(
            aws_metadata["file_name"])].to_list()

        return new_metadata

    @staticmethod
    def parse_file_name(file_name):
        """
            extracts video begin time from file name
        """
        date_list = file_name.split("/")[-1].strip(".mkv").split("-")[1].split(":")
        date_list = [int(x) for x in date_list]

        return date_list

    @staticmethod
    def get_vid_duraiton(file_name):
        """
        use enzyme library to extract video duration object
        param: file_name
        """
        with open(file_name, "rb") as f:
            meta = enzyme.MKV(f)
            t_delta = meta.info.duration

        f.close()
        return t_delta

if __name__ == '__main__':
    cam1 = VidManager()
    bucket = S3Session()

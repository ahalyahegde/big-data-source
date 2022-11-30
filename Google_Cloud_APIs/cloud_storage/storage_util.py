import json
from google.cloud import storage


class StorageUtil:
    storage_client = storage.Client()

    @staticmethod
    def file_exists(bucket_name, file_name):
        """
        Checks if the provided file exists in the bucket

        :param str bucket_name: GCS Bucket name
        :param str file_name: Name of the file
        :return: status whether the file exists or not
        :rtype: bool

        """
        bucket = StorageUtil.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        status = blob.exists()
        return status

    @staticmethod
    def write_to_file(bucket_name, file_name, file_content, content_type="application/json", wrap_data=True):
        """
        Writes data to GCS file

        :param str bucket_name: name of the bucket to which the file needs to be added
        :param str file_name: name of the file to which the data needs to added
        :param str file_content: data that needs to be added into the file
        :param str content_type: content type that needs to used for the file while uploading,
            defaults to 'application/json'
        :param bool wrap_data: to check if we need to use json.dumps() on the file content,
            defaults to True
        """
        bucket = StorageUtil.storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = json.dumps(file_content) if wrap_data else file_content
        blob.upload_from_string(
            data=data,
            content_type=content_type
        )

    @staticmethod
    def delete_file(bucket_name, file_path):
        """
        Deletes file form GCS bucket

        :param str bucket_name: name of the bucket from which the file needs to be deleted
        :param str file_path: File path with in the bucket
        """
        bucket = StorageUtil.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.delete()

    @staticmethod
    def read_file_as_string(bucket, file_path, as_json=False):
        """
        reads data from GCS path

        :param str bucket: name of the bucket
        :param str file_path: File path with in the bucket
        :param bool as_json: To check if we need to convert the downloaded string to dict, defaults to True

        :return: return the data from the file
        :rtype: str
        """
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket)
        blob = bucket.blob(file_path)
        str_data = blob.download_as_string(client=None)
        data = json.loads(str_data) if as_json else str_data
        return data

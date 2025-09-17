"""
This module provides a boto3 s3 client factory `get_client()`, which returns an s3 client that has been augmented by
some additional functionality defined in the ``ClientWrap`` class, also present in this module.

ClientWrap adds a few wrapper methods that simplify simple list / delete / copy operations by (1) handling paging and
batching and (2) dealing only with keys instead of more detailed object metadata.

``get_client()`` also makes it easy to to specify a default bucket for the client, so that you don't need to specify the
bucket in each call.

To use, you just need to make sure that you've set up your credentials e.g. with environment variables or with
credentials file.  See http://boto3.readthedocs.io/en/latest/guide/configuration.html for details.

.. code-block:: python

    # Create a client
    >>> c = get_client(default_bucket='some-bucket')

    # Get a list of keys with supplied prefix
    >>> keys = list(c.list_objects_wrap(prefix='inbound/something'))
    >>> for key in keys:
    >>>     print(key)

    # Copy files (dry run just shows you what the plan is)
    >>> keys = ['inbound/facebooktest/facebook.order_api_beta_20161027T103418.tsv.gz']
    >>> c.copy_objects_wrap(keys, 'abc123/test', ('inbound/facebooktest', ''), dry_run=True)
    Copying 1 files
    {'CopySource': {'Bucket': None, 'Key': 'inbound/facebooktest/facebook.order_api_beta_20161027T103418.tsv.gz'},
    'Bucket': None, 'Key': 'abc123/test/facebook.order_api_beta_20161027T103418.tsv.gz'}

    # Delete objects
    >>> c.delete_objects_wrap(keys=keys)

    # Use regular boto3 client methods
    >>> c.list_objects_v2(MaxKeys=1)


Also, there is a class called :class:`WrappedStreamingBody` which can be used to
stream gzipped files from s3.

.. code-block:: python

    # Read the first few bytes from gzipped text file on s3
    >>> response = c.get_object(Bucket='some-bucket', Key='some-key')
    >>> sb = response['Body']
    >>> wsb = WrappedStreamingBody(sb, 1024)
    >>> gzf = GzipFile(fileobj=wsb, mode='rb')
    >>> first_n_bytes = gzf.read(3000)

Lastly there is a helper function join_key() which works like os.path.join(), but for s3 keys instead of local files.

*** Use at your own risk. ***

"""

import glob
import re
import sys
import threading
from gzip import GzipFile
from math import ceil
from os import path as p
from pathlib import Path

import boto3

from include.utils.data_structures import chunk_list


def get_client(*, default_bucket=None, profile_name=None, **kwargs):
    """
    Returns a boto3 s3 client object augmented with functionality defined in ClientWrap class.

    :rtype: ClientWrap | pyboto3.s3
    """

    def add_custom_class(base_classes, **kwargs):
        base_classes.insert(0, ClientWrap)

    def add_default_bucket(params, **kwargs):
        if "Bucket" not in params or params["Bucket"] is None:
            params["Bucket"] = default_bucket

    session = boto3.Session(profile_name=None, **kwargs)

    session.events.register("creating-client-class.s3", add_custom_class)

    client = session.client("s3")

    if default_bucket is not None:
        event_system = client.meta.events

        event_system.register("provide-client-params.s3.*", add_default_bucket)

    return client


def join_key(path, *paths):
    val = "/".join([path] + list(paths))
    val = re.sub(r"/+", "/", val)
    val = re.sub(r"^/", "", val)
    return val


def get_filename(key):
    """
    Given key, returns the filename; i.e. strips the prefix

    """
    if key.endswith('/'):
        raise ValueError("key should not end with '/'")
    if key.endswith(' '):
        raise ValueError("key should not end with whitespace")
    try:
        return key.rsplit('/', 1)[1]
    except IndexError:
        return key


class ProgressPercentageUpload(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(p.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()


class ClientWrap(object):
    def __init__(self, *args, **kwargs):
        super(ClientWrap, self).__init__(*args, **kwargs)

    def list_objects_wrap(self, *, prefix=None, bucket=None):
        """
        Returns list of keys matching prefix on bucket.

        """

        def _list_objects_wrap(self, prefix=None, bucket=None):
            payload = {"Bucket": bucket}
            if prefix is not None and prefix not in ("", "/"):
                payload["Prefix"] = prefix
            response = self.list_objects_v2(**payload)
            if "Contents" in response:
                for key in map(lambda x: x["Key"], response["Contents"]):
                    yield key

            while "NextContinuationToken" in response:
                response = self.list_objects_v2(
                    ContinuationToken=response["NextContinuationToken"], **payload
                )
                if "Contents" in response:
                    for key in map(lambda x: x["Key"], response["Contents"]):
                        yield key

        return list(_list_objects_wrap(self=self, prefix=prefix, bucket=bucket))

    def copy_objects_wrap(
        self, *, keys, new_prefix, replace=None, src_bucket=None, tgt_bucket=None, dry_run=False
    ):
        """
        :param keys: list of keys to copy
        :param new_prefix: prefix where objects will be moved to
        :param replace: tuple ('search', 'replace') to toss out some portion of the old prefix.
        :param src_bucket: bucket currently holding the objects
        :param tgt_bucket: destination bucket
        :param dry_run: if true, will print out the planned request but not send
        :type self: pyboto3.s3 | ClientWrap

        """
        if dry_run is True:
            print("This is a dry run -- no data movement.")
        else:
            print("Copying %s files" % len(keys))
        copy_configs = []
        for key in keys:
            replace = (join_key("", replace[0]), join_key("", replace[1]))
            new_key = key.replace(*replace) if replace else key
            new_key = join_key(new_prefix, new_key)
            copy_configs.append(
                {
                    "Bucket": tgt_bucket,
                    "CopySource": {"Bucket": src_bucket, "Key": key},
                    "Key": new_key,
                }
            )
        for copy_config in copy_configs:
            if dry_run:
                print(copy_config)
            else:
                self.copy_object(**copy_config)

    def delete_objects_wrap(self, *, keys, bucket=None):
        """
        Delete keys.

        :param keys: list  containing keys to delete
        :type keys: list[str]
        :param bucket: bucket name
        :type bucket: str|unicode
        :type self: pyboto3.s3|ClientWrap
        """
        files_to_delete = list(map(lambda key: {"Key": key}, keys))
        print("Deleting %s files" % len(files_to_delete))
        deleted = []
        if len(files_to_delete) > 0:
            batch_size = 500
            for i in range(0, int(ceil(float(len(files_to_delete)) / batch_size))):
                lowerbound = i * batch_size
                upperbound = (i + 1) * batch_size
                curr_batch = files_to_delete[lowerbound:upperbound]
                deleted += curr_batch
                print("Batch %s (%s files)" % ((i + 1), len(curr_batch)))
                for key in curr_batch:
                    print("Deleting %s" % key["Key"])
                delete_config = {"Objects": curr_batch}
                self.delete_objects(
                    Bucket=bucket,
                    Delete=delete_config,
                )
        print("Done. Deleted %s files" % len(deleted))

    def move_objects_wrap(
        self, *, keys, new_prefix, replace=None, src_bucket=None, tgt_bucket=None, dry_run=False
    ):
        if replace.__class__ != tuple or len(replace) != 2:
            raise Exception("replace param must be tuple of dimension 2")
        """
        Copy + delete
        """
        batch_num = 0
        for subset in chunk_list(keys, 100):
            batch_num += 1
            print(f"processing batch {batch_num}")
            self.copy_objects_wrap(
                keys=subset,
                new_prefix=new_prefix,
                replace=replace,
                src_bucket=src_bucket,
                tgt_bucket=tgt_bucket,
                dry_run=dry_run,
            )
            if dry_run is False:
                self.delete_objects_wrap(keys=subset, bucket=src_bucket)

    def upload_files_wrap(
        self, *, file_list=None, file_glob=None, target_prefix=None, bucket=None, dry_run=False
    ):
        """
        Takes a glob or list of files, and loops through, calling client.upload_file()

        :type self: pyboto3.s3|ClientWrap"""
        if file_list and file_glob:
            raise Exception("Must supply only one of file_list and file_glob; both supplied.")
        elif not file_list and not file_glob:
            raise Exception("Must supply file_list or file_glob")

        if file_list:
            files = file_list
        else:
            files = glob.glob(str(file_glob))

        for filename in files:
            key = join_key(target_prefix, Path(filename).name)
            if dry_run:
                print("dry run: %s" % key)
            else:
                self.upload_file(
                    Filename=Path(filename).as_posix(),
                    Bucket=bucket,
                    Key=key,
                    Callback=ProgressPercentageUpload(filename),
                )

    def upload_file_wrap(self, *, filename, target_prefix=None, bucket=None):
        """
        Takes a glob and loops through, calling client.upload_file()

        :type self: pyboto3.s3|ClientWrap"""
        file_path = Path(filename).resolve()
        key = join_key(target_prefix, file_path.name)
        print("uploading %s" % key)
        self.upload_file(
            Filename=file_path.as_posix(),
            Bucket=bucket,
            Key=key,
            Callback=ProgressPercentageUpload(file_path.as_posix()),
        )

    def mkdir_wrap(self, *, key, bucket=None):
        """
        Will create an empty file at key.

        :type self: pyboto3.s3|ClientWrap
        """

        self.put_object(Bucket=bucket, Key=join_key("", key + "/"))

    def gzip_file_handler(self, *, key, bucket=None, chunksize=1024):
        response = self.get_object(Bucket=bucket, Key=key)
        sb = response["Body"]
        wsb = WrappedStreamingBody(sb, chunksize)
        gzf = GzipFile(fileobj=wsb, mode="rb")
        return gzf


class WrappedStreamingBody(object):
    """
    Wrap boto3's StreamingBody object to provide enough fileobj functionality so that GzipFile is
    satisfied, which is useful for processing files from S3 in AWS Lambda which have been gzipped.
    Appropriated from https://gist.github.com/debedb.

    Examples:

    .. code-block:: python

        # Read first bytes from gzipped text file on s3:
        response = s3_client.get_object(Bucket='some-bucket', Key='some-key')
        sb = response['Body']
        wsb = WrappedStreamingBody(sb, 1024)
        gzf = GzipFile(fileobj=wsb, mode='rb')
        first_n_bytes = gzf.read(3000)
    """

    def __init__(self, sb, size):
        self.sb = sb
        self.pos = 0
        self.size = size

    def tell(self):
        # print("In tell(): %s" % self.pos)
        return self.pos

    def read(self, n=None):
        # print("In read(%s)" % n)
        retval = self.sb.read(n)
        self.pos += len(retval)
        return retval

    def seek(self, offset, whence=0):
        retval = self.pos
        if whence == 2:
            if offset == 0:
                retval = self.size
            else:
                raise Exception("Unsupported")
        else:
            if whence == 1:
                offset = self.pos + offset
                if offset > self.size:
                    retval = self.size
                else:
                    retval = offset
        # print("In seek(%s, %s): %s, size is %s" % (offset, whence, retval, self.size))
        self.pos = retval
        return retval

    def __getattr__(self, attr):
        # print("Calling %s" % attr)
        if attr == "tell":
            return self.tell
        elif attr == "seek":
            return self.seek
        elif attr == "read":
            return self.read
        else:
            return getattr(self.sb, attr)

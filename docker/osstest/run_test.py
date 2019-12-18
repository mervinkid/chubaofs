#!/bin/env python
# -*- coding: utf-8 -*-
import boto3
import requests
import sys
import json

def test_main():
    master = sys.argv[1]
    sess =
    test_put_object()
    test_get_object()
    test_delete_object()
    test_delete_objects()
    test_multipart_complete()
    test_multipart_abort()
    pass

def init_session(master):
    # fetch volume
    url = 'http://%s/client/vol' % master
    param = {"name": "ltptest"}
    header = {"Skip-Owner-Validatio": True}
    resp = requests.get(url, param, header=header)
    if resp.status_code != 200:
        exit(-1)
    resp_map = json.loads(resp.content)
    data=resp_map["data"]
    oss_secure=data["OSSSecure"]
    access_key=oss_secure["AccessKey"]
    secret_key=oss_secure["SecretKey"]

    print('Fetch AccessKey(%s) SecretKey(%s)' % (access_key, secret_key))

    boto3.Session(aws_access_key_id=access_key, )
    pass

def test_put_object():
    print 'test put object'
    pass

def test_get_object():
    print 'test get object'
    pass

def test_list_object_v1():
    print('Test ListObjectV1')
    print('API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html')
    pass

def test_delete_object():
    print('test delete object')
    pass

def test_delete_objects():
    print('test delete objects')
    pass

def test_multipart_complete():
    print 'test complete multipart upload'
    pass

def test_multipart_abort():
    print 'test abort multipart upload'
    pass

if __name__ == '__main__':
    test_main()
# Scality S3 Server

![S3 Server logo](res/Scality-S3-Server-Logo-Large.png)

[![CircleCI][badgepub]](https://circleci.com/gh/scality/S3)
[![Scality CI][badgepriv]](http://ci.ironmann.io/gh/scality/S3)
[![Docker Pulls][badgedocker]](https://hub.docker.com/r/scality/s3server/)
[![Docker Pulls][badgetwitter]](https://twitter.com/s3server)

## Learn more at [s3.scality.com](http://s3.scality.com)

## [May I offer you some lovely documentation?](http://s3-server.readthedocs.io/en/latest/)

## Docker

[Run your S3 server with Docker](https://hub.docker.com/r/scality/s3server/)

## Contributing

In order to contribute, please follow the
[Contributing Guidelines](
https://github.com/scality/Guidelines/blob/master/CONTRIBUTING.md).

## Installation

### Dependencies

Building and running the Scality S3 Server requires node.js 6.9.5 and npm v3
. Up-to-date versions can be found at
[Nodesource](https://github.com/nodesource/distributions).

### Clone source code

```shell
git clone https://github.com/scality/S3.git
```

### Install js dependencies

Go to the ./S3 folder,

```shell
npm install
```

## Run it with a file backend

```shell
npm start
```

This starts an S3 server on port 8000. Two additional ports 9990 and
9991 are also open locally for internal transfer of metadata and data,
respectively.

The default access key is accessKey1 with
a secret key of verySecretKey1.

By default the metadata files will be saved in the
localMetadata directory and the data files will be saved
in the localData directory within the ./S3 directory on your
machine.  These directories have been pre-created within the
repository.  If you would like to save the data or metadata in
different locations of your choice, you must specify them with absolute paths.
So, when starting the server:

```shell
mkdir -m 700 $(pwd)/myFavoriteDataPath
mkdir -m 700 $(pwd)/myFavoriteMetadataPath
export S3DATAPATH="$(pwd)/myFavoriteDataPath"
export S3METADATAPATH="$(pwd)/myFavoriteMetadataPath"
npm start
```

## Run it with multiple data backends

```shell
export S3DATA='multiple'
npm start
```

This starts an S3 server on port 8000.
The default access key is accessKey1 with
a secret key of verySecretKey1.

With multiple backends, you have the ability to
choose where each object will be saved by setting
the following header with a locationConstraint on
a PUT request:

```shell
'x-amz-meta-scal-location-constraint':'myLocationConstraint'
```

If no header is sent with a PUT object request, the
location constraint of the bucket will determine
where the data is saved. If the bucket has no location
constraint, the endpoint of the PUT request will be
used to determine location.

See the Configuration section below to learn how to set
location constraints.

## Run it with an in-memory backend

```shell
npm run mem_backend
```

This starts an S3 server on port 8000.
The default access key is accessKey1 with
a secret key of verySecretKey1.

[badgetwitter]: https://img.shields.io/twitter/follow/s3server.svg?style=social&label=Follow
[badgedocker]: https://img.shields.io/docker/pulls/scality/s3server.svg
[badgepub]: https://circleci.com/gh/scality/S3.svg?style=svg
[badgepriv]: http://ci.ironmann.io/gh/scality/S3.svg?style=svg&circle-token=1f105b7518b53853b5b7cf72302a3f75d8c598ae

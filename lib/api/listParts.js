const querystring = require('querystring');
const async = require('async');

const { errors, s3middleware } = require('arsenal');

const constants = require('../../constants');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const services = require('../services');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');
const escapeForXml = s3middleware.escapeForXml;
const { pushMetric } = require('../utapi/utilities');

const { config } = require('../../lib/Config');
const multipleBackendGateway = require('../data/multipleBackendGateway');

  /*
  Format of xml response:
  <?xml version='1.0' encoding='UTF-8'?>
  <ListPartsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
    <Bucket>example-bucket</Bucket>
    <Key>example-object</Key>
    <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1
    tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
    <Initiator>
        <ID>arn:aws:iam::111122223333:user/some-user-11116a31-
        17b5-4fb7-9df5-b288870f11xx</ID>
        <DisplayName>umat-user-11116a31-17b5-4fb7-9df5-
        b288870f11xx</DisplayName>
    </Initiator>
    <Owner>
      <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
      <DisplayName>someName</DisplayName>
    </Owner>
    <StorageClass>STANDARD</StorageClass>
    <PartNumberMarker>1</PartNumberMarker>
    <NextPartNumberMarker>3</NextPartNumberMarker>
    <MaxParts>2</MaxParts>
    <IsTruncated>true</IsTruncated>
    <Part>
      <PartNumber>2</PartNumber>
      <LastModified>2010-11-10T20:48:34.000Z</LastModified>
      <ETag>'7778aef83f66abc1fa1e8477f296d394'</ETag>
      <Size>10485760</Size>
    </Part>
    <Part>
      <PartNumber>3</PartNumber>
      <LastModified>2010-11-10T20:48:33.000Z</LastModified>
      <ETag>'aaaa18db4cc2f85cedef654fccc4a4x8'</ETag>
      <Size>10485760</Size>
    </Part>
  </ListPartsResult>
   */

function buildXML(xmlParams, xml, encodingFn) {
    xmlParams.forEach(param => {
        if (param.value !== undefined) {
            xml.push(`<${param.tag}>${encodingFn(param.value)}</${param.tag}>`);
        } else {
            if (param.tag !== 'NextPartNumberMarker' &&
                param.tag !== 'PartNumberMarker') {
                xml.push(`<${param.tag}/>`);
            }
        }
    });
}

/**
 * listParts - List parts of an open multipart upload
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function listParts(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'listParts' });

    const bucketName = request.bucketName;
    const objectKey = request.objectKey;
    const uploadId = request.query.uploadId;
    const encoding = request.query['encoding-type'];
    let maxParts = Number.parseInt(request.query['max-parts'], 10) ?
        Number.parseInt(request.query['max-parts'], 10) : 1000;
    if (maxParts < 0) {
        return callback(errors.InvalidArgument);
    }
    if (maxParts > constants.listingHardLimit) {
        maxParts = constants.listingHardLimit;
    }
    const partNumberMarker =
        Number.parseInt(request.query['part-number-marker'], 10) ?
        Number.parseInt(request.query['part-number-marker'], 10) : 0;
    const metadataValMPUparams = {
        authInfo,
        bucketName,
        objectKey,
        uploadId,
        requestType: 'listParts',
    };
    // For validating the request at the destinationBucket level
    // params are the same as validating at the MPU level
    // but the requestType is the more general 'objectPut'
    // (on the theory that if you are listing the parts of
    // an MPU being put you should have the right to put
    // the object as a prerequisite)
    const metadataValParams = Object.assign({}, metadataValMPUparams);
    metadataValParams.requestType = 'objectPut';

    let splitter = constants.splitter;

    async.waterfall([
        function checkDestBucketVal(next) {
            metadataValidateBucketAndObj(metadataValParams, log,
                (err, destinationBucket) => {
                    if (err) {
                        return next(err, destinationBucket, null);
                    }
                    if (destinationBucket.policies) {
                        // TODO: Check bucket policies to see if user is granted
                        // permission or forbidden permission to take
                        // given action.
                        // If permitted, add 'bucketPolicyGoAhead'
                        // attribute to params for validating at MPU level.
                        // This is GH Issue#76
                        metadataValMPUparams.requestType =
                            'bucketPolicyGoAhead';
                    }
                    return next(null, destinationBucket);
                });
        },
        function waterfall2(destBucket, next) {
            metadataValMPUparams.log = log;
            services.metadataValidateMultipart(metadataValMPUparams,
                (err, mpuBucket, mpuOverview) => {
                    if (err) {
                        return next(err, destBucket, null);
                    }
                    return next(null, destBucket, mpuBucket, mpuOverview);
                });
        },
        function waterfall3(destBucket, mpuBucket, mpuOverview, next) {
            // BACKWARD: Remove to remove the old splitter
            if (mpuBucket.getMdBucketModelVersion() < 2) {
                splitter = constants.oldSplitter;
            }

            if (config.backends.data === 'multiple') {
                // Reconstruct mpuOverviewKey to serve
                // as key to pull metadata originally stored when mpu initiated
                const mpuOverviewKey =
                    `overview${splitter}${objectKey}${splitter}${uploadId}`;

                return metadata.getObjectMD(mpuBucket.getName(), mpuOverviewKey,
                {}, log, (err, storedMetadata) => {
                    if (err) {
                        return next(err, destBucket);
                    }
                    const location =
                        storedMetadata.controllingLocationConstraint;
                    return multipleBackendGateway.listParts(objectKey, uploadId,
                    location, destBucket, partNumberMarker, maxParts, log,
                    (err, backendPartList) => {
                        if (err) {
                            return next(err, destBucket);
                        } else if (backendPartList) {
                            return next(null, destBucket, mpuBucket,
                                mpuOverview, backendPartList, splitter);
                        }
                        return next(null, destBucket, mpuBucket, mpuOverview,
                            null, splitter);
                    });
                });
            }
            return next(null, destBucket, mpuBucket, mpuOverview, null,
            splitter);
        },
        function waterfall4(destBucket, mpuBucket, mpuOverviewArray,
        backendPartList, splitter, next) {
            // if parts were returned from cloud backend, they were not
            // stored in Scality S3 metadata, so this step can be skipped
            if (backendPartList) {
                return next(null, destBucket, mpuBucket, backendPartList,
                    mpuOverviewArray);
            }
            const getPartsParams = {
                uploadId,
                mpuBucketName: mpuBucket.getName(),
                maxParts,
                partNumberMarker,
                log,
                splitter,
            };
            return services.getSomeMPUparts(getPartsParams,
            (err, storedParts) => {
                if (err) {
                    return next(err, destBucket, null);
                }
                return next(null, destBucket, mpuBucket, storedParts,
                    mpuOverviewArray);
            });
        }, function waterfall5(destBucket, mpuBucket, storedParts,
            mpuOverviewArray, next) {
            const encodingFn = encoding === 'url'
                ? querystring.escape : escapeForXml;
            const isTruncated = storedParts.IsTruncated;
            const splitterLen = splitter.length;
            const partListing = storedParts.Contents.map(item => {
                // key form:
                // - {uploadId}
                // - {splitter}
                // - {partNumber}
                let partNumber;
                if (item.key) {
                    const index = item.key.lastIndexOf(splitter);
                    partNumber =
                        parseInt(item.key.substring(index + splitterLen), 10);
                } else {
                    partNumber = item.partNumber;
                }
                return {
                    partNumber,
                    lastModified: item.value.LastModified,
                    ETag: item.value.ETag,
                    size: item.value.Size,
                };
            });
            const lastPartShown = partListing.length > 0 ?
                partListing[partListing.length - 1].partNumber : undefined;

            const xml = [];
            xml.push(
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<ListPartsResult xmlns="http://s3.amazonaws.com/doc/' +
                    '2006-03-01/">'
            );
            buildXML([
                { tag: 'Bucket', value: bucketName },
                { tag: 'Key', value: objectKey },
                { tag: 'UploadId', value: uploadId },
            ], xml, encodingFn);
            xml.push('<Initiator>');
            buildXML([
                { tag: 'ID', value: mpuOverviewArray[4] },
                { tag: 'DisplayName', value: mpuOverviewArray[5] },
            ], xml, encodingFn);
            xml.push('</Initiator>');
            xml.push('<Owner>');
            buildXML([
                { tag: 'ID', value: mpuOverviewArray[6] },
                { tag: 'DisplayName', value: mpuOverviewArray[7] },
            ], xml, encodingFn);
            xml.push('</Owner>');
            buildXML([
                { tag: 'StorageClass', value: mpuOverviewArray[8] },
                { tag: 'PartNumberMarker', value: partNumberMarker ||
                    undefined },
                // print only if it's truncated
                { tag: 'NextPartNumberMarker', value: isTruncated ?
                    parseInt(lastPartShown, 10) : undefined },
                { tag: 'MaxParts', value: maxParts },
                { tag: 'IsTruncated', value: isTruncated ? 'true' : 'false' },
            ], xml, encodingFn);

            partListing.forEach(part => {
                xml.push('<Part>');
                buildXML([
                    { tag: 'PartNumber', value: part.partNumber },
                    { tag: 'LastModified', value: part.lastModified },
                    { tag: 'ETag', value: `"${part.ETag}"` },
                    { tag: 'Size', value: part.size },
                ], xml, encodingFn);
                xml.push('</Part>');
            });
            xml.push('</ListPartsResult>');
            pushMetric('listMultipartUploadParts', log, {
                authInfo,
                bucket: bucketName,
            });
            next(null, destBucket, xml.join(''));
        },
    ], (err, destinationBucket, xml) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, destinationBucket);
        return callback(err, xml, corsHeaders);
    });
    return undefined;
}

module.exports = listParts;

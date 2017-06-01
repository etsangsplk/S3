const assert = require('assert');
const async = require('async');
const AWS = require('aws-sdk');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { config } = require('../../../../../lib/Config');
const { getRealAwsConfig } = require('../support/awsConfig');
const { createEncryptedBucketPromise } =
    require('../../lib/utility/createEncryptedBucket');

const awsLocation = 'aws-test';
const awsBucket = config.locationConstraints[awsLocation].details.bucketName;
const bucket = 'buckettestmultiplebackendobjectcopy';
const key = `somekey-${Date.now()}`;
const copyKey = `copyKey-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const locMetaHeader = 'scal-location-constraint';

let bucketUtil;
let s3;
let awsS3;
const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const copyParamBase = {
    Bucket: bucket,
    Key: copyKey,
    CopySource: `/${bucket}/${key}`,
};

function putSourceObj(location, cb) {
    const sourceParams = { Bucket: bucket, Key: key,
        Body: body,
        Metadata: {
            'scal-location-constraint': location,
        },
    };
    s3.putObject(sourceParams, (err, result) => {
        assert.equal(err, null, `Error putting source object: ${err}`);
        assert.strictEqual(result.ETag, `"${correctMD5}"`);
        cb();
    });
}

function assertGetObjects(sourceKey, sourceBucket, sourceLoc, destKey,
destBucket, destLoc, awsKey, callback) {
    const sourceGetParams = { Bucket: sourceBucket, Key: sourceKey };
    const destGetParams = { Bucket: destBucket, Key: destKey };
    const awsParams = { Bucket: awsBucket, Key: awsKey };

    async.series([
        cb => s3.getObject(sourceGetParams, cb),
        cb => s3.getObject(destGetParams, cb),
        cb => awsS3.getObject(awsParams, cb),
    ], (err, results) => {
        assert.equal(err, null, `Error in assertGetObjects: ${err}`);

        const [sourceRes, destRes, awsRes] = results;
        if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
            assert.strictEqual(sourceRes.ServerSideEncryption, 'AES256');
            assert.strictEqual(destRes.ServerSideEncryption, 'AES256');
            assert.strictEqual(awsRes.ServerSideEncryption, 'AES256');
        } else {
            assert.strictEqual(sourceRes.ETag, `"${correctMD5}"`);
            assert.strictEqual(destRes.ETag, `"${correctMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, destRes.Body);
            assert.strictEqual(awsRes.ETag, `"${correctMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, awsRes.Body);
        }
        assert.strictEqual(sourceRes.Metadata[locMetaHeader], sourceLoc);
        assert.strictEqual(destRes.Metadata[locMetaHeader], destLoc);
        callback();
    });
}

describeSkipIfNotMultiple('MultipleBackend object copy', function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const awsConfig = getRealAwsConfig(awsLocation);
            awsS3 = new AWS.S3(awsConfig);
            process.stdout.write('Creating bucket\n');
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketAsync = createEncryptedBucketPromise;
            }
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should copy an object from mem to AWS', done => {
            putSourceObj('mem', () => {
                const copyParams = Object.assign({
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': 'aws-test',
                    } }, copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, 'mem', copyKey, bucket,
                        'aws-test', copyKey, done);
                });
            });
        });

        it('should copy an object from AWS to mem', done => {
            putSourceObj('aws-test', () => {
                const copyParams = Object.assign({
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': 'mem',
                    } }, copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, 'aws-test', copyKey, bucket,
                        'mem', key, done);
                });
            });
        });

        it('should copy an object on AWS', done => {
            putSourceObj('aws-test', () => {
                const copyParams = Object.assign({ MetadataDirective: 'COPY' },
                    copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${correctMD5}"`);
                    assertGetObjects(key, bucket, 'aws-test', copyKey, bucket,
                        'aws-test', copyKey, done);
                });
            });
        });

        it('should return error if AWS source object has ' +
        'been deleted', done => {
            putSourceObj('aws-test', () => {
                awsS3.deleteObject({ Bucket: awsBucket, Key: key }, err => {
                    assert.equal(err, null, 'Error deleting object from AWS: ' +
                        `${err}`);
                    const copyParams = { Bucket: bucket, Key: copyKey,
                        CopySource: `/${bucket}/${key}`,
                        MetadataDirective: 'COPY',
                    };
                    s3.copyObject(copyParams, err => {
                        assert.strictEqual(err.code, 'InternalError');
                        done();
                    });
                });
            });
        });
    });
});

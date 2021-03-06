const url = require('url');
const async = require('async');

const { auth, errors } = require('arsenal');
const { responseJSONBody } = require('arsenal').s3routes.routesUtils;
const vault = require('../auth/vault');
const metadata = require('../metadata/wrapper');
const locationConstraintCheck = require(
    '../api/apiUtils/object/locationConstraintCheck');
const { dataStore } = require('../api/apiUtils/object/storeObject');
const prepareRequestContexts = require(
    '../api/apiUtils/authorization/prepareRequestContexts');
const { metadataValidateBucketAndObj } = require('../metadata/metadataUtils');

auth.setHandler(vault);

const NAMESPACE = 'default';
const CIPHER = null; // replication/lifecycle does not work on encrypted objects

function normalizeBackbeatRequest(req) {
    /* eslint-disable no-param-reassign */
    const parsedUrl = url.parse(req.url, true);
    req.path = parsedUrl.pathname;
    const pathArr = req.path.split('/');
    req.query = parsedUrl.query;
    req.bucketName = pathArr[3];
    req.objectKey = pathArr[4];
    req.resourceType = pathArr[5];
    /* eslint-enable no-param-reassign */
}

function _respond(response, payload, log, callback) {
    const body = typeof payload === 'object' ?
        JSON.stringify(payload) : payload;
    const httpHeaders = {
        'x-amz-id-2': log.getSerializedUids(),
        'x-amz-request-id': log.getSerializedUids(),
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
    };
    response.writeHead(200, httpHeaders);
    response.end(body, 'utf8', callback);
}

function _getRequestPayload(req, cb) {
    const payload = [];
    let payloadLen = 0;
    req.on('data', chunk => {
        payload.push(chunk);
        payloadLen += chunk.length;
    }).on('error', cb)
    .on('end', () => cb(null, Buffer.concat(payload, payloadLen).toString()));
}

/*
PUT /_/backbeat/<bucket name>/<object key>/metadata
PUT /_/backbeat/<bucket name>/<object key>/data
*/

function putData(request, response, bucketInfo, objMd, log, callback) {
    const canonicalID = request.headers['x-scal-canonicalId'];
    const contentMd5 = request.headers['content-md5'];
    const context = {
        bucketName: request.bucketName,
        owner: canonicalID,
        namespace: NAMESPACE,
        objectKey: request.objectKey,
    };
    const payloadLen = parseInt(request.headers['content-length'], 10);
    const backendInfoObj = locationConstraintCheck(
        request, null, bucketInfo, log);
    if (backendInfoObj.err) {
        log.error('error getting backendInfo', {
            error: backendInfoObj.err,
            method: 'routeBackbeat',
        });
        return callback(errors.InternalError);
    }
    const backendInfo = backendInfoObj.backendInfo;
    return dataStore(
        context, CIPHER, request, payloadLen, {},
        backendInfo, log, (err, retrievalInfo, md5) => {
            if (err) {
                return callback(err);
            }
            if (contentMd5 !== md5) {
                return callback(errors.BadDigest);
            }
            const { key, dataStoreName } = retrievalInfo;
            const dataRetrievalInfo = [{
                key,
                dataStoreName,
                size: payloadLen,
                start: 0,
            }];
            return _respond(response, dataRetrievalInfo, log, callback);
        });
}

function putMetadata(request, response, bucketInfo, objMd, log, callback) {
    return _getRequestPayload(request, (err, payload) => {
        if (err) {
            return callback(err);
        }
        let omVal;
        try {
            omVal = JSON.parse(payload);
        } catch (err) {
            // FIXME: add error type MalformedJSON
            return callback(errors.MalformedPOSTRequest);
        }
        // specify both 'versioning' and 'versionId' to create a "new"
        // version (updating master as well) but with specified
        // versionId
        const options = {
            versioning: true,
            versionId: omVal.versionId,
        };
        log.trace('putting object version', {
            objectKey: request.objectKey, omVal, options });
        return metadata.putObjectMD(
            request.bucketName, request.objectKey,
            omVal, options, log, (err, md) => {
                if (err) {
                    return callback(err);
                }
                return _respond(response, md, log, callback);
            });
    });
}


const backbeatRoutes = {
    PUT: { data: putData,
           metadata: putMetadata },
};

function routeBackbeat(clientIP, request, response, log) {
    log.debug('routing request', { method: 'routeBackbeat' });
    normalizeBackbeatRequest(request);
    const invalidRequest = (!request.bucketName ||
                            !request.objectKey ||
                            !request.resourceType);
    if (invalidRequest) {
        log.debug('invalid request', {
            method: request.method, bucketName: request.bucketName,
            objectKey: request.objectKey, resourceType: request.resourceType,
        });
        return responseJSONBody(errors.MethodNotAllowed, null, response, log);
    }
    const requestContexts = prepareRequestContexts('objectReplicate',
                                                   request);
    return async.waterfall([next => auth.server.doAuth(
        request, log, (err, userInfo) => {
            if (err) {
                log.debug('authentication error',
                          { error: err,
                            method: request.method,
                            bucketName: request.bucketName,
                            objectKey: request.objectKey });
            }
            return next(err, userInfo);
        }, 's3', requestContexts),
        (userInfo, next) => {
            const mdValParams = { bucketName: request.bucketName,
                                  objectKey: request.objectKey,
                                  authInfo: userInfo,
                                  requestType: 'ReplicateObject' };
            return metadataValidateBucketAndObj(mdValParams, log, next);
        },
        (bucketInfo, objMd, next) => {
            if (backbeatRoutes[request.method] === undefined ||
                backbeatRoutes[request.method][request.resourceType]
                === undefined) {
                log.debug('no such route', { method: request.method,
                                             bucketName: request.bucketName,
                                             objectKey: request.objectKey,
                                             resourceType:
                                             request.resourceType });
                return next(errors.MethodNotAllowed);
            }
            return backbeatRoutes[request.method][request.resourceType](
                request, response, bucketInfo, objMd, log, next);
        }],
        err => {
            if (err) {
                return responseJSONBody(err, null, response, log);
            }
            log.debug('backbeat route response sent successfully',
                      { method: request.method,
                        bucketName: request.bucketName,
                        objectKey: request.objectKey });
            return undefined;
        });
}


module.exports = routeBackbeat;

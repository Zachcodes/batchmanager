const { S3 } = require('aws-sdk');
const zlib = require('zlib');
const es = require('event-stream');

const s3 = new S3({
  httpOptions: {
    timeout: 900000, // 15 minutes
  },
});

class FileHandler {
    constructor(scriptArguments, esClient) {
        this.bucketName = scriptArguments[2];
        this.initialPrefix = scriptArguments[3];
        this.bucketMap = {};
        this.nestedBucketMapReferences = {};
        this.addUpdateToQueue = this.batchUpdateRecords();
        this.esClient = esClient;
    }

    batchUpdateRecords() {
        const meta = {
            recordCount: 0,
            recordsToProcess: []
        }
        return async (updateObject, promises) => {
            meta.recordsToProcess.push(updateObject);
            meta.recordCount++;
            if(meta.recordsToProcess.length >= 100) {
                promises.push(this.esClient.sendToElasticSearch(meta.recordsToProcess.splice(0)));
                console.log("total records processed");
                console.log(meta.recordCount);
            }
        }
    }

    grabAllS3FileKeys() {
        let completed = false;
        let NextContinuationToken = '';
        const params = {
            Bucket: this.bucketName,
            Prefix: this.initialPrefix,
            MaxKeys: 3
        };
    
        grabKeySet = () => {
            return new Promise((resolve, reject) => {
                if(NextContinuationToken) {
                    params['ContinuationToken'] = NextContinuationToken;
                }
                s3.listObjectsV2(params, (err, data) => {
                    if(err) {
                        reject(err);
                    }
                    console.log(data)
                    data.Contents.map( objectMetaData => {
                        this.assignToBucketMap(objectMetaData.Key, objectMetaData.LastModified);
                    });
                    if(data.NextContinuationToken) {
                        NextContinuationToken = data.NextContinuationToken;
                    }
                    else {
                        completed = true;
                    }
                    completed = true;
                    resolve();
                });
            });
        }
    
        return new Promise(async (resolve, reject) => {
            while(!completed) {
                await grabKeySet();
            }
            resolve();
        });
    }

    assignToBucketMap(s3Key, dateModified) {
        const objects = s3Key.split('/').filter( object => object).slice(1);
        const nestedRefKey = objects.slice(0, objects.length - 1).join('/');
        if(!objects.length) return;
        else if(nestedBucketMapReferences[nestedRefKey]) {
            nestedBucketMapReferences[nestedRefKey][objects[objects.length - 1]] = dateModified;
            return;
        }
        let referencedObject;
        objects.map( object => {
            if(!referencedObject) {
                referencedObject = bucketMap;
            }
            if(object.substr(-3) === '.gz') {
                referencedObject[object] = dateModified;
                nestedBucketMapReferences[nestedRefKey] = referencedObject;
                return;
            }
            else if(!Object.prototype.hasOwnProperty.call(referencedObject, object)) {
                referencedObject[object] = {};
            }
            referencedObject = referencedObject[object];
        });
    }

    processFilesSequentially(referencedBucket = bucketMap, assembledKey = '') {
        let objectKeys = Object.keys(referencedBucket);
        let sorted;
        if(objectKeys[0].substr(-3) !== '.gz') {
            sorted = objectKeys.sort((a, b) => {
                let numA = Number(a);
                let numB = Number(b);
                if(numA > numB) return 1;
                else if(numA < numB) return -1;
                else return 0;
            });
            sorted.map( key => this.processFilesSequentially(referencedBucket[key], `${assembledKey}/${key}`));
        }
        else {
            const sortedFiles = Object.entries(referencedBucket)
            .sort( (entry1, entry2) => {
                const dateTime1 = new Date(entry1[1]).getTime();
                const dateTime2 = new Date(entry2[1]).getTime();
                if(dateTime1 > dateTime2) return 1;
                if(dateTime1 < dateTime2) return -1;
                else return 0;
            })
            .map( entry => entry[0]);
            sortedFiles.map( key => this.processS3File(`${assembledKey}/${key}`));
        }
    }

    setAccurateMonth(month) {
        return month ? month >= 10 ? month : `0${month}` : month + 1;
    }

    processS3File(s3Key) {
        const updatePromises = [];
        // hit s3 and download file
        return new Promise((resolve, reject) => {
            s3.getObject({
                Bucket: bucketName,
                Key: `${process.argv[3].split('/')[0]}${s3Key}`,
            })
            .createReadStream()
            .on('error', (err) => {
                reject(err);
            })
            .pipe(zlib.createGunzip())
            .pipe(es.split())
            .pipe(es.parse())
            .pipe(
                es.through(function (event) {
                    const dateProcessed = new Date(event.timestamp);
                    const presenterUpdateMeta = {
                        esId: `${dateProcessed.getFullYear()}${this.setAccurateMonth(dateProcessed.getMonth())}${event.payload.id}`,
                        presenter_status_id: event.payload.presenter_status_id
                    }
                    this.addUpdateToQueue(presenterUpdateMeta, updatePromises);
                }),
            )
            .on('error', (err) => {
                reject(err);
            })
            .on('end', async () => {
                await Promise.allSettled(updatePromises);
                resolve();
            })
        }); 
    }
}

module.exports = FileHandler;
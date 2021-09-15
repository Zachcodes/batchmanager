const Batch = require('./Batch');
const { uid } = require('uid');

class ParallelManager extends Batch {
    constructor(options) {
        super(options);
        this.meta.queueIntervalId = this.initQueueLoop();
        this.meta.inFlightLimit = options.maxInFlight || 50;
        this.currentInFlight = 0;
        this.errorQueue = [];
    }

    // batchSet can be any of the following
    // function = callback function to invoke when ready
    // array = [funcToCall, [param1, param2]] invoked when ready
    // promise = already kicked off promise, will wait for result. ineligible for retry at this time
    async addToQueue(batchSet) {
        while((this.promiseQueue.length + this.errorQueue.length) >= this.meta.unresolvedBatchLimit) {
            await this.sleep(1000);
        }
        const queueItemUid = uid();
        const isPromise = batchSet instanceof Promise;
        const isArray = Array.isArray(batchSet);
        if(isPromise) {
            while(this.currentInFlight >= this.meta.inFlightLimit) {
                await this.sleep(100);
            }
            this.currentInFlight++;
            batchSet
            .then(((queueItemUid, ref) => {
                return result => this.handlePromiseSuccess.call(ref, result, queueItemUid, 'promise');
            })(queueItemUid, this))
            .catch(((queueItemUid, ref) => {
                return err => this.handlePromiseFailure.call(ref, err, queueItemUid, 'promise');
            })(queueItemUid, this))
        }
        this.promiseQueue.push({ 
            uid: queueItemUid, 
            batch: {
                cb: isArray ? batchSet[0] : isPromise ? null : batchSet,
                params: isArray ? batchSet[1] : null 
            },
            batchPromise: isPromise ? batchSet : null,
            attempt: 1
        });
    }

    processQueue() {
        // filter out all errors and resolves from first promise queue, then repeat for error queue
        this.promiseQueue = this.promiseQueue.filter( batchSet => typeof batchSet !== 'string');
        this.errorQueue = this.errorQueue.filter( batchSet => typeof batchSet !== 'string');
        if(this.currentInFlight < this.meta.inFlightLimit) {
            this.promiseQueue.forEach( batchSet => {
                if(!batchSet.batchPromise && this.currentInFlight < this.meta.inFlightLimit) {
                    this.currentInFlight++;
                    this.setHandlersForQueueItem(batchSet, 'promise');
                }
            })
        }
    }

    setHandlersForQueueItem(batchSet, queueToUse) {
        batchSet.batchPromise = batchSet.batch.cb.apply(null, batchSet.batch.params)
        .then(((queueItemUid, ref, queueToUse) => {
            return result => this.handlePromiseSuccess.call(ref, result, queueItemUid, queueToUse);
        })(batchSet.uid, this, queueToUse))
        .catch(((queueItemUid, ref, queueToUse) => {
            return err => this.handlePromiseFailure.call(ref, err, queueItemUid, queueToUse);
        })(batchSet.uid, this, queueToUse));
    }

    handlePromiseSuccess(result, queueItemUid, queue) {
        if(!this.meta.active) {
            return;
        }
        const queueToSearch = queue === 'promise' ? 'promiseQueue' : 'errorQueue';
        const index = this[queueToSearch].findIndex( queueItem => queueItem.uid === queueItemUid);
        this[queueToSearch].splice(index, 1, 'resolved');
        this.results.push(result);
        this.currentInFlight--;
    }

    handlePromiseFailure(err, queueItemUid, queue) {
        if(!this.meta.active) {
            return;
        }
        const queueToSearch = queue === 'promise' ? 'promiseQueue' : 'errorQueue';
        const index = this[queueToSearch].findIndex( queueItem => queueItem.uid === queueItemUid);
        const erroredBatchSet = this[queueToSearch].splice(index, 1, 'errored')[0];

        if(erroredBatchSet) {
            if(erroredBatchSet.attempt >= this.meta.maxRetries) {
                this.callReporters({ ...erroredBatchSet, err });
                this.currentInFlight--;
            }
            else {
                setTimeout(() => {
                    this.retryFailure(erroredBatchSet, erroredBatchSet.attempt + 1);
                }, this.sleep(1000, true)); 
            }
        }
    }

    retryFailure(erroredBatchSet, attempt = 2) {
        this.setHandlersForQueueItem(erroredBatchSet, 'error');
        this.errorQueue.push({...erroredBatchSet, batchPromise: null, attempt });
    }

    callReporters(erroredBatchSet) {
        this.reporters.map( reporterCb => reporterCb.apply(null, [{ batch: erroredBatchSet.batch, attempts: erroredBatchSet.attempt, err: erroredBatchSet.err }]));
    }
}

module.exports = ParallelManager;
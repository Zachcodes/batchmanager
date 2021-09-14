const { uid } = require('uid');
const batchManagers = {};
const modes = {
    sequential: 'sequential',
    parallel: 'parallel'
};
const queueStates = {
    ready: 'ready',
    waiting: 'waiting', 
    error: 'error'
};  

class Batch {
    // unresolvedBatchLimit = how many promises can be outstanding
    // mode = sequential or parallel, if sequential each batchset is handled sequentially before initiating next in set. parallel allows multiple promises to be active at the same time
    constructor(options) {
        this.promiseQueue = [];
        this.meta = {
            active: true,
            mode: options.mode, 
            unresolvedBatchLimit: options.unresolvedBatchLimit,
            maxRetries: options.maxRetries
        };
        this.managerInstanceName = this.setInstanceName();
        this.results = [];
        this.reporters = [];
    }

    cleanup() {
        clearInterval(this.meta.queueIntervalId);
        this.meta.queueIntervalId = null;
    }

    retire() {
        this.cleanup();
        this.promiseQueue = [];
        this.results = [];
        this.errorQueue = [];
        this.meta.active = false;
        const batchManagerKey = this.managerInstanceName.substr(0, this.managerInstanceName.length - 1);
        if(batchManagers.hasOwnProperty(batchManagerKey) && batchManagers[batchManagerKey][this.managerInstanceName]) {
            delete batchManagers[batchManagerKey][this.managerInstanceName];
        }
        if(batchManagers.hasOwnProperty(batchManagerKey) && !Object.keys(batchManagers[batchManagerKey]).length) {
            delete batchManagers[batchManagerKey];
        }
    }

    setInstanceName() {
        const prefix = this.constructor.name === 'BatchManager' ? '' : `${this.constructor.name}_`;
        const name = `${prefix}BatchManager`;
        const previousInstances = batchManagers[name] ? Object.keys(batchManagers[name]).length : 0;
        const instanceName = `${prefix}BatchManager${previousInstances ? previousInstances + 1 : 1}`;
        batchManagers[name] = batchManagers[name] || {};
        batchManagers[name][instanceName] = this.meta;
        return instanceName;
    }

    // reports is an array of callback functions to be run whenever an event failed to process
    // reporters will be passed the error object as well as the original data if possible
    registerReporters(reporters) {
        this.reporters = reporters;
    }

    initQueueLoop() {
        return setInterval(() => {
            this.processQueue();
        }, 500);
    }

    sleep(ms, stagger = false) {
        const timeout = stagger ? ms * Math.random() : ms;
        return new Promise((resolve) => {
            setTimeout(() => resolve(), timeout);
        });
    }
}

class SequentialManager extends Batch {
    constructor(options) {
        super(options);
        this.meta.queueState = queueStates.ready;
        this.meta.queueIntervalId = this.initQueueLoop();
        this.currentRetryCount = 0;
    }

    // batchSet should be a function passed like (async () => functionToCall(param1, param2))
    // can also be passed an array with params and callback [functionToInvoke, [param1, param2]]
    async addToQueue(batchSet) {
        while(this.promiseQueue.length >= this.meta.unresolvedBatchLimit) {
            await this.sleep(1000);
        }
        this.promiseQueue.push(batchSet);
    }

    processQueue() {
        if(this.meta.queueState === queueStates.ready) {
            this.handleNextInQueue();
        }
    }

    handleNextInQueue() {
        this.queueHandler = this.promiseQueue.splice(0, 1)[0];
        if(!this.queueHandler) return;
        this.meta.queueState = queueStates.waiting;
        const isArray = Array.isArray(this.queueHandler);
        const funcToCall = isArray ? this.queueHandler[0] : this.queueHandler;
        const paramsToPass = isArray ? this.queueHandler[1] : [];
        funcToCall.apply(null, paramsToPass)
        .then(this.handlePromiseResponse.bind(this))
        .catch(this.handlePromiseResponse.bind(this));
    }

    handlePromiseResponse(result) {
        if(result instanceof Error) {
            this.meta.queueState = queueStates.error;
            if(this.currentRetryCount >= this.meta.maxRetries) {
                this.callReporters(result);
                this.resetQueue();
            }
            else {
                this.currentRetryCount = this.currentRetryCount + 1;
                this.retryFailures();
            }
        }
        else {
            this.results.push(result);
            this.meta.queueState = queueStates.ready;
        }
    }

    retryFailures() {
        const isArray = Array.isArray(this.queueHandler);
        const funcToCall = isArray ? this.queueHandler[0] : this.queueHandler;
        const paramsToPass = isArray ? this.queueHandler[1] : [];
        funcToCall.apply(null, paramsToPass)
        .then(this.handlePromiseResponse.bind(this))
        .catch(this.handlePromiseResponse.bind(this));
    }

    resetQueue() {
        this.currentRetryCount = 0;
        this.meta.queueState = queueStates.ready;
        this.queueHandler = null;
    }

    // reporters expect params like so (result, passedParams[param1, param2])
    callReporters(result) {
        let functionArguments = [];
        if(Array.isArray(this.queueHandler) && Array.isArray(this.queueHandler[1])) {
            functionArguments.push([...this.queueHandler[1]]);
        }
        const params = [result, ...functionArguments];
        this.reporters.map( reporterCb => reporterCb.apply(null, params));
    }
}

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
            batchPromise: isPromise ? batchSet : null
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
        const queueToSearch = queue === 'promise' ? 'promiseQueue' : 'errorQueue';
        const index = this[queueToSearch].findIndex( queueItem => queueItem.uid === queueItemUid);
        this[queueToSearch].splice(index, 1, 'resolved');
        this.results.push(result);
        this.currentInFlight--;
    }

    handlePromiseFailure(err, queueItemUid, queue) {
        if(queue === 'promise') {
            const index = this.promiseQueue.findIndex( queueItem => queueItem.uid === queueItemUid);
            const erroredBatchSet = this.promiseQueue.splice(index, 1, 'errored');
            setTimeout(() => {
                this.retryFailure(erroredBatchSet);
            }, this.sleep(1000, true));
        }
        else {
            const index = this.errorQueue.findIndex( queueItem => queueItem.uid === queueItemUid);
            const erroredBatchSet = this.errorQueue.splice(index, 1, 'errored');
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
        this.reporters.map( reporterCb => reporterCb.apply(null, erroredBatchSet));
    }
}

// for parallel mode, maxInFlight can be passed to set the amount of active promises
const defaultOptions = {unresolvedBatchLimit: 100, mode: modes.sequential, maxRetries: 10}
const batchManager = (options = {}) => {
    const mergedOptions = {...defaultOptions, ...options};
    switch(mergedOptions.mode) {
        case modes.parallel:
            return new ParallelManager(mergedOptions);
        case modes.sequential:
        default:
            return new SequentialManager(mergedOptions);
    }
}

module.exports = batchManager;
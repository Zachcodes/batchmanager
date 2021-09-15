const Batch = require('./Batch');
const queueStates = {
    ready: 'ready',
    waiting: 'waiting', 
    error: 'error'
}; 

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
            await this.sleep(100);
        }
        this.promiseQueue.push(batchSet);
    }

    async processQueue() {
        // cancel timer, run through all active handlers
        if(this.promiseQueue.length) {
            this.cleanup();
            while(this.promiseQueue.length) {
                await this.handleNextInQueue();
            }
            this.meta.queueIntervalId = this.initQueueLoop();
        }
    }

    async handleNextInQueue() {
        return new Promise((resolve) => {
            this.resolveQueueItemCb = resolve;
            this.queueHandler = this.promiseQueue.splice(0, 1)[0];
            this.meta.queueState = queueStates.waiting;
            const isArray = Array.isArray(this.queueHandler);
            const funcToCall = isArray ? this.queueHandler[0] : this.queueHandler;
            const paramsToPass = isArray ? this.queueHandler[1] : [];
            funcToCall.apply(null, paramsToPass)
            .then(this.handlePromiseResponse.bind(this))
            .catch(this.handlePromiseResponse.bind(this));
        });
    }

    handlePromiseResponse(result) {
        if(!this.meta.active) {
            return;
        }
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
            this.queueHandler = null;
            this.resolveQueueItemCb();
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
        this.resolveQueueItemCb();
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

module.exports = SequentialManager;
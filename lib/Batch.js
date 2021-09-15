const batchManagers = {};

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

    async addOne(queueItem) {
        await this.addToQueue(queueItem);
    }

    async addMany(queueMap) {
        for(const queueItem of queueMap) {
            await this.addToQueue(queueItem);
        }
    }
}

module.exports = Batch;
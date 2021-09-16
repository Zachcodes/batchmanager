const batchManager = require('./lib/batchManager');
jest.setTimeout(10000);

const createQueueItem = resolveMessage => {
    return async () => {
        if(resolveMessage) {
            console.log(resolveMessage);
        }
        return resolveMessage;
    }
}

const sleep = ms => {
    return new Promise((resolve) => {
        setTimeout(() => resolve(), ms);
    });
}

const createDelayedItem = (resolvedMessage, ms = 1000) => {
    return async () => {
        await sleep(ms);
        if(resolvedMessage) {
            console.log(resolvedMessage)
        }
    }
}

const createRejectionItem = (errorMessage, ms = 1000) => {
    return async () => {
        await sleep(ms);
        throw new Error(errorMessage);
    }
}

const createQueueItemWithCallback = (cb, ms = 1000) => {
    return async (...params) => {
        await sleep(ms);
        cb(...params);
    }
}

describe(('BatchManager Base Class Functionality'), () => {
    let manager;
    afterEach(() => {
        manager.retire();
    });

    test('default options should use sequential manager', () => {
        manager = batchManager();
        expect(manager.meta.mode).toBe('sequential');
    });

    test('cleanup should clear queue interval', () => {
        manager = batchManager();
        expect(manager.meta.queueIntervalId).not.toBeNull();
        manager.cleanup();
        expect(manager.meta.queueIntervalId).toBeNull();
    });

    test('manager instance name should be defined', () => {
        manager = batchManager();
        manager.setInstanceName();
        expect(manager.managerInstanceName).not.toBeNull();
    });

    test('retire should deactivate and remove all utilized manager resources', () => {
        manager = batchManager();
        manager.retire();
        expect(manager.processQueue.length).toBe(0);
        expect(manager.errorQueue.length).toBe(0);
        expect(manager.meta.queueIntervalId).toBeNull();
    });

    test('sleep should resolve after the specified amount of ms', () => {
        manager = batchManager();
        const startTime = new Date().getTime();
        const ms = 1000;
        return manager.sleep(ms).then( _ => {
            const endTime = new Date().getTime();
            expect(endTime - startTime).toBeGreaterThanOrEqual(ms);
        });
    });

    test('sleep should stagger with random time interval', () => {
        manager = batchManager();
        const startTime = new Date().getTime();
        const ms = 1000;
        return manager.sleep(ms, true).then( _ => {
            const endTime = new Date().getTime();
            expect(endTime - startTime).toBeLessThanOrEqual(ms);
        });
    });

    test('calling retire while promises still in flight wont throw errors', async () => {
        const successMock = jest.fn();
        manager = batchManager();
        manager.addToQueue([createQueueItemWithCallback(successMock)]);
        while(manager.meta.queueState !== 'waiting') {
            await sleep(100);
        }
        manager.retire();
        expect(manager.meta.active).toBeFalsy();
        while(!successMock.mock.calls.length) {
            await sleep(500);
        }
        expect(manager.results.length).toBeFalsy();
    });

    test('addOne allows adding a single item to the queue', async () => {
        manager = batchManager();
        const jestPromises = [];
        for(let i = 0; i < 100; i++) {
            const fn = jest.fn();
            jestPromises.push(fn);
            await manager.addOne([createQueueItemWithCallback(fn, 0)]);
        }   
        expect(manager.promiseQueue.length).toBe(100);
        while(manager.results.length !== 100) {
            await sleep(100);
        }
        const calls = jestPromises.reduce((callCount, fn) => {
            return callCount + fn.mock.calls.length
        }, 0);
        expect(manager.promiseQueue.length).toBe(0);
        expect(calls).toBe(100);
    });

    test('addMany allows adding multiple items to the queue', async () => {
        manager = batchManager();
        const jestPromises = [];
        const items = [];
        for(let i = 0; i < 100; i++) {
            const fn = jest.fn();
            jestPromises.push(fn);
            items.push(createQueueItemWithCallback(fn, 0))
        }
        manager.addMany(items);
        while(manager.results.length !== 100) {
            await sleep(100);
        }
        const calls = jestPromises.reduce((callCount, fn) => {
            return callCount + fn.mock.calls.length
        }, 0);
        expect(manager.results.length).toBe(100);
        expect(calls).toBe(100);
    });
});

describe(('SequentialManager Functionality'), () => {
    let manager;
    afterEach(() => {
        manager.retire();
    });

    test('addToQueue should add an item to the process queue', () => {
        manager = batchManager();
        manager.addToQueue([createQueueItem('ran the callback')]);
        expect(manager.promiseQueue.length).toBe(1);
    });

    test('queue items are processed sequentially', () => {
        manager = batchManager();
        manager.addToQueue([createQueueItem('running callback 1')]);
        manager.addToQueue([createQueueItem('running callback 2')]);
        return new Promise((resolve) => {
            setTimeout(() => resolve(), 3000)
        })
    });

    test('queue items can be an array with function and params', async () => {
        const successMock = jest.fn();
        manager = batchManager();
        manager.addToQueue([createQueueItemWithCallback(successMock), ['foo', 'bar']]);
        while(!manager.results.length) {
            await sleep(500);
        }
        expect(successMock.mock.calls.length).toBe(1);
        expect(successMock.mock.calls[0]).toEqual(['foo', 'bar']);
    });

    test('queue status goes to waiting while promise in flight', async () => {
        manager = batchManager();
        manager.addToQueue([createDelayedItem('running delayed')]);
        await sleep(500);
        expect(manager.meta.queueState).toBe('waiting');
        while(manager.meta.queueState !== 'ready') {
            await sleep(500);
        }
        expect(manager.meta.queueState).toBe('ready');
    });

    test('results array is populated with all successful promise resolutions', async () => {
        manager = batchManager();
        manager.addToQueue([createQueueItem('result 1')]);
        manager.addToQueue([createQueueItem('result 2')]);
        manager.addToQueue([createQueueItem('result 3')]);
        while(manager.results.length !== 3) {
            await sleep(500);
        }
        expect(manager.meta.queueState).toBe('ready');
        expect(manager.results).toEqual(['result 1', 'result 2', 'result 3']);
    });

    test('queue status goes to error when current promise rejects', async () => {
        manager = batchManager({ maxRetries: 1 });
        manager.addToQueue([createRejectionItem('rejected')]);
        await sleep(500);
        expect(manager.meta.queueState).toBe('waiting');
        while(['waiting', 'ready'].indexOf(manager.meta.queueState) !== -1) {
            await sleep(500);
        }
        expect(manager.meta.queueState).toBe('error');
        while(manager.meta.queueState !== 'ready') {
            await sleep(500);
        }
        expect(manager.meta.queueState).toBe('ready');
    });

    test('retries failed batch sets until max limit is reached', async () => {
        manager = batchManager({ maxRetries: 3 });
        manager.addToQueue([createRejectionItem('rejected')]);
        await sleep(500);
        expect(manager.meta.queueState).toBe('waiting');
        while(['waiting', 'ready'].indexOf(manager.meta.queueState) !== -1) {
            await sleep(500);
        }
        expect(manager.meta.queueState).toBe('error');
        let lastRetryCount = 0;
        while(manager.meta.queueState !== 'ready') {
            lastRetryCount = manager.currentRetryCount;
            await sleep(500);
        }
        expect(lastRetryCount).toBeLessThanOrEqual(manager.meta.maxRetries);
    });

    test('reporters are called once retry limit is reached', async () => {
        const reporter1 = jest.fn();
        const reporter2 = jest.fn();
        manager = batchManager({ maxRetries: 1 });
        manager.registerReporters([reporter1, reporter2]);
        manager.addToQueue([createRejectionItem('rejected')]);
        await sleep(500);
        while(['waiting', 'ready'].indexOf(manager.meta.queueState) !== -1) {
            await sleep(500);
        }
        while(manager.meta.queueState !== 'ready') {
            await sleep(500);
        }
        expect(reporter1.mock.calls.length).toBe(1);
        expect(reporter2.mock.calls.length).toBe(1);
    });

    test('reporters are passed error as well as original function arguments', async () => {
        const reporter1 = jest.fn();
        manager = batchManager({ maxRetries: 1 });
        manager.registerReporters([reporter1]);
        manager.addToQueue([createRejectionItem('rejected'), [ 'foo', 'bar' ]]);
        await sleep(500);
        while(['waiting', 'ready'].indexOf(manager.meta.queueState) !== -1) {
            await sleep(500);
        }
        while(manager.meta.queueState !== 'ready') {
            await sleep(500);
        }
        expect(reporter1.mock.calls[0][0] instanceof Error).toBeTruthy();
        expect(reporter1.mock.calls[0][1]).toEqual(['foo', 'bar']);
    });

    test('next batch item is run after max failure count reached', async () => {
        const reporter1 = jest.fn();
        const successMock = jest.fn();
        manager = batchManager({ maxRetries: 1 });
        manager.registerReporters([reporter1]);
        manager.addToQueue([createRejectionItem('rejected')]);
        manager.addToQueue([createQueueItemWithCallback(successMock)]);
        await sleep(500);
        while(manager.meta.queueState !== 'error') {
            await sleep(500);
        }
        while(!reporter1.mock.calls.length || !successMock.mock.calls.length) {
            await sleep(500);
        }
        expect(successMock.mock.calls.length).toBe(1);
    });

    test('addToQueue waits until promise queue goes under unresolvedBatchLimit', async () => {
        manager = batchManager();
        manager.addToQueue(createDelayedItem());
        for(let i = 0; i < 100; i++) {
            manager.addToQueue(createQueueItem());
        }
        const startTime = new Date().getTime();
        await manager.addToQueue(createQueueItem());
        const endTime = new Date().getTime();
        expect(endTime - startTime).toBeGreaterThanOrEqual(500);
        while(manager.promiseQueue.length) {
            await sleep(500);
        }
        expect(manager.promiseQueue.length).toBe(0);
    });
});

describe(('ParallelManager Functionality'), () => {
    let manager;
    afterEach(() => {
        manager.retire();
    });
    
    test('passing parallel mode uses ParallelManager', () => {
        manager = batchManager({ mode: 'parallel' });
        expect(manager.meta.mode).toBe('parallel');
    });

    test('multiple promises can be in flight at the same time', async () => {
        const jestFns = [];
        manager = batchManager({ mode: 'parallel' });
        const startTime = new Date().getTime();
        for(let i = 0; i < 50; i++) {
            const func = jest.fn();
            jestFns.push(func);
            manager.addToQueue(createQueueItemWithCallback(func));
        }
        expect(manager.promiseQueue.length).toBeLessThanOrEqual(50);
        while(manager.promiseQueue.length > 0) {
            await sleep(500);
        }
        const endTime = new Date().getTime();
        const calls = jestFns.reduce( (callCount, mock) => {
            return callCount + mock.mock.calls.length;
        }, 0);
        expect(calls).toBe(50);
        expect(manager.promiseQueue.length).toBe(0);
        expect(manager.results.length).toBe(50);
        // sequential would be at least 1000 ms per item, parallel should execute in less
        expect(endTime - startTime).toBeLessThan(50 * 1000);
    });

    test('addToQueue accepts initiated promises as argument and resolves them', async () => {
        const promiseCb = jest.fn();
        manager = batchManager({ mode: 'parallel' });
        manager.addToQueue(createQueueItemWithCallback(promiseCb)());
        while(manager.currentInFlight > 0) {
            await sleep(500);
        }
        expect(manager.currentInFlight).toBe(0);
        expect(promiseCb.mock.calls.length).toBe(1);
    });

    test('addToQueue accepts cb and calls it', async () => {
        const promiseCb = jest.fn();
        manager = batchManager({ mode: 'parallel' });
        manager.addToQueue(createQueueItemWithCallback(promiseCb));
        while(!promiseCb.mock.calls.length) {
            await sleep(500);
        }
        expect(manager.currentInFlight).toBe(0);
        expect(promiseCb.mock.calls.length).toBe(1);
    });

    test('addToQueue accepts array with callback at index 0 and calls it', async () => {
        const promiseCb = jest.fn();
        manager = batchManager({ mode: 'parallel' });
        manager.addToQueue([createQueueItemWithCallback(promiseCb)]);
        while(!promiseCb.mock.calls.length) {
            await sleep(500);
        }
        expect(manager.currentInFlight).toBe(0);
        expect(promiseCb.mock.calls.length).toBe(1);
    });

    test('addToQueue accepts array with callback at index 0 and calls it with arguments at index 1', async () => {
        const promiseCb = jest.fn();
        manager = batchManager({ mode: 'parallel' });
        manager.addToQueue([createQueueItemWithCallback(promiseCb), ['foo', 'bar']]);
        while(!promiseCb.mock.calls.length) {
            await sleep(500);
        }
        expect(manager.currentInFlight).toBe(0);
        expect(promiseCb.mock.calls.length).toBe(1);
        expect(promiseCb.mock.calls[0]).toEqual(['foo', 'bar']);
    });

    test('inflight promises cant go above set limit', async () => {
        const jestFns = [];
        manager = batchManager({ mode: 'parallel' });
        for(let i = 0; i < 100; i++) {
            const func = jest.fn();
            jestFns.push(func);
            manager.addToQueue(createQueueItemWithCallback(func, 1000 * Math.random()));
        }
        expect(manager.promiseQueue.length).toBe(100);
        await sleep(1200);
        while(manager.currentInFlight > 0) {
            await sleep(100);
            expect(manager.currentInFlight).toBeLessThanOrEqual(50);
        }
        const calls = jestFns.reduce( (callCount, mock) => {
            return callCount + mock.mock.calls.length;
        }, 0);
        expect(calls).toBe(100);
    });

    test('initiated promises passed to queue cause wait over inflight limit', async () => {
        const promiseCb = jest.fn();
        const jestFns = [];
        manager = batchManager({ mode: 'parallel' });
        for(let i = 0; i < 51; i++) {
            const func = jest.fn();
            jestFns.push(func);
            manager.addToQueue(createQueueItemWithCallback(func, 1500));
        }
        while(manager.currentInFlight !== 50) {
            await sleep(100);
        }
        expect(manager.currentInFlight).toBe(50);
        const startTime = new Date().getTime();
        await manager.addToQueue(createQueueItemWithCallback(promiseCb)());
        const endTime = new Date().getTime();
        expect(endTime - startTime).toBeGreaterThanOrEqual(1000);
        while(manager.promiseQueue.length > 0) {
            await sleep(100);
        }
        expect(promiseCb.mock.calls.length).toBe(1);
    });

    test('rejected promises are moved to the failure queue', async () => {
        const reporter = jest.fn();
        manager = batchManager({ mode: 'parallel', maxRetries: 3 });
        manager.registerReporters([reporter])
        manager.addToQueue([createRejectionItem()]);
        while(!manager.currentInFlight) {
            await sleep(200);
        }
        expect(manager.currentInFlight).toBe(1);
        expect(manager.promiseQueue.length).toBe(1);
        while(!manager.errorQueue.length || manager.promiseQueue.length) {
            await sleep(200);
        }
        expect(manager.errorQueue.length).toBe(1);
        expect(manager.promiseQueue.length).toBe(0);
        while(!reporter.mock.calls.length) {
            await sleep(250);
        }
        expect(reporter.mock.calls.length).toBe(1);
    });

    test('reporters are called with batch and error details', async () => {
        const reporter = jest.fn();
        manager = batchManager({ mode: 'parallel', maxRetries: 1 });
        manager.registerReporters([reporter])
        manager.addToQueue([createRejectionItem()]);
        while(!reporter.mock.calls.length) {
            await sleep(250);
        }
        expect(reporter.mock.calls.length).toBe(1);
        expect(Object.prototype.hasOwnProperty.call(reporter.mock.calls[0][0], 'batch')).toBeTruthy();
        expect(Object.prototype.hasOwnProperty.call(reporter.mock.calls[0][0], 'attempts')).toBeTruthy();
        expect(Object.prototype.hasOwnProperty.call(reporter.mock.calls[0][0], 'err')).toBeTruthy();
    });
});
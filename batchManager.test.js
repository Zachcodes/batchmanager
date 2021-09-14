const batchManager = require('./lib/BatchManager');

const createQueueItem = resolveMessage => {
    return async () => {
        console.log(resolveMessage)
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
        console.log(resolvedMessage)
    }
}

const createRejectionItem = (errorMessage, ms = 1000) => {
    return async () => {
        await sleep(ms);
        throw new Error(errorMessage);
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
});

describe.only(('SequentialManager Functionality'), () => {
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

    test.only('reporters are passed error as well as original function arguments', async () => {
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
});
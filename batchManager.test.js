const batchManager = require('./lib/BatchManager');

const createQueueItem = resolveMessage => {
    return async () => {
        console.log(resolveMessage)
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
    })
});
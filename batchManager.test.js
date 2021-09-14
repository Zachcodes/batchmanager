const batchManager = require('./lib/BatchManager');

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
    })
});
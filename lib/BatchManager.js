const modes = {
    sequential: 'sequential',
    parallel: 'parallel'
};

// for parallel mode, maxInFlight can be passed to set the amount of active promises
const defaultOptions = {unresolvedBatchLimit: 100, mode: modes.sequential, maxRetries: 10}
const batchManager = (options = {}) => {
    const mergedOptions = {...defaultOptions, ...options};
    const ManagerToUse = require(`./${mergedOptions.mode[0].toUpperCase() + mergedOptions.mode.substr(1, mergedOptions.mode.length)}Manager`);
    return new ManagerToUse(mergedOptions);
}

module.exports = batchManager;
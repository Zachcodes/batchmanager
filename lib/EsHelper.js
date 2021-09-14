class EsHelper {
    constructor() {
        this.client = 'test';
    }

    sendToElasticSearch(updates) {
        return new Promise((resolve) => {
            setTimeout(() => {
                resolve();
            }, 3000);
        })
    }
}

module.exports = EsHelper;
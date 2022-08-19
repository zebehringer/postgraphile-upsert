"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createPool = void 0;
const pg_1 = require("pg");
async function createPool(config) {
    const client = new pg_1.Pool({
        user: config.username,
        host: "localhost",
        ...config,
    });
    return client;
}
exports.createPool = createPool;
//# sourceMappingURL=client.js.map
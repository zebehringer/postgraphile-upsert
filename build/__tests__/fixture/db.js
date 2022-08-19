"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.container = exports.purgeContainer = exports.imageExists = void 0;
const freeport_1 = require("./freeport");
const dockerode_1 = __importDefault(require("dockerode"));
const execa_1 = __importDefault(require("execa"));
const DB_IMAGE = "postgres:12-alpine";
const containers = new Set();
const docker = new dockerode_1.default({ socketPath: "/var/run/docker.sock" });
async function imageExists(imageName) {
    try {
        await execa_1.default("docker", ["image", "inspect", imageName]);
        return true;
    }
    catch (err) {
        // @TODO this is fragile, but dockerode is being a PIA
        return false;
    }
}
exports.imageExists = imageExists;
const isStatusCodeError = (x) => !!x && Number.isFinite(x.statusCode);
async function purgeContainer(container) {
    try {
        await container.kill();
    }
    finally {
        await containers.delete(container);
        try {
            await container.remove({ force: true });
        }
        catch (err) {
            const statusCode = isStatusCodeError(err) ? err.statusCode : 0;
            // if 404, we probably used the --rm flag on container launch. it's all good.
            if (!(statusCode === 404 || statusCode === 409)) {
                throw err; // eslint-disable-line
            }
        }
    }
}
exports.purgeContainer = purgeContainer;
exports.container = {
    async setup(ctx) {
        const port = await freeport_1.freeport();
        if (!(await imageExists(DB_IMAGE))) {
            await execa_1.default("docker", ["pull", DB_IMAGE]);
        }
        const container = await docker.createContainer({
            Image: DB_IMAGE,
            ExposedPorts: {
                "5432/tcp": {},
            },
            Env: ["POSTGRES_PASSWORD=postgres"],
            HostConfig: {
                AutoRemove: true,
                PortBindings: { "5432/tcp": [{ HostPort: port.toString() }] },
            },
            Cmd: ["-c", "log_statement=all"],
        });
        await container.start();
        containers.add(container);
        ctx.dbContainer = container;
        const dbConfig = {
            port,
            username: "postgres",
            password: "postgres",
            database: "postgres",
        };
        ctx.dbConfig = dbConfig;
    },
    async teardown(ctx) {
        const container = ctx.dbContainer;
        if (container)
            await purgeContainer(container);
    },
};
//# sourceMappingURL=db.js.map
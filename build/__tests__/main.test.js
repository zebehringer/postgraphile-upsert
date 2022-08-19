"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const db_1 = require("./fixture/db");
const client_1 = require("./fixture/client");
const http_1 = require("http");
const freeport_1 = require("./fixture/freeport");
const postgraphile_upsert_1 = require("../postgraphile-upsert");
const postgraphile_1 = require("postgraphile");
const ava_1 = __importDefault(require("ava"));
const nanographql = require("nanographql");
const Bluebird = require("bluebird");
const node_fetch_1 = __importDefault(require("node-fetch"));
const test = ava_1.default;
test.beforeEach(async (t) => {
    await db_1.container.setup(t.context);
    await Bluebird.delay(5000);
    t.context.client = await client_1.createPool(t.context.dbConfig);
    t.context.client.on("error", () => null);
    await t.context.client.query(`
    create table bikes (
      id serial,
      weight real,
      make varchar,
      model varchar,
      serial_number varchar,
      primary key (id),
      constraint serial_weight_unique unique (serial_number, weight)
    )
  `);
    await t.context.client.query(`
    create table roles (
      id serial primary key,
      project_name varchar,
      title varchar,
      name varchar,
      rank integer,
      updated timestamptz,
      unique (project_name, title)
    )
  `);
    await t.context.client.query(`COMMENT ON COLUMN roles.rank IS E'@omit updateOnConflict'`);
    await t.context.client.query(`
      create table no_primary_keys(
        name text
      )
  `);
});
test.afterEach(async (t) => {
    t.context.client.on("error", () => null);
    db_1.container.teardown(t.context).catch(console.error);
    await t.context.middleware.release();
    await new Promise((res) => t.context.server.close(res));
});
const initializePostgraphile = async (t, options = {}) => {
    const middleware = postgraphile_1.postgraphile(t.context.client, "public", {
        graphiql: true,
        appendPlugins: [postgraphile_upsert_1.PgMutationUpsertPlugin],
        exportGqlSchemaPath: "./postgraphile.graphql",
        graphileBuildOptions: {
            ...options,
        },
    });
    t.context.middleware = middleware;
    const serverPort = await freeport_1.freeport();
    t.context.serverPort = serverPort;
    t.context.server = http_1.createServer(middleware).listen(serverPort);
};
const execGqlOp = (t, query) => node_fetch_1.default(`http://localhost:${t.context.serverPort}/graphql`, {
    body: query(),
    headers: {
        "Content-Type": "application/json",
    },
    method: "POST",
}).then(async (res) => {
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`op failed: ${res.statusText}\n\n${text}`);
    }
    const json = await res.json();
    if (json.errors)
        throw new Error(JSON.stringify(json.errors));
    return json;
});
const fetchType = async (t, name) => {
    const queryString = `
    {
      __type(name: "${name}") {
        name
        kind
      }
    }
    `;
    const query = nanographql(queryString);
    return execGqlOp(t, query);
};
const fetchMutationTypes = async (t) => {
    const query = nanographql `
    query {
      __type(name: "Mutation") {
        name
        fields {
          name
          args { 
            name
          }
        }
      }
    }
  `;
    return execGqlOp(t, query);
};
const fetchAllBikes = async (t) => {
    const query = nanographql `
    query {
      allBikes {
        edges {
          node {
            id
            make
            model
          }
        }
      }
    }
  `;
    return execGqlOp(t, query);
};
const fetchAllRoles = async (t) => {
    const query = nanographql `
  query {
    allRoles(orderBy: RANK_ASC) {
      edges {
        node {
          id
          projectName
          title
          name
          rank
          updated
        }
      }
    }
  }`;
    return execGqlOp(t, query);
};
const fetchAllCars = async (t) => {
    const query = nanographql `
  query {
    allCars {
      edges {
        node {
          make
          model
          trim
          active
        }
      }
    }
  }`;
    return execGqlOp(t, query);
};
const create = async (t, extraProperties = {}) => {
    const defaultRecordFields = {
        make: '"kona"',
        model: '"kula deluxe"',
        weight: 0.0,
    };
    const bikeQueryStr = Object.entries(Object.entries(extraProperties).reduce((record, [k, v]) => {
        return {
            ...record,
            [k]: v,
        };
    }, defaultRecordFields))
        .map(([property, value]) => `${property}: ${value}`)
        .join("\n");
    const mutation = `mutation {
    upsertBike(input: {
      bike: {
        ${bikeQueryStr}
      }
    }) {
      clientMutationId
    }
  }`;
    return execGqlOp(t, nanographql(mutation));
};
test("ignores tables without primary keys", async (t) => {
    await initializePostgraphile(t);
    await create(t);
    const res = await fetchMutationTypes(t);
    const upsertMutations = new Set(res.data.__type.fields
        .map(({ name }) => name)
        .filter((name) => name.startsWith("upsert")));
    t.assert(upsertMutations.size === 2);
    t.assert(upsertMutations.has("upsertBike"));
    t.assert(upsertMutations.has("upsertRole"));
});
test("does not create update/nothing specifications when enableQueryDefinedConflictResolutionTuning is false (default)", async (t) => {
    await initializePostgraphile(t /* { enableQueryDefinedConflictResolutionTuning: false } */); // defaults to false
    await create(t, {});
    t.like(await fetchType(t, "UpsertBikeOnConflict"), {
        data: { __type: null },
    });
    const upsertBikeMutationArgs = new Set((await fetchMutationTypes(t)).data.__type.fields
        .find(({ name }) => name.startsWith("upsertBike"))
        .args.map(({ name }) => name));
    t.deepEqual(upsertBikeMutationArgs, new Set(["where", "input"]));
});
test("creates update/nothing specifications when enableQueryDefinedConflictResolutionTuning is true", async (t) => {
    await initializePostgraphile(t, {
        enableQueryDefinedConflictResolutionTuning: true,
    });
    await create(t, {});
    t.like(await fetchType(t, "UpsertBikeOnConflict"), {
        data: { __type: { name: "UpsertBikeOnConflict", kind: "INPUT_OBJECT" } },
    });
    const upsertBikeMutationArgs = new Set((await fetchMutationTypes(t)).data.__type.fields
        .find(({ name }) => name.startsWith("upsertBike"))
        .args.map(({ name }) => name));
    t.deepEqual(upsertBikeMutationArgs, new Set(["where", "input", "onConflict"]));
});
test("upsert crud - match primary key constraint", async (t) => {
    await initializePostgraphile(t);
    await create(t); // test upsert without where clause
    const res = await fetchAllBikes(t);
    t.is(res.data.allBikes.edges.length, 1);
    t.is(res.data.allBikes.edges[0].node.make, "kona");
});
test("upsert crud - match unique constraint", async (t) => {
    await initializePostgraphile(t);
    await create(t, { serialNumber: '"123"' }); // test upsert without where clause
    const res = await fetchAllBikes(t);
    t.is(res.data.allBikes.edges.length, 1);
    t.is(res.data.allBikes.edges[0].node.make, "kona");
});
test("upsert crud - update on unique constraint", async (t) => {
    await initializePostgraphile(t);
    await create(t, { weight: 20, serialNumber: '"123"' }); // test upsert without where clause
    await create(t, {
        model: '"updated_model"',
        weight: 20,
        serialNumber: '"123"',
    }); // test upsert without where clause
    const res = await fetchAllBikes(t);
    t.is(res.data.allBikes.edges.length, 1);
    t.is(res.data.allBikes.edges[0].node.model, "updated_model");
});
test("ensure valid values are included (i.e. 0.0 for numerics)", async (t) => {
    await initializePostgraphile(t);
    await create(t, { serialNumber: '"123"' });
    const query = nanographql(`
    mutation {
      upsertBike(where: {
        weight: 0.0,
        serialNumber: "123"
      },
      input: {
        bike: {
          model: "kula deluxe v2"
          weight: 0.0,
          serialNumber: "123"
        }
      }) {
        clientMutationId
      }
    }
  `);
    await execGqlOp(t, query);
    const res = await fetchAllBikes(t);
    t.is(res.data.allBikes.edges.length, 1);
    t.is(res.data.allBikes.edges[0].node.model, "kula deluxe v2");
});
test("Includes where clause values if ommitted from input", async (t) => {
    await initializePostgraphile(t);
    await create(t, { serialNumber: '"123"' });
    // Hit unique key with weight/serialNumber, but omit from input entry
    const query = nanographql(`
    mutation {
      upsertBike(where: {
        weight: 0.0,
        serialNumber: "123"
      },
      input: {
        bike: {
          model: "kula deluxe v2"
        }
      }) {
        clientMutationId
      }
    }
  `);
    await execGqlOp(t, query);
    const res = await fetchAllBikes(t);
    t.is(res.data.allBikes.edges.length, 1);
    t.is(res.data.allBikes.edges[0].node.model, "kula deluxe v2");
});
test("throws an error if input values differ from where clause values", async (t) => {
    await initializePostgraphile(t);
    try {
        await create(t, { serialNumber: '"123"' });
        const query = nanographql(`
      mutation {
        upsertBike(where: {
          weight: 0.0,
          serialNumber: "123"
        },
        input: {
          bike: {
            model: "kula deluxe v2"
            weight: 0.0,
            serialNumber: "1234"
          }
        }) {
          clientMutationId
        }
      }
    `);
        await execGqlOp(t, query);
        t.fail("Mutation should fail if values differ");
    }
    catch (e) {
        t.truthy((e instanceof Error ? e.message : `${e}`).includes("Value passed in the input for serialNumber does not match the where clause value."));
    }
});
test("upsert where clause", async (t) => {
    await initializePostgraphile(t, {
        enableQueryDefinedConflictResolutionTuning: true,
    });
    const upsertDirector = async ({ projectName = "sales", title = "director", name = "jerry", rank = 1, }) => {
        const query = nanographql(`
      mutation {
        upsertRole(where: {
          projectName: "sales",
          title: "director"
        },
        input: {
          role: {
            projectName: "${projectName}",
            title: "${title}",
            name: "${name}",
            rank: ${rank}
          }
        }) {
          clientMutationId
        }
      }
    `);
        return execGqlOp(t, query);
    };
    {
        // add director
        await upsertDirector({ name: "jerry" });
        const res = await fetchAllRoles(t);
        t.is(res.data.allRoles.edges.length, 1);
        t.like(res.data.allRoles.edges[0], {
            node: {
                projectName: "sales",
                title: "director",
                name: "jerry",
            },
        });
    }
    {
        // update director
        await upsertDirector({ name: "frank", rank: 2 });
        const res = await fetchAllRoles(t);
        t.like(res.data.allRoles.edges[0], {
            node: {
                projectName: "sales",
                title: "director",
                name: "frank",
                rank: 1,
            },
        });
        // assert only one record
        t.is(res.data.allRoles.edges.length, 1);
    }
});
test("upsert where clause omit onConflictUpdate", async (t) => {
    await initializePostgraphile(t, {
        enableQueryDefinedConflictResolutionTuning: true,
    });
    const upsertDirector = async ({ projectName = "sales", title = "director", name = "jerry", rank = 1, }) => {
        const query = nanographql(`
      mutation {
        upsertRole(onConflict: {doUpdate: {name: ignore, updated: current_timestamp}}, where: {
          projectName: "sales",
          title: "director"
        },
        input: {
          role: {
            projectName: "${projectName}",
            title: "${title}",
            name: "${name}",
            rank: ${rank}
          }
        }) {
          clientMutationId
        }
      }
    `);
        return execGqlOp(t, query);
    };
    {
        t.like(await upsertDirector({ name: "jerry" }), await upsertDirector({ name: "frank", rank: 2 }), "omit onConflictUpdate should yield no upsert effect");
    }
});
test("upsert where clause on conflict do nothing", async (t) => {
    await initializePostgraphile(t, {
        enableQueryDefinedConflictResolutionTuning: true,
    });
    const upsertDirector = async ({ projectName = "sales", title = "director", name = "jerry", rank = 1, }) => {
        const query = nanographql(`
      mutation {
        upsertRole(onConflict: {doNothing: true}, where: {
          projectName: "sales",
          title: "director"
        },
        input: {
          role: {
            projectName: "${projectName}",
            title: "${title}",
            name: "${name}",
            rank: ${rank}
          }
        }) {
          clientMutationId
        }
      }
    `);
        return execGqlOp(t, query);
    };
    {
        t.like(await upsertDirector({ name: "jerry" }), await upsertDirector({ name: "frank", rank: 2 }), "onConflict: {doNothing: true} should yield no upsert effect");
    }
});
test("upsert handling of nullable defaulted columns", async (t) => {
    await t.context.client.query(`
      create table car(
        id serial primary key,
        make text not null,
        model text not null,
        trim text not null default 'standard',
        active boolean,
        unique (make, model, trim)
      )
  `);
    await initializePostgraphile(t);
    const upsertCar = async ({ trim, active = false, } = {}) => {
        const query = nanographql(`
      mutation {
        upsertCar(
        input: {
          car: {
            make: "Honda",
            model: "Civic",
            ${trim ? `trim: "${trim}"` : ""}
            active: ${active}
          }
        }) {
          clientMutationId
        }
      }
    `);
        return execGqlOp(t, query);
    };
    {
        await upsertCar();
        await upsertCar({ active: true });
        let res = await fetchAllCars(t);
        t.is(res.data.allCars.edges.length, 1);
        t.like(res.data.allCars.edges[0], {
            node: {
                active: true,
                make: "Honda",
                model: "Civic",
                trim: "standard",
            },
        });
        await upsertCar({ trim: "non-standard" });
        res = await fetchAllCars(t);
        t.is(res.data.allCars.edges.length, 2);
    }
});
//# sourceMappingURL=main.test.js.map
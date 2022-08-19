import Docker from "dockerode";
export interface DbContextDbConfig {
    port: number;
    username: "postgres";
    password: "postgres";
    database: "postgres";
}
export interface DbContext {
    dbContainer: Docker.Container;
    dbConfig: DbContextDbConfig;
}
export declare function imageExists(imageName: string): Promise<boolean>;
export declare function purgeContainer(container: Docker.Container): Promise<void>;
export declare const container: {
    setup(ctx: DbContext): Promise<void>;
    teardown(ctx: DbContext): Promise<void>;
};
//# sourceMappingURL=db.d.ts.map
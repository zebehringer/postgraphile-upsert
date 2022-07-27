import { Plugin } from "graphile-build"; // eslint-disable-line no-unused-vars

export const PgMutationUpsertPlugin: Plugin = (builder) => {
  builder.hook("GraphQLObjectType:fields", (fields, build, context) => {
    const {
      extend,
      gql2pg,
      graphql: {
        GraphQLObjectType,
        GraphQLInputObjectType,
        GraphQLNonNull,
        GraphQLString,
        GraphQLBoolean,
      },
      inflection,
      newWithHooks,
      parseResolveInfo,
      pgGetGqlInputTypeByTypeIdAndModifier,
      pgGetGqlTypeByTypeIdAndModifier,
      pgIntrospectionResultsByKind,
      pgQueryFromResolveData: queryFromResolveData,
      pgSql: sql,
      pgViaTemporaryTable: viaTemporaryTable,
      pgField,
      pgOmit: omit,
    } = build;
    const {
      scope: { isRootMutation },
      fieldWithHooks,
    } = context;
    if (!isRootMutation) return fields;
    return extend(
      fields,
      pgIntrospectionResultsByKind.class
        .filter((table: any) => !!table.namespace)
        .filter((table: any) => !!table.primaryKeyConstraint)
        .filter((table: any) => !omit(table, "upsert"))
        .filter((table: any) => table.isSelectable)
        .filter((table: any) => table.isInsertable)
        .filter((table: any) => table.isUpdatable)
        .reduce((memo: any, table: any) => {
          const Table = pgGetGqlTypeByTypeIdAndModifier(table.type.id, null);
          if (!Table) return memo;
          const TableInput = pgGetGqlInputTypeByTypeIdAndModifier(
            table.type.id,
            null
          );
          if (!TableInput) return memo;
          const tableTypeName = inflection.tableType(table);
          const uniqueConstraints = pgIntrospectionResultsByKind.constraint
            .filter((con) => con.classId === table.id)
            .filter((con) => con.type === "u" || con.type === "p");
          const attributes = pgIntrospectionResultsByKind.attribute
            .filter((attr) => attr.classId === table.id)
            .sort((a, b) => a.num - b.num);

          /**
           * The WhereType needs to be a combo of TableCondition
           * But with the constraints of a uniqueConstraint
           * So find the query generator for an allTable query
           * But filter by the uniqueConstraints above
           *
           * See also:
           * PgRowByUniqueConstraint
           * PgConnectionArgCondition
           * PgAllRows
           */

          // For each unique constraint we gather all of the fields into an
          // InputType _technically_ we probably want to have **each**
          // uniqueConstraint create it's own type and then union these, but
          // YOLO
          const fields = uniqueConstraints.reduce((acc, constraint) => {
            const keys = constraint.keyAttributeNums.map((num) =>
              attributes.find((attr) => attr.num === num)
            );
            if (keys.some((key) => omit(key, "read"))) {
              return;
            }
            if (!keys.every((_) => _)) {
              throw new Error(
                "Consistency error: could not find an attribute!"
              );
            }
            //
            keys.forEach((key) => {
              const fieldName = inflection.camelCase(key.name);
              const InputType = pgGetGqlInputTypeByTypeIdAndModifier(
                key.typeId,
                key.typeModifier
              );
              if (!InputType) {
                throw new Error(
                  `Could not find input type for key '${key.name}' on type '${tableTypeName}'`
                );
              }
              acc[fieldName] = { type: InputType };
            });
            return acc;
          }, {});
          // Unique Where conditions
          const WhereType = newWithHooks(
            GraphQLInputObjectType,
            {
              name: `Upsert${tableTypeName}Where`,
              description: `Where conditions for the upsert \`${tableTypeName}\` mutation.`,
              fields,
            },
            {
              isPgCreateInputType: false,
              pgInflection: table,
            }
          );
          const IgnoreType = newWithHooks(
            GraphQLInputObjectType,
            {
              name: `Upsert${tableTypeName}Ignore`,
              description: `Fields to skip when resolving conflicts upsert \`${tableTypeName}\` mutation.`,
              fields: attributes.reduce((acc, attr) => {
                acc[ inflection.camelCase(attr.name) ] = { type: GraphQLBoolean };
                return acc;
              }, {}),
            },
            {
              isPgCreateInputType: false,
              pgInflection: table,
            }
          );

          // Standard input type that 'create' uses
          const InputType = newWithHooks(
            GraphQLInputObjectType,
            {
              name: `Upsert${tableTypeName}Input`,
              description: `All input for the upsert \`${tableTypeName}\` mutation.`,
              fields: {
                clientMutationId: {
                  description:
                    "An arbitrary string value with no semantic meaning. Will be included in the payload verbatim. May be used to track mutations by the client.",
                  type: GraphQLString,
                },
                ...(TableInput
                  ? {
                      [inflection.tableFieldName(table)]: {
                        description: `The \`${tableTypeName}\` to be upserted by this mutation.`,
                        type: new GraphQLNonNull(TableInput),
                      },
                    }
                  : null),
              },
            },
            {
              isPgCreateInputType: false,
              pgInflection: table,
            }
          );

          // Standard payload type that 'create' uses
          const PayloadType = newWithHooks(
            GraphQLObjectType,
            {
              name: `Upsert${tableTypeName}Payload`,
              description: `The output of our upsert \`${tableTypeName}\` mutation.`,
              fields: ({ fieldWithHooks }) => {
                const tableName = inflection.tableFieldName(table);
                return {
                  clientMutationId: {
                    description:
                      "The exact same `clientMutationId` that was provided in the mutation input, unchanged and unused. May be used by a client to track mutations.",
                    type: GraphQLString,
                  },
                  [tableName]: pgField(build, fieldWithHooks, tableName, {
                    description: `The \`${tableTypeName}\` that was upserted by this mutation.`,
                    type: Table,
                  }),
                };
              },
            },
            {
              isMutationPayload: true,
              isPgCreatePayloadType: false,
              pgIntrospection: table,
            }
          );

          // Create upsert fields from each introspected table
          const fieldName = `upsert${tableTypeName}`;

          memo[fieldName] = fieldWithHooks(
            fieldName,
            (context) => {
              const { getDataFromParsedResolveInfoFragment } = context;
              return {
                description: `Upserts a single \`${tableTypeName}\`.`,
                type: PayloadType,
                args: {
                  where: {
                    type: WhereType,
                  },
                  input: {
                    type: new GraphQLNonNull(InputType),
                  },
                  ignore: {
                    type: IgnoreType,
                  },
                },
                async resolve(
                  data,
                  { where, input, ignore },
                  { pgClient },
                  resolveInfo
                ) {
                  const parsedResolveInfoFragment =
                    parseResolveInfo(resolveInfo);
                  const resolveData = getDataFromParsedResolveInfoFragment(
                    parsedResolveInfoFragment,
                    PayloadType
                  );
                  const insertedRowAlias = sql.identifier(Symbol()); // eslint-disable-line
                  const query = queryFromResolveData(
                    insertedRowAlias,
                    insertedRowAlias,
                    resolveData,
                    {}
                  );

                  const sqlColumns: any[] = [];
                  const sqlValues: any[] = [];
                  const inputData: any[] =
                    input[inflection.tableFieldName(table)];

                  // Find the unique constraints
                  const uniqueConstraints =
                    pgIntrospectionResultsByKind.constraint
                      .filter((con) => con.classId === table.id)
                      .filter((con) => con.type === "u" || con.type === "p");

                  // Store attributes (columns) for easy access
                  const attributes =
                    pgIntrospectionResultsByKind.attribute.filter(
                      (attr) => attr.classId === table.id
                    );

                  // Figure out to which columns the unique constraints belong to
                  const constraintColumns: { [key: string]: any[] } =
                    uniqueConstraints.reduce(
                      (acc, constraint) => ({
                        ...acc,
                        [constraint.name]: new Set(
                          constraint.keyAttributeNums.map((num) =>
                            attributes.find((attr) => attr.num === num)
                          )
                        ),
                      }),
                      {}
                    );

                  const upsertedColumns: Set<string> = new Set(
                    attributes.filter((attr) => {
                      if (
                        Object.prototype.hasOwnProperty.call(
                          inputData,
                          fieldName
                        )
                      ) {
                        return attr.name;
                      }
                    })
                  );

                  // Depending on whether a where clause was passed, we want to determine which
                  // constraint to use in the upsert ON CONFLICT cause.
                  // If where clause: Check for the first constraint that the where clause provides all matching unique columns
                  // If no where clause: Check for the first constraint that our upsert columns provides all matching unique columns
                  //     or default to primary key constraint (existing functionality).
                  const primaryKeyConstraint = uniqueConstraints.find(
                    (con) => con.type === "p"
                  );
                  const matchingConstraint = where
                    ? Object.entries(constraintColumns).find(([_, columns]) =>
                        [...columns].every(
                          (col) =>
                            where[inflection.camelCase(col.name)] !== undefined
                        )
                      )
                    : Object.entries(constraintColumns).find(([_, columns]) =>
                        [...columns].every((col) =>
                          upsertedColumns.has(col.name)
                        )
                      ) ??
                      Object.entries(constraintColumns).find(
                        ([key]) => key === primaryKeyConstraint.name
                      );

                  if (!matchingConstraint) {
                    throw new Error(
                      `Unable to determine upsert unique constraint for given upserted columns: ${[
                        ...upsertedColumns,
                      ].join(", ")}`
                    );
                  }

                  const [constraintName] = matchingConstraint;

                  const ignoreUpdate = {};

                  // Loop thru columns and "SQLify" them
                  attributes.forEach((attr) => {
                    // where clause should override any "input" for the matching column to be a true upsert
                    let hasWhereClauseValue = false;
                    let whereClauseValue = undefined;
                    if (
                      where &&
                      Object.prototype.hasOwnProperty.call(
                        where,
                        inflection.camelCase(attr.name)
                      )
                    ) {
                      whereClauseValue = where[inflection.camelCase(attr.name)];
                      hasWhereClauseValue = true;
                    }

                    ignoreUpdate[attr.name] = omit(attr, "updateOnConflict");

                    if (ignore && Object.prototype.hasOwnProperty.call(
                      ignore,
                      inflection.camelCase(attr.name)
                    )) {
                      ignoreUpdate[attr.name] = ignore[inflection.camelCase(attr.name)];
                    }

                    // Do we have a value for the field in input?
                    const fieldName = inflection.column(attr);
                    if (
                      Object.prototype.hasOwnProperty.call(inputData, fieldName)
                    ) {
                      const val = inputData[fieldName];

                      // The user passed a where clause condition value that does not match the upsert input value for the same property
                      if (hasWhereClauseValue && whereClauseValue !== val) {
                        throw new Error(
                          `Value passed in the input for ${fieldName} does not match the where clause value.`
                        );
                      }

                      sqlColumns.push(sql.identifier(attr.name));
                      sqlValues.push(gql2pg(val, attr.type, attr.typeModifier));
                    } else if (hasWhereClauseValue) {
                      // If it was ommitted in the input, we should add it
                      sqlColumns.push(sql.identifier(attr.name));
                      sqlValues.push(
                        gql2pg(whereClauseValue, attr.type, attr.typeModifier)
                      );
                    }
                  });

                  // Construct a array in case we need to do an update on conflict
                  const conflictUpdateArray = sqlColumns.filter((col) => ignoreUpdate[col.names[0]] != true).map(
                    (col) =>
                      sql.query`${sql.identifier(
                        col.names[0]
                      )} = excluded.${sql.identifier(col.names[0])}`
                  );

                  // SQL query for upsert mutations
                  // see: http://www.postgresqltutorial.com/postgresql-upsert/
                  const mutationQuery = sql.query`
                        insert into ${sql.identifier(
                          table.namespace.name,
                          table.name
                        )}
                        ${
                          sqlColumns.length
                            ? sql.fragment`(${sql.join(sqlColumns, ", ")})
                            values (${sql.join(sqlValues, ", ")})
                            on conflict on constraint ${sql.identifier(
                              constraintName
                            )}
                            do update set ${sql.join(
                              conflictUpdateArray,
                              ", "
                            )}`
                            : sql.fragment`default values`
                        } returning *`;

                  const rows = await viaTemporaryTable(
                    pgClient,
                    sql.identifier(table.namespace.name, table.name),
                    mutationQuery,
                    insertedRowAlias,
                    query
                  );
                  return {
                    clientMutationId: input.clientMutationId,
                    data: rows[0],
                  };
                },
              };
            },
            {
              pgFieldIntrospection: table,
              isPgCreateMutationField: false,
            }
          );
          return memo;
        }, {})
    );
  });
};

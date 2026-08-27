import { NamedPlaceholdersNotSupportedError } from "../../error/NamedPlaceholdersNotSupportedError"
import type { ObjectLiteral } from "../../common/ObjectLiteral"
import { QueryFailedError } from "../../error/QueryFailedError"
import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError"
import { QueryResult } from "../../query-runner/QueryResult"
import { Broadcaster } from "../../subscriber/Broadcaster"
import { BroadcasterResult } from "../../subscriber/BroadcasterResult"
import { AbstractSqliteQueryRunner } from "../sqlite-abstract/AbstractSqliteQueryRunner"
import type { ReactNativeDriver } from "./ReactNativeDriver"

/**
 * Runs queries on a single sqlite database connection.
 */
export class ReactNativeQueryRunner extends AbstractSqliteQueryRunner {
    /**
     * Database driver used by connection.
     */
    // @ts-ignore temporary, we need to fix the issue with the AbstractSqliteDriver and circular errors
    driver: ReactNativeDriver

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: ReactNativeDriver) {
        super()
        this.driver = driver
        this.dataSource = driver.dataSource
        this.broadcaster = new Broadcaster(this)
    }

    /**
     * Called before migrations are run.
     */
    async beforeMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = OFF`)
    }

    /**
     * Called after migrations are run.
     */
    async afterMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = ON`)
    }

    /**
     * Executes a given SQL query.
     *
     * @param query
     * @param parameters
     * @param useStructuredResult
     */
    async query(
        query: string,
        parameters?: any[] | ObjectLiteral,
        useStructuredResult = false,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()
        if (parameters && !Array.isArray(parameters))
            throw new NamedPlaceholdersNotSupportedError()

        const databaseConnection = await this.connect()

        this.driver.dataSource.logger.logQuery(query, parameters, this)
        await this.broadcaster.broadcast("BeforeQuery", query, parameters)

        const broadcasterResult = new BroadcasterResult()

        const queryStartTime = Date.now()

        return new Promise((ok, fail) => {
            try {
                databaseConnection.executeSql(
                    query,
                    parameters,
                    async (raw: any) => {
                        try {
                            // log slow queries if maxQueryExecution time is set
                            const maxQueryExecutionTime =
                                this.driver.options.maxQueryExecutionTime
                            const queryEndTime = Date.now()
                            const queryExecutionTime =
                                queryEndTime - queryStartTime
                            this.broadcaster.broadcastAfterQueryEvent(
                                broadcasterResult,
                                query,
                                parameters,
                                true,
                                queryExecutionTime,
                                raw,
                                undefined,
                            )

                            if (
                                maxQueryExecutionTime &&
                                queryExecutionTime > maxQueryExecutionTime
                            )
                                this.driver.dataSource.logger.logQuerySlow(
                                    queryExecutionTime,
                                    query,
                                    parameters,
                                    this,
                                )

                            const result = new QueryResult()

                            if (raw?.hasOwnProperty("rowsAffected")) {
                                result.affected = raw.rowsAffected
                            }

                            if (raw?.hasOwnProperty("rows")) {
                                const records = []
                                for (let i = 0; i < raw.rows.length; i++) {
                                    records.push(raw.rows.item(i))
                                }

                                result.raw = records
                                result.records = records
                            }

                            // return id of inserted row, if query was insert statement.
                            if (query.startsWith("INSERT INTO")) {
                                result.raw = raw.insertId
                            }

                            // the subscribers have to finish before the caller
                            // is resumed, otherwise it observes the query as
                            // complete while they are still running
                            await broadcasterResult.wait()

                            if (useStructuredResult) {
                                ok(result)
                            } else {
                                ok(result.raw)
                            }
                        } catch (err) {
                            // a subscriber that rejects would otherwise leave this
                            // promise unsettled forever, since neither "ok" nor
                            // "fail" is reached
                            fail(err)
                        }
                    },
                    async (err: any) => {
                        try {
                            this.driver.dataSource.logger.logQueryError(
                                err,
                                query,
                                parameters,
                                this,
                            )
                            this.broadcaster.broadcastAfterQueryEvent(
                                broadcasterResult,
                                query,
                                parameters,
                                false,
                                undefined,
                                undefined,
                                err,
                            )

                            await broadcasterResult.wait()

                            fail(new QueryFailedError(query, parameters, err))
                        } catch (subscriberError) {
                            fail(subscriberError)
                        }
                    },
                )
            } catch (err) {
                fail(err)
            }
        })
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Parametrizes given object of values. Used to create column=value queries.
     *
     * @param objectLiteral
     * @param startIndex
     */
    protected parametrize(
        objectLiteral: ObjectLiteral,
        startIndex: number = 0,
    ): string[] {
        return Object.keys(objectLiteral).map((key, index) => `"${key}"` + "=?")
    }
}

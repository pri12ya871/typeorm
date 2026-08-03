import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"
import type { DataSource } from "../../../../src/data-source/DataSource"
import { expect } from "chai"
import { Post } from "./entity/Post"
import { Comment } from "./entity/Comment"

describe("query builder > stream entities", () => {
    let dataSources: DataSource[]
    before(async () => {
        dataSources = await createTestingConnections({
            entities: [Post, Comment],
            schemaCreate: true,
            dropSchema: true,
        })
    })
    beforeEach(() => reloadTestingDatabases(dataSources))
    after(() => closeTestingConnections(dataSources))

    // These cover the validation performed before any streaming begins, so they
    // run on drivers that do not implement QueryRunner.stream (sqlite).
    // Hydration across chunk boundaries needs a streaming driver and is covered
    // separately.

    it("throws when the query is not ordered by the root primary key", () =>
        Promise.all(
            dataSources.map(async (dataSource) => {
                const qb = dataSource
                    .createQueryBuilder(Post, "post")
                    .leftJoinAndSelect("post.comments", "comment")
                    .orderBy("post.title", "ASC")

                expect(() => qb.streamEntities()).to.throw(
                    /ordered by the root primary key/,
                )
            }),
        ))

    it("throws when ordering starts with a non primary key column", () =>
        Promise.all(
            dataSources.map(async (dataSource) => {
                const qb = dataSource
                    .createQueryBuilder(Post, "post")
                    .orderBy("post.title", "ASC")
                    .addOrderBy("post.id", "ASC")

                expect(() => qb.streamEntities()).to.throw(
                    /ordered by the root primary key/,
                )
            }),
        ))

    it("accepts a query ordered by the root primary key", () =>
        Promise.all(
            dataSources.map(async (dataSource) => {
                const qb = dataSource
                    .createQueryBuilder(Post, "post")
                    .leftJoinAndSelect("post.comments", "comment")
                    .orderBy("post.id", "ASC")

                expect(() => qb.streamEntities()).to.not.throw()
            }),
        ))

    it("throws when selecting something without entity metadata", () =>
        Promise.all(
            dataSources.map(async (dataSource) => {
                // a table name that maps to no entity, so the alias carries no
                // metadata — "post" would resolve to the Post entity
                const qb = dataSource
                    .createQueryBuilder()
                    .select("t.id")
                    .from("unmapped_table", "t")

                expect(() => qb.streamEntities()).to.throw(
                    /can only be used when selecting from an entity/,
                )
            }),
        ))

    it("throws on a non positive chunk size", () =>
        Promise.all(
            dataSources.map(async (dataSource) => {
                const qb = dataSource
                    .createQueryBuilder(Post, "post")
                    .orderBy("post.id", "ASC")

                expect(() => qb.streamEntities({ chunkSize: 0 })).to.throw(
                    /"chunkSize" must be a positive number/,
                )
            }),
        ))

    // Validation must happen when the method is called, not when the resulting
    // iterator is first pulled from, which is what a bare async generator does.
    it("validates eagerly rather than on first iteration", () =>
        Promise.all(
            dataSources.map(async (dataSource) => {
                const qb = dataSource
                    .createQueryBuilder(Post, "post")
                    .orderBy("post.title", "ASC")

                let threwSynchronously = false
                try {
                    qb.streamEntities()
                } catch {
                    threwSynchronously = true
                }
                expect(threwSynchronously).to.be.true
            }),
        ))
})

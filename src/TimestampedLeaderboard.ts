
import IORedis, { Pipeline } from "ioredis";
import { Leaderboard, LeaderboardOptions, ID, Entry } from "./Leaderboard";
import { AssertionError } from "assert";

type PATH_ID = string;
type TIME_ID = string;


export type TimestampedLeaderboardOptions = LeaderboardOptions & {
    earlierToLater: boolean
}

export class TimestampedLeaderboard extends Leaderboard {
    protected earlierToLater: boolean

    constructor(client: IORedis.Redis, options: Partial<TimestampedLeaderboardOptions>) {
        super(client, options);
        this.earlierToLater = options.earlierToLater === undefined ? true : options.earlierToLater
    }

    protected id2PathedId(id: ID): PATH_ID {
        const pathId = this.getPath() + "/ids/" + id
        return pathId
    }

    protected id2CurrentTimestampedId(id: ID): TIME_ID {
        let timestamp = (new Date()).getTime()
        if (!this.earlierToLater) timestamp = 10 ** 13 - timestamp
        const timestampedId = timestamp.toString() + ":" + id // 13 digits is enough for more than 315 years
        return timestampedId
    }

    protected timestampedId2Id(timestampedId: TIME_ID): ID {
        const id = timestampedId.split(":").slice(1).join("")
        return id
    }

    protected normalizeEntry(entry: Entry): Entry {
        entry.id = this.timestampedId2Id(entry.id)
        return entry
    }

    protected async getLastTimeStampedId(id: ID, createIfNotExists: boolean = true): Promise<ID | null> {
        const pathedId = this.id2PathedId(id)
        let timestampedId = await this.client.get(pathedId)
        if (createIfNotExists && timestampedId === null) {
            timestampedId = await this.updateTimeStampedId(id)
        }
        return timestampedId
    }

    protected async updateTimeStampedId(id: ID, timestampedId?: TIME_ID): Promise<TIME_ID> {
        if (timestampedId === undefined) timestampedId = this.id2CurrentTimestampedId(id)
        const pathedId = this.id2PathedId(id)
        const lastTimestampedId = await this.getLastTimeStampedId(id, false)

        let pipeline = this.client.pipeline()
        pipeline.set(pathedId, timestampedId)
        if (lastTimestampedId !== null) pipeline = super.removeMulti(lastTimestampedId!, pipeline)
        await pipeline.exec()

        return timestampedId
    }

    protected async deleteTimestampedId(id: ID): Promise<TIME_ID | null> {
        const pathedId = this.id2PathedId(id)
        const oldTimestampedId = await this.client.get(pathedId)
        const exists = oldTimestampedId === null
        if (exists) {
            await this.client.del(pathedId)
        }
        return oldTimestampedId
    }

    public async add(id: string, score: number): Promise<void> {
        const timestampedId = await this.updateTimeStampedId(id)
        super.add(timestampedId, score)
    }

    public async improve(id: string, amount: number): Promise<Boolean> {
        const lastTimestampedId = await this.getLastTimeStampedId(id, false)
        const currentTimestampedId = this.id2CurrentTimestampedId(id)
        let updated: Boolean
        if (lastTimestampedId === null) {
            updated = await super.improve(currentTimestampedId, amount)
            await this.updateTimeStampedId(id, currentTimestampedId)
        }
        else {
            updated = await super.improve(lastTimestampedId!, amount)
            if (updated) {
                await this.updateTimeStampedId(id, currentTimestampedId)
                await super.add(currentTimestampedId, amount)
            }
        }
        return updated
    }

    public async incr(id: string, amount: number): Promise<number> {
        const lastTimestampedId = await this.getLastTimeStampedId(id, false)
        let newScore: number
        if (lastTimestampedId === null) {
            newScore = amount
        }
        else {
            newScore = await super.incr(lastTimestampedId, amount)
        }
        const timestampedId = await this.updateTimeStampedId(id)
        await super.add(timestampedId, amount)
        return newScore

    }

    public async remove(id: string): Promise<void> {
        const lastTimestampedId = await this.deleteTimestampedId(id)
        if (lastTimestampedId !== null) {
            await super.remove(lastTimestampedId)
        }
    }

    async clear(): Promise<void> {
        const allTimestampedIds = await this.client.zrange(this.getPath(), 0, -1)
        const allIds = Array.from(allTimestampedIds, (timestampedId, _) => this.timestampedId2Id(timestampedId))
        const allPathedIds = Array.from(allIds, (id, _) => this.id2PathedId(id))
        await this.client.del(...allPathedIds)
        await this.client.del(this.getPath());
    }

    async peek(id: ID): Promise<Entry | null> {
        const timestampedId = await this.getLastTimeStampedId(id, false)
        if (timestampedId === null) return null

        const entry = await super.peek(timestampedId)
        entry!.id = id
        return entry
    }

    async score(id: ID): Promise<number | null> {
        const timestampedId = await this.getLastTimeStampedId(id, false)
        if (timestampedId === null) return null

        return await super.score(timestampedId)
    }

    async rank(id: ID): Promise<number | null> {
        const timestampedId = await this.getLastTimeStampedId(id, false)
        if (timestampedId === null) return null

        return await super.rank(timestampedId)
    }

    async list(low: number, high: number): Promise<Entry[]> {
        const entries = await super.list(low, high)
        entries.forEach((entry, index, _) => { this.normalizeEntry(entry) })
        return entries
    }

    async around(id: ID, distance: number, fillBorders: boolean = false): Promise<Entry[]> {
        const timestampedId = await this.getLastTimeStampedId(id, false)
        if (timestampedId === null) return []
        const entries = await super.around(timestampedId, distance, fillBorders)
        entries.forEach((entry, index, _) => { this.normalizeEntry(entry) })
        return entries
    }


    // ***************************** *********************** ***************************
    // ***************************** NOT IMPLEMENTED YET !!! ***************************
    // ***************************** *********************** ***************************

    public addMulti(id: string, score: number, pipeline: Pipeline): Pipeline {
        throw new AssertionError({ message: "not implemented yet!" })
        super.addMulti(id, score, pipeline)
    }

    public improveMulti(id: string, amount: number, pipeline: Pipeline): Pipeline {
        throw new AssertionError({ message: "not implemented yet!" })
        super.addMulti(id, amount, pipeline)
    }

    public incrMulti(id: string, amount: number, pipeline: Pipeline): Pipeline {
        throw new AssertionError({ message: "not implemented yet!" })
        super.incrMulti(id, amount, pipeline)
    }

    public removeMulti(id: string, pipeline: Pipeline): Pipeline {
        throw new AssertionError({ message: "not implemented yet!" })
        pipeline = super.removeMulti(id, pipeline)
        return pipeline
    }
}

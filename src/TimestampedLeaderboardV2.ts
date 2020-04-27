
import IORedis, { Pipeline } from "ioredis";
import { Leaderboard, ID, Entry } from "./Leaderboard";
import { TimestampedLeaderboardOptions, TIME_ID } from "./TimestampedLeaderboard";
import { buildScript } from "./Common";

export class TimestampedLeaderboardV2 extends Leaderboard {
    protected earlierToLater: boolean

    constructor(client: IORedis.Redis, options: Partial<TimestampedLeaderboardOptions>) {
        super(client, options);
        this.earlierToLater = options.earlierToLater === undefined ? true : options.earlierToLater
    }

    protected getTimestamp(): number {
        let timestamp = (new Date()).getTime()
        if (!this.earlierToLater) timestamp = 10 ** 13 - timestamp
        return timestamp
    }

    protected timestampedId2Id(timestampedId: TIME_ID): ID {
        const id = timestampedId.split(":").slice(1).join("")
        return id
    }

    protected normalizeEntry(entry: Entry): Entry {
        entry.id = this.timestampedId2Id(entry.id)
        return entry
    }

    protected async getLastTimestampedId(id: ID, createIfNotExists: boolean = true): Promise<ID | null> {
        let timestampedId = await this.client.eval(
            buildScript(`return getLastTimestampedId(ARGV[1], ARGV[2], ARGV[3], ARGV[4])`),
            0, this.getPath(), this.getTimestamp(), id, createIfNotExists.toString())
        return timestampedId
    }

    public async add(id: string, score: number): Promise<void> {
        await this.client.eval(
            buildScript(`return timestampedAdd(ARGV[1], ARGV[2], ARGV[3], ARGV[4])`),
            0, this.getPath(), this.getTimestamp(), id, score)
    }

    public addMulti(id: string, score: number, pipeline: Pipeline): Pipeline {
        pipeline = pipeline.eval(
            buildScript(`return timestampedAdd(ARGV[1], ARGV[2], ARGV[3], ARGV[4])`),
            0, this.getPath(), this.getTimestamp(), id, score)
        return pipeline
    }

    public async improve(id: string, score: number): Promise<Boolean> {
        const updated = await this.client.eval(
            buildScript(`return timestampedImprove(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])`),
            0, this.getPath(), this.getTimestamp(), this.isLowToHigh().toString(), id, score)
        return updated == 1
    }

    public improveMulti(id: string, score: number, pipeline: Pipeline): Pipeline {
        pipeline = pipeline.eval(
            buildScript(`return timestampedImprove(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5])`),
            0, this.getPath(), this.getTimestamp(), this.isLowToHigh().toString(), id, score)
        return pipeline
    }

    public async incr(id: string, amount: number): Promise<number> {
        console.log(id, amount);
        
        const newScore: string = await this.client.eval(
            buildScript(`return timestampedIncr(ARGV[1], ARGV[2], ARGV[3], ARGV[4])`),
            0, this.getPath(), this.getTimestamp(), id, amount)
        console.log(newScore);
        
        return parseFloat(newScore)

    }

    public incrMulti(id: string, amount: number, pipeline: Pipeline): Pipeline {
        pipeline = pipeline.eval(
            buildScript(`return timestampedIncr(ARGV[1], ARGV[2], ARGV[3], ARGV[4])`),
            0, this.getPath(), this.getTimestamp(), id, amount)
        return pipeline
    }

    public async remove(id: string): Promise<void> {
        await this.client.eval(
            buildScript(`return timestampedRemove(ARGV[1], ARGV[2], ARGV[3])`),
            0, this.getPath(), this.getTimestamp(), id)
    }

    public removeMulti(id: string, pipeline: Pipeline): Pipeline {
        pipeline = pipeline.eval(
            buildScript(`return timestampedRemove(ARGV[1], ARGV[2], ARGV[3])`),
            0, this.getPath(), this.getTimestamp(), id)
        return pipeline
    }

    async clear(): Promise<void> {
        await this.client.eval(
            buildScript(`return timestampedClear(ARGV[1])`),
            0, this.getPath())
    }

    async peek(id: ID): Promise<Entry | null> {
        const timestampedId = await this.getLastTimestampedId(id, false)
        if (timestampedId === null) return null

        const entry = await super.peek(timestampedId)
        entry!.id = id
        return entry
    }

    async score(id: ID): Promise<number | null> {
        const timestampedId = await this.getLastTimestampedId(id, false)
        if (timestampedId === null) return null

        return await super.score(timestampedId)
    }

    async rank(id: ID): Promise<number | null> {
        const timestampedId = await this.getLastTimestampedId(id, false)
        if (timestampedId === null) return null

        return await super.rank(timestampedId)
    }

    async list(low: number, high: number): Promise<Entry[]> {
        const entries = await super.list(low, high)
        entries.forEach((entry, index, _) => { this.normalizeEntry(entry) })
        return entries
    }

    async around(id: ID, distance: number, fillBorders: boolean = false): Promise<Entry[]> {
        const timestampedId = await this.getLastTimestampedId(id, false)
        if (timestampedId === null) return []
        const entries = await super.around(timestampedId, distance, fillBorders)
        entries.forEach((entry, index, _) => { this.normalizeEntry(entry) })
        return entries
    }


    // ***************************** *********************** ***************************
    // ***************************** NOT IMPLEMENTED YET !!! ***************************
    // ***************************** *********************** ***************************
}

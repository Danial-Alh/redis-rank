
import IORedis, { Pipeline } from "ioredis";
import { Leaderboard, LeaderboardOptions, ID, Entry } from "./Leaderboard";
import { AssertionError } from "assert";
import { TimestampedLeaderboard } from "./TimestampedLeaderboard";
import { buildScript } from "./Common";

type PATH_ID = string;
type NEW_ID = string;

export type MultimetricLeaderboardOptions = {
    path: string,
    leaderboards: TimestampedLeaderboard[],
    maxUsers: number
}

export class MultimetricLeaderboard extends Leaderboard {
    protected leaderboards: TimestampedLeaderboard[]
    protected requiredDigitsForMaxUsers: number

    constructor(client: IORedis.Redis, options: Partial<MultimetricLeaderboardOptions>) {
        super(client, { path: options.path, lowToHigh: true })
        if (options.leaderboards === undefined || options.maxUsers === undefined || options.leaderboards.length === 0) {
            throw new AssertionError({ message: "Invalid options! At least one leaderboard and maxUsers must be set." })
        }
        this.leaderboards = options.leaderboards
        this.requiredDigitsForMaxUsers = Math.ceil(Math.log10(options.maxUsers))
    }

    protected id2PathedId(id: ID): PATH_ID {
        const pathId = this.getPath() + "/ids/" + id
        return pathId
    }

    protected async id2NewId(id: ID): Promise<string | null> {
        let idCompnents = []
        for (let i = 0; i < this.leaderboards.length; i++) {
            let rank = await this.leaderboards[i].rank(id)
            if (rank === null) return null
            let rankStr = rank.toString()
            rankStr = rankStr.padStart(this.requiredDigitsForMaxUsers, "0")
            idCompnents.push(rankStr)
        }
        let timestamp = (new Date()).getTime()
        timestamp = 10 ** 13 - timestamp
        idCompnents.push(timestamp.toString())

        let newId = idCompnents.join("-") + ":" + id
        return newId
    }

    public async getLastNewId(id: ID): Promise<string | null> {
        return await this.client.get(this.id2PathedId(id))
    }

    public newId2Id(newId: NEW_ID): string {
        let newId_meta_info_length = (13 + 1) + this.leaderboards.length * (this.requiredDigitsForMaxUsers + 1)
        return newId.slice(newId_meta_info_length)
    }

    protected normalizeEntry(entry: Entry): Entry {
        entry.id = this.newId2Id(entry.id)
        
        return entry
    }

    public async updateRank(id: ID): Promise<void> {
        let pathedId = this.id2PathedId(id)
        let newId = await this.id2NewId(id)
        if (newId === null) throw new AssertionError({ message: "the updating ID must exists on all leaderboards!" })
        let oldId = await this.client.get(pathedId)


        if (oldId === null || newId < oldId) {
            let pipeline = this.client.multi()

            if (oldId !== null) {
                pipeline = super.removeMulti(oldId, pipeline)
            }

            pipeline = super.addMulti(newId, 0, pipeline)
            pipeline = pipeline.set(pathedId, newId)
            await pipeline.exec()
        }
    }

    async peek(id: ID): Promise<Entry | null> {
        const newId = await this.getLastNewId(id)
        if (newId === null) return null

        const entry = await super.peek(newId)
        this.normalizeEntry(entry!)
        return entry
    }

    async score(id: ID): Promise<number | null> {
        const newId = await this.getLastNewId(id)
        if (newId === null) return null

        return await super.score(newId)
    }

    async rank(id: ID): Promise<number | null> {
        const newId = await this.getLastNewId(id)
        if (newId === null) return null

        return await super.rank(newId)
    }

    async list(low: number, high: number): Promise<Entry[]> {
        const entries = await super.list(low, high)
        entries.forEach((entry, index, _) => { this.normalizeEntry(entry) })
        return entries
    }

    async around(id: ID, distance: number, fillBorders: boolean = false): Promise<Entry[]> {
        const newId = await this.getLastNewId(id)
        if (newId === null) return []
        const entries = await super.around(newId, distance, fillBorders)
        entries.forEach((entry, index, _) => { this.normalizeEntry(entry) })
        return entries
    }

    async clear(): Promise<void> {
        await this.client.eval(
            buildScript(`return timestampedClear(ARGV[1])`),
            0, this.getPath())
    }

    // ***************************** *********************** ***************************
    // ***************************** NOT IMPLEMENTED YET !!! ***************************
    // ***************************** *********************** ***************************


    public async add(id: string, score: number): Promise<void> {
        throw new AssertionError({ message: "not implemented yet!" })
    }

    public async improve(id: string, amount: number): Promise<Boolean> {
        throw new AssertionError({ message: "not implemented yet!" })
    }

    public async incr(id: string, amount: number): Promise<number> {
        throw new AssertionError({ message: "not implemented yet!" })
    }

    public async remove(id: string): Promise<void> {
        throw new AssertionError({ message: "not implemented yet!" })
    }

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

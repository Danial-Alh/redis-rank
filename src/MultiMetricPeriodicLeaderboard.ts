import { Leaderboard } from './Leaderboard';
import { Redis } from 'ioredis';
import { TimestampedLeaderboard } from './TimestampedLeaderboard';
import { MultimetricLeaderboard } from './MultiMetricLeaderboard';
import { AssertionError } from 'assert';

import PL = require('./PeriodicLeaderboard')
export type TimeFrame = PL.TimeFrame

export type MultiMetricPeriodicLeaderboardOptions = {
    /** base key to store the leaderboards (plb:<time key>) */
    path: string,
    /** leaderboard cycle */
    timeFrame: TimeFrame,
    /** custom function to evaluate the current time */
    now(): Date,
    /** underlying leaderboard options */
    leaderboards: PL.PeriodicLeaderboard[],
    maxUsers: number
}

export class MultiMetricPeriodicLeaderboard extends PL.BasePeriodicLeaderboard {
    /** active leaderboard */
    protected leaderboard: (Leaderboard | TimestampedLeaderboard | null) = null;

    constructor(client: Redis, options: Partial<MultiMetricPeriodicLeaderboardOptions> = {}) {
        super(
            client,
            Object.assign({
                path: "plb",
                timeFrame: 'all-time',
                now: () => new Date,
                leaderboardOptions: null
            }, options))

        if (options.leaderboards === undefined) {
            throw new AssertionError({ message: "the sub-leaderboards must be provided!" })
        }
    }

    /**
     * Get a the leaderboard in a specific date
     */
    public get(date: Date): Leaderboard {
        const subLeaderboards = Array.from(this.options.leaderboards,
            (pl: PL.PeriodicLeaderboard, i) => (pl.get(date) as TimestampedLeaderboard))
        return new MultimetricLeaderboard(this.client,
            {
                path: `${this.options.path}:${this.getKey(date)}`,
                leaderboards: subLeaderboards,
                maxUsers: this.options.maxUsers
            });
    }

    /**
     * Get the leaderboard based on the current time
     */
    public getCurrent(): Leaderboard {
        let path = `${this.options.path}:${this.getCurrentKey()}`;

        if (this.leaderboard === null || this.leaderboard.getPath() !== path) {
            delete this.leaderboard;
            const subLeaderboards = Array.from(this.options.leaderboards,
                (pl: PL.PeriodicLeaderboard, i) => (pl.getCurrent() as TimestampedLeaderboard))
            this.leaderboard = new MultimetricLeaderboard(this.client,
                {
                    path: path,
                    leaderboards: subLeaderboards,
                    maxUsers: this.options.maxUsers
                })
        }

        return this.leaderboard;
    }

    public async clear(date: Date): Promise<void> {
        await this.get(date).clear()
    }

    public async clearCurrent(): Promise<void> {
        await this.getCurrent().clear()
    }
}

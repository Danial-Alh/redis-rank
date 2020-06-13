import { Leaderboard, LeaderboardOptions } from './Leaderboard';
import { Redis } from 'ioredis';
import moment from 'moment';
import { TimestampedLeaderboardOptions, TimestampedLeaderboard } from './TimestampedLeaderboard';

/**
 * Time interval of one leaderboard cycle
 */
export type TimeFrame = 'minute' | 'hourly' | 'daily' | 'weekly' | 'monthly' | 'yearly' | 'all-time';

export type PeriodicLeaderboardOptions = {
    /** base key to store the leaderboards (plb:<time key>) */
    path: string,
    /** leaderboard cycle */
    timeFrame: TimeFrame,
    /** custom function to evaluate the current time */
    now(): Date,
    // leaderboardClass: (typeof Leaderboard) | (typeof TimestampedLeaderboard) | (typeof MultimetricLeaderboard),
    leaderboardClass: typeof Leaderboard,
    /** underlying leaderboard options */
    leaderboardOptions?: Partial<LeaderboardOptions | TimestampedLeaderboardOptions>
}

export class BasePeriodicLeaderboard {
    protected client: Redis
    protected format: string
    protected options: any

    constructor(client: Redis, options: Partial<PeriodicLeaderboardOptions>) {
        this.client = client
        this.options = options
        this.format = BasePeriodicLeaderboard.momentFormat(this.options.timeFrame);
    }

    /**
     * Returns the appropiate moment format for a Time Frame
     * 
     * e.g. for 'minute' [y]YYYY-[m]MM-[w]ww-[d]DD-[h]HH-[m]mm
     */
    protected static momentFormat(timeFrame: TimeFrame): string {
        if (timeFrame == 'all-time')
            return '[all]';

        const frames = ['yearly', 'monthly', 'weekly', 'daily', 'hourly', 'minute'];
        const format = ['YYYY', 'MM', 'ww', 'DD', 'HH', 'mm'];

        const formatForTimeframe = format.slice(0, frames.indexOf(timeFrame) + 1).join('-')
        return formatForTimeframe;
    }

    /**
     * Return the format used for the current Time Frame
     */
    getKeyFormat() {
        return this.format;
    }

    /**
     * Get a the key of the leaderboard in a specific date
     */
    getKey(date: Date): string {
        return moment(date).format(this.format);
    }

    /**
     * Returns the key of the leaderboard that
     * should be used based on the current time
     */
    getCurrentKey(): string {
        return this.getKey(this.options.now());
    }
}


export class PeriodicLeaderboard extends BasePeriodicLeaderboard {
    /** active leaderboard */
    protected leaderboard: (Leaderboard | TimestampedLeaderboard | null) = null;

    constructor(client: Redis, options: Partial<PeriodicLeaderboardOptions> = {}) {
        super(
            client,
            Object.assign({
                path: "plb",
                timeFrame: 'all-time',
                now: () => new Date,
                leaderboardClass: Leaderboard,
                leaderboardOptions: null
            }, options))
    }

    /**
     * Get a the leaderboard in a specific date
     */
    get(date: Date): Leaderboard {
        return new this.options.leaderboardClass(this.client, {
            path: `${this.options.path}:${this.getKey(date)}`,
            ...this.options.leaderboardOptions
        });
    }

    /**
     * Get the leaderboard based on the current time
     */
    getCurrent(): Leaderboard {
        let path = `${this.options.path}:${this.getCurrentKey()}`;

        if (this.leaderboard === null || this.leaderboard.getPath() !== path) {
            delete this.leaderboard;
            this.leaderboard = new this.options.leaderboardClass(this.client, {
                ...this.options.leaderboardOptions,
                path: path
            });
        }

        return this.leaderboard!;
    }
}

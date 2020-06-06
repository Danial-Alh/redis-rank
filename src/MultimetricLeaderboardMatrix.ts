import { Redis, Pipeline } from 'ioredis';
import { Leaderboard, ID } from './Leaderboard';
import { TimeFrame, PeriodicLeaderboard } from './PeriodicLeaderboard';
import { buildScript } from './Common';
import { TimestampedLeaderboardOptions, TimestampedLeaderboard } from './TimestampedLeaderboard';
import { LeaderboardMatrix } from './LeaderboardMatrix';
import { AssertionError } from 'assert';
import { MultimetricLeaderboard } from './MultiMetricLeaderboard';
import { MultiMetricPeriodicLeaderboard } from './MultiMetricPeriodicLeaderboard';


export type DimensionDefinition = {
    /** dimension name */
    name: string;
    /** dimension Time Frame */
    timeFrame?: TimeFrame
}

export type FeatureDefinition = {
    /** feature name */
    name: string;
    /** underlying leaderboard options. path is ignored */
    options?: Partial<TimestampedLeaderboardOptions>;
}

export type MultiMetricLeaderboardMatrixOptions = {
    /**
     * base path to store all the leaderboards
     * 
     * <path>:<dimension>:<feature>:<time key>
     */
    path: string,
    /** leaderboard dimensions. Provide at least one */
    dimensions: DimensionDefinition[],
    /** leaderboard features. Provide at least one */
    features: FeatureDefinition[],
    /** custom function to evaluate the current time */
    now(): Date,
    maxUsers: number,
}

export type MatrixEntry = {
    /** identifier */
    id: ID,
    /** ranking */
    rank: number,
    /** feature scores */
    [feature: string]: ID | number
}

export class MultiMetricLeaderboardMatrix extends LeaderboardMatrix {
    protected maxUsers: number;
    protected ALL_METRIC: string

    constructor(client: Redis, options: Partial<MultiMetricLeaderboardMatrixOptions> = {}) {
        options = Object.assign({
            path: 'multimetriclbmatrix',
            dimensions: [{
                name: 'global',
                timeFrame: 'all-time'
            }],
            features: [{
                name: 'default',
                options: {
                    lowToHigh: false,
                    earlierToLater: true,
                }
            }],
            now: () => new Date()
        }, options);
        if (options.features === undefined || options.maxUsers === undefined) throw new AssertionError({ message: 'Invalid option. featrues is undefined! maxUsers not set!' })
        options.features.push({ name: 'allMetrics' })
        super(client, options)
        this.maxUsers = options.maxUsers
        this.ALL_METRIC = 'allMetrics'
    }

    /**
     * Get the corresponding leaderboard in the matrix
     */
    get(dimension: string, feature: string, time?: Date): (TimestampedLeaderboard | MultimetricLeaderboard | null) {
        // check dimension & feature
        let dim_index = this.options.dimensions.findIndex((dim) => dim.name === dimension);
        let feat_index = this.options.features.findIndex((feat) => feat.name === feature);

        if (dim_index === -1 || feat_index === -1) {
            return null;
        }

        let dim = this.options.dimensions[dim_index];
        let feat = this.options.features[feat_index];

        if (feat.name === this.ALL_METRIC) {
            if (this.matrix[dim_index][feat_index] === null) {
                this.options.features.forEach((fd, i) => fd.name !== this.ALL_METRIC ? this.get(dimension, fd.name, time) : null)
                let allOtherLeaderboards = Array.from(
                    this.options.features.splice(feat_index),
                    (_, i) => this.matrix[dim_index][i] as PeriodicLeaderboard)

                this.matrix[dim_index][feat_index] = new MultiMetricPeriodicLeaderboard(this.client, {
                    path: `${this.options.path}:${dim.name}:${feat.name}`,
                    timeFrame: dim.timeFrame,
                    now: this.options.now,
                    leaderboards: allOtherLeaderboards,
                    maxUsers: this.maxUsers
                });
            }
            return (this.matrix[dim_index][feat_index] as MultiMetricPeriodicLeaderboard).get(time ? time : this.options.now()) as MultimetricLeaderboard;

        }
        return super.get(dimension, feature, time) as TimestampedLeaderboard
    }

    /**
     * Creates or updates an entry present in multiple leaderboards in the matrix
     * 
     * e.g.
     * 
     * ```
     * add('id', {
     *   feature1: 99,
     *   feature2: 48
     * }, ['dimension1', 'dimension2'])
     * ```
     * 
     * Note: if a feature/dimension is invalid, the combination is ignored
     * Note: if the entry is not present in the leaderboard, it is created
     * 
     * @param features key-value object with features as key and values as scores
     * @param dimensions if provided, insertion will only occur on the provided dimensions.
     *                   if not provided, all dimensions are used
     */
    async add(id: ID, features: { [key: string]: number }, dimensions: string[] = []): Promise<void> {
        await super.add(id, features, dimensions)
        if (dimensions.length == 0) { // use all dimensions
            dimensions = this.options.dimensions.map((dim) => dim.name);
        }
        for (let dimension of dimensions) {
            let mmlb = this.get(dimension, this.ALL_METRIC) as MultimetricLeaderboard;
            await mmlb.updateRank(id);
        }
    }

    /**
     * Increments an entry present in multiple leaderboards in the matrix
     * 
     * e.g.
     * 
     * ```
     * incr('id', {
     *   feature1: 8,
     *   feature2: 5
     * }, ['dimension1', 'dimension2'])
     * ```
     * 
     * Note: if a feature/dimension is invalid, the combination is ignored
     * Note: if the entry is not present in the leaderboard, it is created
     * 
     * @param features key-value object with features as key and values as scores
     * @param dimensions if provided, insertion will only occur on the provided dimensions.
     *                   if not provided, all dimensions are used
     */
    async incr(id: ID, features: { [key: string]: number }, dimensions: string[] = []): Promise<void> {
        await super.incr(id, features, dimensions)
        if (dimensions.length == 0) { // use all dimensions
            dimensions = this.options.dimensions.map((dim) => dim.name);
        }
        for (let dimension of dimensions) {
            let mmlb = this.get(dimension, this.ALL_METRIC) as MultimetricLeaderboard;
            await mmlb.updateRank(id);
        }
    }

    /**
     * Improves the scores of an entry present in multiple leaderboards in the matrix
     * 
     * e.g.
     * 
     * ```
     * improve('id', {
     *   feature1: 8,
     *   feature2: 5
     * }, ['dimension1', 'dimension2'])
     * ```
     * 
     * Note: if a feature/dimension is invalid, the combination is ignored
     * Note: if the entry is not present in the leaderboard, it is created
     * 
     * @param features key-value object with features as key and the value to increment as value
     * @param dimensions if provided, improvement will only occur on the provided dimensions.
     *                   if not provided, all dimensions are used
     */
    async improve(id: ID, features: { [key: string]: number }, dimensions: string[] = []): Promise<void> {
        await super.improve(id, features, dimensions)
        if (dimensions.length == 0) { // use all dimensions
            dimensions = this.options.dimensions.map((dim) => dim.name);
        }
        for (let dimension of dimensions) {
            let mmlb = this.get(dimension, this.ALL_METRIC) as MultimetricLeaderboard;
            console.log(mmlb);
            console.log("****************************");
            console.log(dimension);
            console.log("****************************");
            

            await mmlb.updateRank(id);
        }
    }

    /**
     * Retrieve an entry from the leaderboard
     * 
     * @param feature only provide feature if you need the entry to be ranked
     */
    async peek(id: ID, dimension: string, feature?: string): Promise<MatrixEntry | null> {
        if (feature) {
            // just rely on around()
            let list = await this.around(dimension, feature, id, 0, false);
            return list.length == 0 ? null : list[0];
        }
        // else, only query the features across the dimension

        if (this.options.dimensions.find((dim) => dim.name === dimension) === undefined)
            return null;

        let result = await this.client.eval(buildScript(`
            return retrieveMultimetricEntry(ARGV[1], KEYS)
            `),
            this.options.features.length,
            this.options.features.map(f => this.get(dimension, f.name)!.getPath()),

            id
        );

        if (result.every((e: any) => e === null))
            return null;

        let entry: MatrixEntry = { id, rank: 0 };
        this.options.features.map((f, f_i) => {
            entry[f.name] = parseFloat(result[f_i])
        });
        return entry;
    }

    /**
     * Retrieve the entries ranked between some boundaries (one-based)
     * 
     * @param low lower bound to query (inclusive)
     * @param high higher bound to query (inclusive)
     */
    async list(dimension: string, feature: string, low: number, high: number): Promise<MatrixEntry[]> {
        let lb = this.get(dimension, feature);
        if (!lb) return [];

        let result = await this.client.eval(buildScript(`
            return retrieveMultimetricEntries(KEYS[1], ARGV[1], slice(KEYS, 2, ARGV[2]+1), ARGV[3], ARGV[4])
            `),
            this.options.features.length + 1,
            lb.getPath(),
            this.options.features.map(f => this.get(dimension, f.name)!.getPath()),

            lb.isLowToHigh().toString(),
            this.options.features.length,
            low - 1,
            high - 1
        );

        return this.parseEntries(result, low);
    }

    /**
     * Retrieve the top entries
     * @param max max number of entries to return
     * 
     * Note: This function is an alias for list(1, max)
     */
    top(dimension: string, feature: string, max: number = 10): Promise<MatrixEntry[]> {
        return this.list(dimension, feature, 1, max);
    }

    /**
     * Retrieve the entries around an entry
     * 
     * @param distance number of entries at each side of the queried entry
     * @param fillBorders include entries at the other side if the entry is too close to one of the borders.
     * 
     * @see Leaderboard.around for details
     */
    async around(dimension: string, feature: string, id: ID, distance: number, fillBorders: boolean = false): Promise<MatrixEntry[]> {
        let lb = this.get(dimension, feature);
        if (!lb) return [];
        if (distance < 0)
            return [];

        let result = await this.client.eval(buildScript(`
            local range = multimetricAroundRange(KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4]);
            if range[1] == -1 then return { {}, { {},{} } } end
            return {
                range[1],
                retrieveMultimetricEntries(KEYS[1], ARGV[1], slice(KEYS, 2, ARGV[5]+1), range[1], range[2])
            }
            `),
            this.options.features.length + 1,
            lb.getPath(),
            this.options.features.map(f => this.get(dimension, f.name)!.getPath()),

            lb.isLowToHigh().toString(),
            id,
            distance,
            fillBorders.toString(),
            this.options.features.length
        );

        return this.parseEntries(result[1], parseFloat(result[0]) + 1);
    }

    /**
     * Parses the result of the lua function 'retrieveEntries'
     * 
     * @param data from retrieveEntries
     * @param low rank of the first entry
     */
    protected parseEntries(result: any, low: number): MatrixEntry[] {
        return result[0].map((id: ID, index: number) => {
            let entry: MatrixEntry = { id, rank: low + index };
            this.options.features.map((f, f_i) => {
                entry[f.name] = parseFloat(result[1][f_i][index])
            });
            return entry;
        });
    }

    public async clear(time?: Date): Promise<void> {
        for (let i_dim = 0; i_dim < this.options.dimensions.length; i_dim++) {
            const dim = this.options.dimensions[i_dim];
            for (let i_feat = 0; i_feat < this.options.features.length; i_feat++) {
                const feat = this.options.features[i_feat];
                await this.get(dim.name, feat.name, time)!.clear()
                this.matrix[i_dim][i_feat] = null
            }

        }
    }
}

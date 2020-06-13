import Redis from 'ioredis';

import { MultiMetricLeaderboardMatrix } from '../src/MultimetricLeaderboardMatrix'
import { TimeFrame } from '../src/PeriodicLeaderboard';

(async () => {
    let rc = new Redis(6379, "127.0.0.1");

    const lbOptions = {
        path: 'multimetricbigtestv1.0',
        dimensions: [
            {
                name: "dim1",
                timeFrame: ("all-time" as TimeFrame)
            },
            {
                name: "dim2",
                timeFrame: ("monthly" as TimeFrame)
            }
        ],
        features: [
            {
                name: 'feat1',
                options: {
                    earlierToLater: true,
                    lowToHigh: false,
                }
            },
            {
                name: 'feat2',
                options: {
                    earlierToLater: true,
                    lowToHigh: true,
                }
            }

        ],
        maxUsers: 10000,
    }
    const lb = new MultiMetricLeaderboardMatrix(rc, lbOptions)

    let cond = 1
    console.log('cond value: ', cond);

    var start = (new Date()).getTime()
    if (cond == 1) {
        console.log('testing');

        for (let i = 0; i < 1; i++) {
            const id = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
            const score = Math.random() * 100
            await lb.improve(id, { feat1: score, feat2: score / 6 })
            if (i % 10000 == 0) console.log(i);

        }
    }
    else if (cond == 2) {
        console.log('clearing');
        await lb.clear()
    }

    else if (cond == 3) {
        const result = await lb.top('dim1', 'feat1', 5)
        console.log(result);

    }

    var end = (new Date()).getTime() - start
    console.log(end / 1000, "seconds");

    rc.disconnect();

})().then()


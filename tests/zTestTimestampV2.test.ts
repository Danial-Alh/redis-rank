import { TimestampedLeaderboardV2 } from '../src/TimestampedLeaderboardV2'
import rc from './redis';

const lbOptions = {
    path: 'bigtestv1.0',
    lowToHigh: false,
    earlierToLater: true
}
const lb = new TimestampedLeaderboardV2(rc, lbOptions)

const cond = true
// const cond ۴= false

if (cond) {
    console.log('testing');
    
    for (let i = 0; i < 1000; i++) {
        const id = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
        const score = Math.random() * 100
        lb.improve(id, score).then();
    }
}
else {
    console.log('clearing');
    lb.clear()
}

test("dummy", async () => {
    expect(true).toBe(true);
});
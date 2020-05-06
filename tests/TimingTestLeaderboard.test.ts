import Redis from 'ioredis';

import { Leaderboard } from '../src/Leaderboard'


(async () => {
    let rc = new Redis(6379, "127.0.0.1");

    const lbOptions = {
        path: 'bigtestv1.0',
        lowToHigh: false,
        earlierToLater: true
    }
    const lb = new Leaderboard(rc, lbOptions)

    let cond = 2
    console.log('cond value: ', cond);
    
    var start = (new Date()).getTime()
    if (cond == 1) {
        console.log('testing');

        for (let i = 0; i < 1000000; i++) {
            const id = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
            const score = Math.random() * 100
            await lb.improve(id, score)
            if (i % 100000 == 0) console.log(i);
            
        }
    }
    else if (cond == 2) {
        console.log('clearing');
        await lb.clear()
    }

    else if(cond == 3){
        await lb.top(100)
    }
    else if(cond == 4){
        const id = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
        const score = Math.random() * 100
        await lb.improve(id, score)
    }

    var end = (new Date()).getTime() - start
    console.log(end/1000, "seconds");

    rc.disconnect();

})().then()


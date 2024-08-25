import fs from 'fs';
import readline from 'readline';

function processCsvChunk(path) {
    // track the time
    console.time('processing_csv_time');

    const reader = fs.createReadStream(path, { encoding: "utf8" });

    reader.on('error', (err) => console.error(`Error reading file: ${err}`));

    let data = '';
    reader.on('data', (chunk) => data += chunk);

    reader.on('end', () => {
        const customers = data.split('\n')
                                .slice(1)
                                .filter(row => row !== '')
                                .map(row => {
                                    let splitted = [];
                                    let current = '';
                                    let inQuotes = false;
                                    for (let char of row) {
                                        if (char === '"') {
                                            inQuotes = !inQuotes;
                                        } else if (char === ',' && !inQuotes) {
                                            splitted.push(current);
                                            current = '';
                                        } else {
                                            current += char;
                                        }
                                    }
                                    splitted.push(current);
                                    return splitted;
                                });
            // data = '';

        let cities = new Map(); // 'city name': total customer
        customers.forEach((val) => {
            if (cities.has(val[6])) {
                cities.set(val[6], cities.get(val[6]) + 1);
            } else {
                cities.set(val[6], 1);
            }
        });

        const sorted = Array.from(cities).sort((a, b) => b[1] - a[1]);

        console.log('rows count: ' + customers.length);
        console.log(JSON.stringify(sorted));
        console.timeEnd('processing_csv_time');
    });
}

async function processCsvAwait(path) {
    // track the time
    console.time('processing_csv_time');
    
    const reader = fs.createReadStream(path, { encoding: "utf8" });
    const rl = readline.createInterface({
        input: reader,
        crlfDelay: Infinity
    });

    let count = 0;
    let cities = new Map();
    for await (const line of rl) {
        if (count > 1) { // skip header row
            let splitted = [];
            let current = '';
            let inQuotes = false;
            for (let char of line) {
                if (char === '"') {
                    inQuotes = !inQuotes;
                } else if (char === ',' && !inQuotes) {
                    splitted.push(current);
                    current = '';
                } else {
                    current += char;
                }
            }
            splitted.push(current);

            // mapping cities
            if (cities.has(splitted[6])) {
                cities.set(splitted[6], cities.get(splitted[6]) + 1);
            } else {
                cities.set(splitted[6], 1);
            }
        }
        count++;
    }

    const sorted = Array.from(cities).sort((a, b) => b[1] - a[1]);

    console.log('rows count: ' + (count - 1)); // exclude header row
    console.log(JSON.stringify(sorted));
    console.timeEnd('processing_csv_time');
}

processCsvAwait('../data/customers-1000000.csv');

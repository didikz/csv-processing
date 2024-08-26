import fs from 'fs';
import readline from 'readline';

function processCsvChunk(path) {
    // track the time
    console.time('processing_csv_time');

    const reader = fs.createReadStream(path, { encoding: "utf8" });

    // handle errors
    reader.on('error', (err) => console.error(`Error reading file: ${err}`));

    // chunk the data stream and concat it into a single variable
    let data = '';
    reader.on('data', (chunk) => data += chunk);

    // at the end of the process, process the concatenated data and process it to map & sorting
    reader.on('end', () => {
        const customers = data.split('\n')
                                .slice(1) // do not include header row
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

        // map the data for city and total customers
        let cities = new Map(); // 'city name': total customer
        customers.forEach((val) => {
            if (cities.has(val[6])) {
                cities.set(val[6], cities.get(val[6]) + 1);
            } else {
                cities.set(val[6], 1);
            }
        });

        // sorted by the biggest customers first
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
        // process each row, excluding header row
        // map data customer to cities
        if (count > 1) { // skip header row
            let splitted = [];
            let current = '';
            let inQuotes = false;

            // handle double quote contents because there's possibility comma inside the the double quotes
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

    // sorted by the biggest customers first
    const sorted = Array.from(cities).sort((a, b) => b[1] - a[1]);

    console.log('rows count: ' + (count - 1)); // exclude header row
    console.log(JSON.stringify(sorted));
    console.timeEnd('processing_csv_time');
}

processCsvAwait('../data/customers-1000000.csv');

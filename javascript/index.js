import fs from 'fs';

// track the time
console.time('read_csv');

const reader = fs.createReadStream("../data/customers-100000.csv", { encoding: "utf8" });

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
    console.timeEnd('read_csv');
});

<?php
$startMemory = memory_get_usage();
$timeStart = microtime(true); 
// open the file
$f = fopen("../data/customers-1000000.csv", "r");
$cityMap = [];
$rows = 0;
if ($f !== false) {
    $records = extractCsv($f);
    foreach ($records as $key => $record) {
        $rows++;
        if ($key == 0) {
            continue;  // skip header row
        }
        
        if (array_key_exists($record[6], $cityMap)) {
            $cityMap[$record[6]]++;
        } else {
            $cityMap[$record[6]] = 1;
        }
    }
    arsort($cityMap); // descending sort city by customer count value
} else {
    echo "Error opening file\n";
}

$timeEnd = microtime(true);
$endMemory = memory_get_usage();
echo sprintf("Rows count: %d", $rows) . PHP_EOL;
echo sprintf("processing time: %f (s)", ($timeEnd - $timeStart))  . PHP_EOL;
echo sprintf("memory usage: %f (Mb)", ($endMemory - $startMemory) / 1024 / 1024)  . PHP_EOL;
echo "sorted from most customers in the city: " . json_encode($cityMap) . PHP_EOL;

function extractCsv($file) {
    while (($csv = fgetcsv($file)) !== FALSE) {
        $num = count($csv);
        $record = [];
        for ($c=0; $c < $num; $c++) {
            $record[] = $csv[$c];
        }
        yield $record;
    }
}

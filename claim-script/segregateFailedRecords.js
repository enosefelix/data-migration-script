const fs = require('fs').promises;
const path = require('path');

async function segregateFailedRecords(failedRecordsFile) {
  console.log("🚀 ~ segregateFailedRecords ~ failedRecordsFile:", failedRecordsFile)
  // const filePath = path.resolve(__dirname, './output/failed_records.json');
  const data = await fs.readFile(failedRecordsFile, 'utf-8');
  const records = JSON.parse(data);

  const segregatedData = {};

  records.forEach(record => {
    const errorCategory = record.error.split(':')[0];
    if (!segregatedData[errorCategory]) {
      segregatedData[errorCategory] = [];
    }
    segregatedData[errorCategory].push(record);
  });

  // const outputFilePath = path.resolve(__dirname, './output/failed_records.json');
  const outputFilePath = failedRecordsFile;

  await fs.writeFile(outputFilePath, JSON.stringify(segregatedData, null, 2), 'utf-8');
  console.log(`Segregated data has been written to ${outputFilePath}`);
}

module.exports = { segregateFailedRecords };

// segregateFailedRecords().catch(console.error); 
const fs = require('fs');
const path = require('path');

// const filePath = path.join(__dirname, 'failed_records.json');
let writeQueue = Promise.resolve();

async function logFailedRecord(filePath, data) {
  console.log("🚀 ~ logFailedRecord ~ data:", data)
  console.log("🚀 ~ logFailedRecord ~ filePath:", filePath)
  writeQueue = writeQueue.then(async () => {
    try {
      const fileExists = fs.existsSync(filePath);
      let records = [];

      // Read existing data only once
      if (fileExists) {
        const existingData = fs.readFileSync(filePath, 'utf-8');
        if (existingData.trim()) {
          records = JSON.parse(existingData);
        }
      }

      if (!Array.isArray(records)) {
        throw new Error('Invalid data format: expected an array');
      }

      // Append new record
      records.push(data);

      // Write updated records back to file
      fs.writeFileSync(filePath, JSON.stringify(records, null, 2), 'utf-8');
      console.log(`✅ Logged record: ${data.claimNumber}`);
    } catch (error) {
      console.error('❌ Error logging failed record:', error);
    }
  });

  return writeQueue;
}

module.exports = {logFailedRecord};

// json_to_excel.js
// ----------------
// Reads a JSON array from “data.json” and writes “output.xlsx”.
// - Collects every key from all objects as a column header.
// - If a value is an object or array, serializes it to JSON string.
// - Leaves null or undefined as empty cells.

const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');

const INPUT_JSON  = path.join(__dirname, 'successful_records.json');
const OUTPUT_XLSX = path.join(__dirname, 'SuccessfulClaimsProd2024.xlsx');

try {
  // 1. Read & parse the JSON file
  const raw = fs.readFileSync(INPUT_JSON, 'utf8');
  const data = JSON.parse(raw);
  console.log("🚀 ~ data length:", data.length);

  if (!Array.isArray(data)) {
    throw new Error('Expected data.json to be a JSON array of objects.');
  }
  if (data.length === 0) {
    throw new Error('JSON array is empty—nothing to write.');
  }

  // 2. Determine every unique key across all objects
  const headerSet = new Set();
  data.forEach((obj) => {
    if (typeof obj !== 'object' || obj === null) return;
    Object.keys(obj).forEach((k) => headerSet.add(k));
  });
  const headers = Array.from(headerSet);

  // 3. Build a 2D array: first row = headers, then one row per object
  const rows = [];

  // 3a. Header row
  rows.push(headers);

  // 3b. Each data row
  data.forEach((obj) => {
    const row = headers.map((col) => {
      let val = obj[col];

      // Convert null/undefined to empty string
      if (val === null || val === undefined) {
        return '';
      }

      // If it's an object or array, stringify it
      if (typeof val === 'object') {
        return JSON.stringify(val);
      }

      // Otherwise (string, number, boolean), leave as is
      return val;
    });
    rows.push(row);
  });

  // 4. Convert the 2D array into a worksheet
  const worksheet = XLSX.utils.aoa_to_sheet(rows);

  // 5. Create a new workbook & append
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');

  // 6. Write to output.xlsx
  XLSX.writeFile(workbook, OUTPUT_XLSX);

  console.log(
    `✅ Wrote ${data.length} record(s) to "${path.basename(OUTPUT_XLSX)}".`
  );
} catch (err) {
  console.error('❌ Error:', err.message);
  process.exit(1);
}

// json_to_excel.js
// ----------------
// Usage: `node json_to_excel.js`
// Reads "failed_records.json" in the same folder and writes "FailedProd2024Records.xlsx".

const fs   = require('fs');
const path = require('path');
const XLSX = require('xlsx');

const INPUT_JSON  = path.join(__dirname, 'failed_records.json');
const OUTPUT_XLSX = path.join(__dirname, 'FailedProd2024Records.xlsx');

try {
  // 1. Load & parse
  const raw  = fs.readFileSync(INPUT_JSON, 'utf8');
  const data = JSON.parse(raw);
  console.log("🚀 ~ data length:", data);
  if (typeof data !== 'object' || data === null) {
    throw new Error(
      'Expected top-level JSON object mapping categories → arrays'
    );
  }

  // 2. Create a new workbook
  const workbook = XLSX.utils.book_new();
  const usedSheetNames = new Set();
  const MAX_SHEET_NAME_LEN = 31;

  // 3. For each error category...
  Object.entries(data).forEach(([category, entries]) => {
    if (!Array.isArray(entries) || entries.length === 0) return;

    // extract records
    const records = entries
      .map(e => e.record)
      .filter(r => typeof r === 'object' && r !== null);
    if (records.length === 0) return;

    // collect headers
    const headerSet = new Set();
    records.forEach(obj =>
      Object.keys(obj).forEach(key => headerSet.add(key))
    );
    const headers = Array.from(headerSet);

    // build rows
    const aoa = [headers];
    records.forEach(obj => {
      const row = headers.map(col => {
        const v = obj[col];
        if (v === null || v === undefined) return '';
        if (typeof v === 'object')       return JSON.stringify(v);
        return v;
      });
      aoa.push(row);
    });

    // make worksheet
    const ws = XLSX.utils.aoa_to_sheet(aoa);

    // ensure unique sheet name ≤31 chars
    let base = category.substring(0, MAX_SHEET_NAME_LEN);
    let name = base;
    let idx = 1;
    while (usedSheetNames.has(name)) {
      const suf = `_${idx++}`;
      name = base.substring(0, MAX_SHEET_NAME_LEN - suf.length) + suf;
    }
    usedSheetNames.add(name);

    XLSX.utils.book_append_sheet(workbook, ws, name);
  });

  // 4. Write and exit
  XLSX.writeFile(workbook, OUTPUT_XLSX);
  console.log(
    `✅ Wrote ${OUTPUT_XLSX} with sheets: ${[...usedSheetNames].join(', ')}`
  );
  process.exit(0);      // ← force Node to quit here
}
catch (err) {
  console.error('❌ Error:', err.message);
  process.exit(1);      // ← non-zero on error
}
